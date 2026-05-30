"""
pipeline.risk_features - Risk/Label Feature Engineering
========================================================
Production ML Feature Store - Risk Features Layer

ARCHITECTURE PRINCIPLE:
    Risk features are DERIVED from fraud detection rules.
    
    Source: gold_fraud_alerts ONLY
    Output: vendor_daily_risk_features
    
    These are effectively "labels" for supervised learning.

PRODUCTION PATTERN (Google, Netflix, Uber):
    Label Store = Ground truth derived from business rules/human review
    Separate from: Feature Store (behavioral features)
    
    Why separate?
    - Rule logic evolves frequently (new fraud patterns)
    - Behavioral features are stable
    - Clear separation: features (X) vs labels (y)
    - Can version labels independently

FEATURES COMPUTED (5 risk signals):
    
    ALERT COUNTS (day-level):
    - anomaly_alert_count: ANOMALY rule triggers today
    - velocity_alert_count: VELOCITY rule triggers today
    - duplicate_alert_count: DUPLICATE rule triggers today
    - total_alert_count: Total alerts (any type)
    
    DERIVED RISK SCORE:
    - high_risk_alert_ratio: alerts / invoice_count
      (requires join with behavioral features for invoice_count)

ROLLING RISK WINDOWS (computed via BigQuery scheduled query):
    - anomaly_count_30d: 30-day anomaly alert history
    - velocity_count_30d: 30-day velocity alert history
    - total_alert_count_30d: 30-day total alert history
    
    Why 30-day?
    - Captures vendor risk trajectory over time
    - ML models learn "this vendor has been risky for weeks"
    - Distinguishes one-time errors from persistent fraud

USAGE:
    Batch:
        gold_alerts | BuildVendorDailyRiskFeatures()
    
    Streaming:
        gold_alerts | BuildVendorDailyRiskFeatures()

IMPORTANT:
    This module is OPTIONAL. For pure feature engineering, you can skip
    risk features entirely and use behavioral features only.
    
    Risk features are only needed if:
    - You want supervised learning (need labels)
    - You want risk scoring dashboards
    - You want to track vendor risk over time
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Iterable

import apache_beam as beam

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# RISK FEATURE COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════

class ComputeVendorDailyRiskFeatures(beam.DoFn):
    """
    Compute risk features from fraud alerts for one vendor-day.
    
    INPUT:
        ((vendor_id, date_str), [alert_dicts])
    
    OUTPUT:
        Risk feature row dict.
    
    DESIGN DECISIONS:
        
        Q: Why count alerts instead of summing fraud_scores?
        A: Count is more robust. fraud_score can have varying scales across
           rule types. Count is simple, interpretable, and stable.
        
        Q: Why not compute high_risk_alert_ratio here?
        A: Requires invoice_count from behavioral features. We compute it
           in BigQuery scheduled query after joining behavioral + risk.
        
        Q: Should we include alert severity?
        A: For Phase 1, count is sufficient. For Phase 2, add severity
           as a feature: avg_severity, max_severity, critical_count.
    """
    
    def process(self, element: Tuple[Tuple[str, str], Iterable[Dict[str, Any]]]) -> Iterable[Dict[str, Any]]:
        """
        Compute risk features for one vendor-day.
        
        Args:
            element: ((vendor_id, date_str), [alert_dicts])
        
        Yields:
            Risk feature row dict.
        """
        (vendor_id, date_str), alerts = element
        
        # Convert to list
        alert_list = list(alerts)
        
        if not alert_list:
            # Vendor-day with no alerts (clean vendor)
            # Still create row with zero counts
            pass
        
        # ─── ALERT COUNT FEATURES ──────────────────────────────────────────────
        anomaly_alert_count = sum(1 for a in alert_list if a.get('rule_name') == 'ANOMALY')
        velocity_alert_count = sum(1 for a in alert_list if a.get('rule_name') == 'VELOCITY')
        duplicate_alert_count = sum(1 for a in alert_list if a.get('rule_name') == 'DUPLICATE')
        total_alert_count = len(alert_list)
        
        # ─── BUILD OUTPUT ROW ──────────────────────────────────────────────────
        feature_row = {
            'vendor_id': vendor_id,
            'feature_date': date_str,
            
            # Alert counts
            'anomaly_alert_count': anomaly_alert_count,
            'velocity_alert_count': velocity_alert_count,
            'duplicate_alert_count': duplicate_alert_count,
            'total_alert_count': total_alert_count,
            
            # high_risk_alert_ratio computed in BigQuery (requires invoice_count)
            
            # Metadata
            'computed_at': datetime.now(timezone.utc).isoformat(),
        }
        
        log.debug(f"Risk features for vendor {vendor_id} on {date_str}: "
                  f"{total_alert_count} total alerts")
        
        yield feature_row


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════

class BuildVendorDailyRiskFeatures(beam.PTransform):
    """
    End-to-end risk feature engineering transform.
    
    INPUT:
        PCollection of alert dicts from gold_fraud_alerts
    
    OUTPUT:
        PCollection of risk feature row dicts.
    
    PIPELINE PATTERN:
        alerts → key by (vendor_id, date) → group → compute risk features
    
    PRODUCTION BENEFITS:
        - Separate from behavioral features (clear separation of concerns)
        - Labels can evolve independently (new rule types)
        - Can version risk features separately
        - Optional: can skip for unsupervised learning
    
    DESIGN DECISION:
        Q: What about vendors with no alerts on a given day?
        A: They won't have rows in vendor_daily_risk_features.
           When joining with behavioral features, use LEFT JOIN so
           clean vendors still appear (with NULL risk features = 0 alerts).
    """
    
    def expand(self, alerts: beam.PCollection) -> beam.PCollection:
        """
        Build risk features from alerts.
        
        Args:
            alerts: PCollection of alert dicts from gold layer
        
        Returns:
            PCollection of risk feature row dicts.
        """
        
        def key_by_vendor_date(alert: Dict[str, Any]) -> Tuple[Tuple[str, str], Dict[str, Any]]:
            """Extract (vendor_id, date_str) key from alert."""
            vendor_id = alert.get('vendor_id')
            
            # detected_at timestamp
            ts = alert.get('detected_at')
            if isinstance(ts, str):
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            elif isinstance(ts, datetime):
                dt = ts
            else:
                log.warning(f"Invalid timestamp in alert: {ts}")
                return None
            
            date_str = dt.date().isoformat()  # "YYYY-MM-DD"
            
            return ((vendor_id, date_str), alert)
        
        features = (
            alerts
            | "KeyAlertsByVendorDate" >> beam.Map(key_by_vendor_date)
            | "FilterNoneKeys" >> beam.Filter(lambda x: x is not None)
            | "GroupByVendorDate" >> beam.GroupByKey()
            | "ComputeRiskFeatures" >> beam.ParDo(ComputeVendorDailyRiskFeatures())
        )
        
        return features


# ═══════════════════════════════════════════════════════════════════════════
# BIGQUERY SCHEMA
# ═══════════════════════════════════════════════════════════════════════════

VENDOR_DAILY_RISK_FEATURES_SCHEMA = {
    "fields": [
        {"name": "vendor_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "feature_date", "type": "DATE", "mode": "REQUIRED"},
        
        # Alert count features
        {"name": "anomaly_alert_count", "type": "INT64", "mode": "REQUIRED"},
        {"name": "velocity_alert_count", "type": "INT64", "mode": "REQUIRED"},
        {"name": "duplicate_alert_count", "type": "INT64", "mode": "REQUIRED"},
        {"name": "total_alert_count", "type": "INT64", "mode": "REQUIRED"},
        
        # Derived risk score (computed in BigQuery scheduled query)
        {"name": "high_risk_alert_ratio", "type": "FLOAT64", "mode": "NULLABLE"},
        
        # Rolling risk windows (computed via BigQuery scheduled query)
        {"name": "anomaly_count_30d", "type": "INT64", "mode": "NULLABLE"},
        {"name": "velocity_count_30d", "type": "INT64", "mode": "NULLABLE"},
        {"name": "total_alert_count_30d", "type": "INT64", "mode": "NULLABLE"},
        
        # Metadata
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}
