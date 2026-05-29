"""
pipeline.feature_engineering - ML Feature Store Layer
======================================================
Phase 1 of ML Roadmap: Feature Engineering Foundation

PURPOSE:
    Build behavioral features at vendor-day granularity for future ML models.
    This is NOT training a model yet - we're building the feature infrastructure
    that Vertex AI models will consume.

ARCHITECTURE:
    silver_deduplicated_invoices + gold_fraud_alerts
                    ↓
        Feature Engineering Layer (this module)
                    ↓
           vendor_daily_features
                    ↓
    (Future: Vertex AI Training, Trust Scores, Risk Models)

ML CONCEPTS EXPLAINED:
    
    Feature Engineering = transforming raw data into numerical signals that
    ML models can learn from. Good features capture business behavior patterns
    that correlate with fraud.
    
    Why vendor-day granularity?
        - Daily = short enough to detect rapid behavior changes
        - Vendor = natural grouping for trust/risk modeling
        - Time-series ready for LSTM/Transformer models
    
    Feature types we're building:
        1. Volume features: spending patterns, invoice counts
        2. Statistical features: mean, stddev, min, max (distribution shape)
        3. Temporal features: submission rate, recency
        4. Label features: alert counts (supervised learning labels)
        5. Derived features: ratios, rates (non-linear relationships)

USAGE:
    Batch mode:
        python -m pipeline.gcp_batch_main --mode=batch ...
        (reads silver/gold tables, writes vendor_daily_features)
    
    Streaming mode:
        python -m pipeline.gcp_stream_main --mode=streaming ...
        (aggregates per-day windows, writes vendor_daily_features)

FUTURE INTEGRATION:
    - Vertex AI AutoML Tables: reads vendor_daily_features for training
    - Vertex AI Predictions: uses same features for real-time scoring
    - Looker Studio: vendor risk dashboards
    - BigQuery ML: ARIMA time-series forecasting per vendor
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Iterable

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms import combiners

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# FEATURE CALCULATION LOGIC
# ═══════════════════════════════════════════════════════════════════════════

class ComputeVendorDailyFeatures(beam.DoFn):
    """
    Aggregates invoices and alerts to compute vendor-day features.
    
    WHY THIS EXISTS:
        ML models need numerical features, not raw events. This DoFn transforms
        raw invoice/alert data into statistical summaries that capture vendor
        behavior patterns.
    
    INPUT:
        Key: (vendor_id, date_str)  # e.g., ("V-1001", "2026-05-29")
        Value: {
            'invoices': [list of invoice dicts],
            'alerts': [list of alert dicts]
        }
    
    OUTPUT:
        BigQuery row dict with all features computed.
    
    FEATURE ENGINEERING DECISIONS:
        
        1. invoice_count:
           - CAPTURES: Vendor activity level
           - ML USE: Baseline behavior (normal vendors have consistent counts)
           - FRAUD SIGNAL: Sudden spike = possible flooding attack
           - TYPE: Short-term behavioral signal
        
        2. avg_invoice_amount:
           - CAPTURES: Typical spending per invoice
           - ML USE: Anomaly detection baseline
           - FRAUD SIGNAL: Deviation from historical average = suspicious
           - TYPE: Long-term behavioral signal (slow-changing)
        
        3. stddev_invoice_amount:
           - CAPTURES: Invoice consistency/volatility
           - ML USE: Risk models weight volatile vendors differently
           - FRAUD SIGNAL: High stddev = erratic behavior = higher risk
           - TYPE: Statistical feature (distribution shape)
        
        4. min/max_invoice_amount:
           - CAPTURES: Invoice range boundaries
           - ML USE: Detect split invoicing (many small) or inflated billing (one large)
           - FRAUD SIGNAL: min << avg = possible invoice splitting fraud
           - TYPE: Distribution extremes
        
        5. total_invoice_amount:
           - CAPTURES: Total daily spending
           - ML USE: Vendor size/importance weighting
           - FRAUD SIGNAL: Massive total = high-stakes fraud attempt
           - TYPE: Aggregate volume signal
        
        6. avg_invoices_per_hour:
           - CAPTURES: Submission rate/velocity
           - ML USE: Bot detection, automation detection
           - FRAUD SIGNAL: High rate = automated bot flooding
           - TYPE: Temporal pattern
        
        7. alert_counts (anomaly, velocity, duplicate):
           - CAPTURES: Rule-based fraud signals
           - ML USE: Supervised learning labels (if alert > 0, likely fraud)
           - FRAUD SIGNAL: Direct fraud indicators from rule engine
           - TYPE: Label features (ground truth for training)
        
        8. high_risk_alert_ratio:
           - CAPTURES: Fraud propensity (alerts / invoices)
           - ML USE: Probability-like score (0.0 - 1.0)
           - FRAUD SIGNAL: High ratio = most invoices triggered alerts
           - TYPE: Derived feature (non-linear relationship)
        
        9. latest_invoice_timestamp:
           - CAPTURES: Recency of activity
           - ML USE: Time decay weighting (recent activity more relevant)
           - FRAUD SIGNAL: Long gap then sudden activity = dormant account attack
           - TYPE: Temporal metadata
    
    ML MODEL USAGE PATTERNS:
        - Logistic Regression: Uses all features linearly weighted
        - Random Forest: Splits on feature thresholds (e.g., if stddev > X)
        - Neural Networks: Learns non-linear combinations
        - Time-Series Models: Uses features across multiple days (sequences)
        - AutoML: Automatically selects most predictive features
    """
    
    def process(self, element: Tuple[Tuple[str, str], Dict[str, list]]) -> Iterable[Dict[str, Any]]:
        """
        Compute all features for one vendor-day.
        
        Args:
            element: ((vendor_id, date_str), {'invoices': [...], 'alerts': [...]})
        
        Yields:
            Feature row dict ready for BigQuery insert.
        """
        (vendor_id, date_str), grouped_data = element
        
        invoices = grouped_data.get('invoices', [])
        alerts = grouped_data.get('alerts', [])
        
        # Edge case: no invoices for this vendor-day (shouldn't happen, but defensive)
        if not invoices:
            log.warning(f"No invoices for vendor {vendor_id} on {date_str}")
            return
        
        # ─── VOLUME FEATURES ───────────────────────────────────────────────────
        invoice_count = len(invoices)
        amounts = [float(inv.get('invoice_amount', 0)) for inv in invoices]
        total_invoice_amount = sum(amounts)
        avg_invoice_amount = total_invoice_amount / invoice_count if invoice_count > 0 else 0.0
        min_invoice_amount = min(amounts) if amounts else 0.0
        max_invoice_amount = max(amounts) if amounts else 0.0
        
        # Standard deviation (population stddev for consistency)
        if len(amounts) > 1:
            mean = avg_invoice_amount
            variance = sum((x - mean) ** 2 for x in amounts) / len(amounts)
            stddev_invoice_amount = variance ** 0.5
        else:
            stddev_invoice_amount = 0.0
        
        # ─── TEMPORAL FEATURES ─────────────────────────────────────────────────
        # Extract timestamps
        timestamps = []
        for inv in invoices:
            ts = inv.get('invoice_timestamp')
            if isinstance(ts, str):
                timestamps.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
            elif isinstance(ts, datetime):
                timestamps.append(ts)
        
        latest_invoice_timestamp = max(timestamps) if timestamps else None
        
        # Calculate invoices per hour
        # Logic: if all invoices fall within a date, spread = 24 hours max
        # For more precision, calculate actual time span, but for daily features,
        # we assume 24-hour window and distribute evenly.
        # Smarter approach: calculate actual span between first and last invoice
        if len(timestamps) > 1:
            time_span_hours = (max(timestamps) - min(timestamps)).total_seconds() / 3600.0
            if time_span_hours > 0:
                avg_invoices_per_hour = invoice_count / time_span_hours
            else:
                # All invoices at exact same timestamp (bot behavior!)
                avg_invoices_per_hour = float(invoice_count)
        else:
            # Single invoice
            avg_invoices_per_hour = 0.0
        
        # ─── ALERT FEATURES ────────────────────────────────────────────────────
        # Count alerts by rule type
        anomaly_alert_count = sum(1 for a in alerts if a.get('rule_name') == 'ANOMALY')
        velocity_alert_count = sum(1 for a in alerts if a.get('rule_name') == 'VELOCITY')
        duplicate_alert_count = sum(1 for a in alerts if a.get('rule_name') == 'DUPLICATE')
        total_alert_count = len(alerts)
        
        # High-risk alert ratio (fraud propensity score)
        # This is a key ML feature: what % of invoices triggered alerts?
        high_risk_alert_ratio = total_alert_count / invoice_count if invoice_count > 0 else 0.0
        
        # ─── BUILD OUTPUT ROW ──────────────────────────────────────────────────
        feature_row = {
            'vendor_id': vendor_id,
            'feature_date': date_str,  # DATE type in BigQuery
            
            # Volume features
            'invoice_count': invoice_count,
            'total_invoice_amount': round(total_invoice_amount, 2),
            'avg_invoice_amount': round(avg_invoice_amount, 2),
            'min_invoice_amount': round(min_invoice_amount, 2),
            'max_invoice_amount': round(max_invoice_amount, 2),
            'stddev_invoice_amount': round(stddev_invoice_amount, 2),
            
            # Temporal features
            'avg_invoices_per_hour': round(avg_invoices_per_hour, 2),
            'latest_invoice_timestamp': latest_invoice_timestamp.isoformat() if latest_invoice_timestamp else None,
            
            # Alert features
            'anomaly_alert_count': anomaly_alert_count,
            'velocity_alert_count': velocity_alert_count,
            'duplicate_alert_count': duplicate_alert_count,
            'total_alert_count': total_alert_count,
            'high_risk_alert_ratio': round(high_risk_alert_ratio, 4),
            
            # Metadata
            'computed_at': datetime.now(timezone.utc).isoformat(),
        }
        
        log.info(f"Computed features for vendor {vendor_id} on {date_str}: "
                 f"{invoice_count} invoices, {total_alert_count} alerts")
        
        yield feature_row


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE TRANSFORMS
# ═══════════════════════════════════════════════════════════════════════════

class BuildVendorDailyFeatures(beam.PTransform):
    """
    End-to-end feature engineering transform.
    
    INPUT:
        invoices: PCollection of invoice dicts from silver layer
        alerts: PCollection of alert dicts from gold layer
    
    OUTPUT:
        PCollection of feature row dicts ready for BigQuery.
    
    PIPELINE PATTERN:
        This is a "join + aggregate" pattern common in ML feature engineering:
        
        1. Key both inputs by (vendor_id, date)
        2. CoGroupByKey to join invoices + alerts
        3. Compute features from grouped data
        4. Output to feature store
    
    WHY COGROUPBYKEY:
        CoGroupByKey allows us to join two unbounded streams (invoices + alerts)
        by a common key without loading everything into memory. It's Beam's
        distributed join operator.
    
    PRODUCTION CONSIDERATIONS:
        - Handles missing data gracefully (vendors with no alerts)
        - Aggregates per-vendor-day to bound memory usage
        - Preserves event time for correct windowing
        - Idempotent: re-running produces same features
    """
    
    def expand(self, inputs: pvalue.PCollectionDict) -> beam.PCollection:
        """
        Build feature pipeline.
        
        Args:
            inputs: PCollectionDict with:
                - 'invoices': PCollection of invoice dicts
                - 'alerts': PCollection of alert dicts
        
        Returns:
            PCollection of feature row dicts.
        """
        invoices = inputs['invoices']
        alerts = inputs['alerts']
        
        # ─── KEY BY (VENDOR_ID, DATE) ──────────────────────────────────────────
        def key_by_vendor_date(record, record_type):
            """Extract (vendor_id, date_str) key from invoice or alert."""
            vendor_id = record.get('vendor_id')
            
            # Extract timestamp (different field names for invoices vs alerts)
            if record_type == 'invoice':
                ts = record.get('invoice_timestamp')
            else:  # alert
                ts = record.get('detected_at')
            
            # Parse timestamp to date string
            if isinstance(ts, str):
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            elif isinstance(ts, datetime):
                dt = ts
            else:
                log.warning(f"Invalid timestamp in {record_type}: {ts}")
                return None
            
            date_str = dt.date().isoformat()  # "YYYY-MM-DD"
            
            return ((vendor_id, date_str), record)
        
        keyed_invoices = (
            invoices
            | "KeyInvoicesByVendorDate" >> beam.Map(lambda inv: key_by_vendor_date(inv, 'invoice'))
            | "FilterNoneKeysInvoices" >> beam.Filter(lambda x: x is not None)
        )
        
        keyed_alerts = (
            alerts
            | "KeyAlertsByVendorDate" >> beam.Map(lambda alert: key_by_vendor_date(alert, 'alert'))
            | "FilterNoneKeysAlerts" >> beam.Filter(lambda x: x is not None)
        )
        
        # ─── JOIN INVOICES + ALERTS ────────────────────────────────────────────
        # CoGroupByKey produces: ((vendor_id, date), {'invoices': [...], 'alerts': [...]})
        joined = (
            {'invoices': keyed_invoices, 'alerts': keyed_alerts}
            | "JoinInvoicesAndAlerts" >> beam.CoGroupByKey()
        )
        
        # ─── COMPUTE FEATURES ──────────────────────────────────────────────────
        features = (
            joined
            | "ComputeFeatures" >> beam.ParDo(ComputeVendorDailyFeatures())
        )
        
        return features


# ═══════════════════════════════════════════════════════════════════════════
# BIGQUERY SCHEMA
# ═══════════════════════════════════════════════════════════════════════════

VENDOR_DAILY_FEATURES_SCHEMA = {
    "fields": [
        {"name": "vendor_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "feature_date", "type": "DATE", "mode": "REQUIRED"},
        
        # Volume features
        {"name": "invoice_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "total_invoice_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "avg_invoice_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "min_invoice_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "max_invoice_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "stddev_invoice_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        
        # Temporal features
        {"name": "avg_invoices_per_hour", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "latest_invoice_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        
        # Alert features
        {"name": "anomaly_alert_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "velocity_alert_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "duplicate_alert_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "total_alert_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "high_risk_alert_ratio", "type": "FLOAT64", "mode": "NULLABLE"},
        
        # Metadata
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}
