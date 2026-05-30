"""
pipeline.behavioral_features - Pure Behavioral Feature Engineering
===================================================================
Production ML Feature Store - Behavioral Features Layer

ARCHITECTURE PRINCIPLE:
    Behavioral features are INDEPENDENT from fraud detection logic.
    
    Source: silver_deduplicated_invoices ONLY
    Output: vendor_daily_behavioral_features
    
    NO dependency on gold_fraud_alerts.
    This ensures feature stability and reusability.

PRODUCTION PATTERN (Google, Netflix, Uber):
    Feature Store = Immutable, versioned, reusable behavioral features
    Separate from: Rule engines, alert systems, labels
    
    Why?
    - Features are stable (invoice schema rarely changes)
    - Fraud rules evolve frequently (new attack patterns)
    - Same features used by multiple ML models
    - Clear separation of concerns

FEATURES COMPUTED (8 behavioral signals):
    
    VOLUME FEATURES:
    - invoice_count: Daily activity level
    - total_invoice_amount: Daily spending volume
    - avg_invoice_amount: Typical invoice size
    - min_invoice_amount: Distribution lower bound
    - max_invoice_amount: Distribution upper bound
    - stddev_invoice_amount: Invoice volatility/consistency
    
    TEMPORAL FEATURES:
    - avg_invoices_per_hour: Submission rate (bot detection)
    - latest_invoice_timestamp: Recency (for time decay weighting)

ROLLING WINDOWS (computed via BigQuery scheduled query):
    - invoice_count_7d: 7-day rolling sum
    - invoice_count_30d: 30-day rolling sum
    - avg_invoice_amount_7d: 7-day rolling average
    - avg_invoice_amount_30d: 30-day rolling average
    
    Why separate?
    - Beam state for rolling windows is complex and costly
    - BigQuery WINDOW functions are optimized and fast
    - Scheduled query runs nightly, updates rolling features
    - See: gcp/compute_rolling_features.sql

USAGE:
    Batch:
        silver_invoices | BuildVendorDailyBehavioralFeatures()
    
    Streaming:
        silver_invoices | BuildVendorDailyBehavioralFeatures()
    
    Same code, different sources (GCS vs Pub/Sub).
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Iterable

import apache_beam as beam

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# BEHAVIORAL FEATURE COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════

class ComputeVendorDailyBehavioralFeatures(beam.DoFn):
    """
    Compute behavioral features from invoices for one vendor-day.
    
    INPUT:
        ((vendor_id, date_str), [invoice_dicts])
    
    OUTPUT:
        Behavioral feature row dict.
    
    DESIGN DECISIONS:
        
        Q: Why compute stddev here instead of in BigQuery?
        A: Beam computes on raw events (one pass), BigQuery would require
           reading all rows twice (once for mean, once for stddev).
           More efficient to compute during aggregation.
        
        Q: Why not use Beam CombineFn?
        A: Could, but DoFn is simpler and sufficient for vendor-day grain.
           CombineFn shines for global aggregations or large groups.
        
        Q: Why round to 2 decimal places?
        A: BigQuery NUMERIC(15,2) precision. Prevents floating-point drift.
    """
    
    def process(self, element: Tuple[Tuple[str, str], Iterable[Dict[str, Any]]]) -> Iterable[Dict[str, Any]]:
        """
        Compute behavioral features for one vendor-day.
        
        Args:
            element: ((vendor_id, date_str), [invoice_dicts])
        
        Yields:
            Behavioral feature row dict.
        """
        (vendor_id, date_str), invoices = element
        
        # Convert to list (Beam iterable may not support len())
        invoice_list = list(invoices)
        
        if not invoice_list:
            log.warning(f"No invoices for vendor {vendor_id} on {date_str}")
            return
        
        # ─── VOLUME FEATURES ───────────────────────────────────────────────────
        invoice_count = len(invoice_list)
        amounts = [float(inv.get('invoice_amount', 0)) for inv in invoice_list]
        
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
        timestamps = []
        for inv in invoice_list:
            ts = inv.get('invoice_timestamp')
            if isinstance(ts, str):
                timestamps.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
            elif isinstance(ts, datetime):
                timestamps.append(ts)
        
        latest_invoice_timestamp = max(timestamps) if timestamps else None
        
        # Calculate invoices per hour
        # Logic: time span between first and last invoice
        if len(timestamps) > 1:
            time_span_hours = (max(timestamps) - min(timestamps)).total_seconds() / 3600.0
            if time_span_hours > 0:
                avg_invoices_per_hour = invoice_count / time_span_hours
            else:
                # All invoices at exact same timestamp (bot behavior!)
                avg_invoices_per_hour = float(invoice_count)
        else:
            # Single invoice - no rate calculation
            avg_invoices_per_hour = 0.0
        
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
            
            # Metadata
            'computed_at': datetime.now(timezone.utc).isoformat(),
        }
        
        log.debug(f"Behavioral features for vendor {vendor_id} on {date_str}: "
                  f"{invoice_count} invoices, avg=${avg_invoice_amount:.2f}")
        
        yield feature_row


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════

class BuildVendorDailyBehavioralFeatures(beam.PTransform):
    """
    End-to-end behavioral feature engineering transform.
    
    INPUT:
        PCollection of invoice dicts from silver_deduplicated_invoices
    
    OUTPUT:
        PCollection of behavioral feature row dicts.
    
    PIPELINE PATTERN:
        invoices → key by (vendor_id, date) → group → compute features
    
    PRODUCTION BENEFITS:
        - Independent from Gold layer (no dependency on fraud alerts)
        - Reusable across multiple ML models
        - Stable feature schema (invoice schema rarely changes)
        - Can version features independently of fraud rules
    
    DESIGN DECISION:
        Q: Why not join with alerts here?
        A: Separation of concerns. Behavioral features are INTRINSIC to vendor.
           Risk features (alerts) are EXTRINSIC (derived from rules).
           Large companies keep these separate for feature store stability.
    """
    
    def expand(self, invoices: beam.PCollection) -> beam.PCollection:
        """
        Build behavioral features from invoices.
        
        Args:
            invoices: PCollection of invoice dicts from silver layer
        
        Returns:
            PCollection of behavioral feature row dicts.
        """
        
        def key_by_vendor_date(invoice: Dict[str, Any]) -> Tuple[Tuple[str, str], Dict[str, Any]]:
            """Extract (vendor_id, date_str) key from invoice."""
            vendor_id = invoice.get('vendor_id')
            
            ts = invoice.get('invoice_timestamp')
            if isinstance(ts, str):
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            elif isinstance(ts, datetime):
                dt = ts
            else:
                log.warning(f"Invalid timestamp in invoice: {ts}")
                return None
            
            date_str = dt.date().isoformat()  # "YYYY-MM-DD"
            
            return ((vendor_id, date_str), invoice)
        
        features = (
            invoices
            | "KeyInvoicesByVendorDate" >> beam.Map(key_by_vendor_date)
            | "FilterNoneKeys" >> beam.Filter(lambda x: x is not None)
            | "GroupByVendorDate" >> beam.GroupByKey()
            | "ComputeBehavioralFeatures" >> beam.ParDo(ComputeVendorDailyBehavioralFeatures())
        )
        
        return features


# ═══════════════════════════════════════════════════════════════════════════
# BIGQUERY SCHEMA
# ═══════════════════════════════════════════════════════════════════════════

VENDOR_DAILY_BEHAVIORAL_FEATURES_SCHEMA = {
    "fields": [
        {"name": "vendor_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "feature_date", "type": "DATE", "mode": "REQUIRED"},
        
        # Volume features
        {"name": "invoice_count", "type": "INT64", "mode": "REQUIRED"},
        {"name": "total_invoice_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "avg_invoice_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "min_invoice_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "max_invoice_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "stddev_invoice_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        
        # Temporal features
        {"name": "avg_invoices_per_hour", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "latest_invoice_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        
        # Rolling windows (computed via BigQuery scheduled query)
        {"name": "invoice_count_7d", "type": "INT64", "mode": "NULLABLE"},
        {"name": "invoice_count_30d", "type": "INT64", "mode": "NULLABLE"},
        {"name": "avg_invoice_amount_7d", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "avg_invoice_amount_30d", "type": "NUMERIC", "mode": "NULLABLE"},
        
        # Metadata
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}
