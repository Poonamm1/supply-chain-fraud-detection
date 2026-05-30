"""
pipeline.gcp_stream_main — Streaming variant using Pub/Sub
============================================================
Reads ERP invoice events from Pub/Sub, applies the SAME fraud detection
transforms as gcp_batch_main.py, and writes to BigQuery bronze/silver/gold.

Key differences from batch mode:
    * Source: ReadFromPubSub instead of ReadFromText (GCS)
    * Windowing: Uses event-time windowing for velocity detection
    * Triggers: Auto-triggers for continuous output
    * Stopping: Run until manually cancelled (gcloud dataflow jobs cancel)

Run via gcp/run_stream_template.sh — never invoke directly in production.
"""
import argparse
import json
import logging
import os
import sys

# Bootstrap sys.path for Flex Template execution
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import trigger
from datetime import datetime, timezone

from pipeline.transforms import (
    AnomalyCheckDoFn,
    AssignEventTimestamp,
    DeduplicateInvoices,
    ParseErpLine,
    ParseWmsLine,
    VelocityFraudCheck,
    event_to_bronze_row,
)
from pipeline.schemas import ErpInvoiceEvent
from pipeline.behavioral_features import (
    BuildVendorDailyBehavioralFeatures,
    VENDOR_DAILY_BEHAVIORAL_FEATURES_SCHEMA,
)
from pipeline.risk_features import (
    BuildVendorDailyRiskFeatures,
    VENDOR_DAILY_RISK_FEATURES_SCHEMA,
)


log = logging.getLogger(__name__)


# ─── BigQuery Schema Definitions (must match gcp_batch_main.py) ────────
BRONZE_SCHEMA = {
    "fields": [
        {"name": "event_uuid", "type": "STRING", "mode": "REQUIRED"},
        {"name": "source_system", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "payload", "type": "JSON", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

SILVER_SCHEMA = {
    "fields": [
        {"name": "invoice_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "po_no", "type": "STRING", "mode": "NULLABLE"},
        {"name": "vendor_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "upc_no", "type": "STRING", "mode": "NULLABLE"},
        {"name": "invoice_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "invoice_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "bank_account_hash", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

GOLD_SCHEMA = {
    "fields": [
        {"name": "invoice_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "vendor_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "rule_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "severity", "type": "STRING", "mode": "REQUIRED"},
        {"name": "reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "evidence", "type": "JSON", "mode": "NULLABLE"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "fraud_score", "type": "INT64", "mode": "NULLABLE"},
        {"name": "alert_source", "type": "STRING", "mode": "NULLABLE"},
        {"name": "detected_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


# ─── Timestamp DoFns (add timestamps at processing time, not definition time) ───
class AddIngestedTimestamp(beam.DoFn):
    """DoFn to add ingested_at timestamp at processing time."""
    def process(self, element):
        element["ingested_at"] = datetime.now(timezone.utc).isoformat()
        yield element


class AddDetectedTimestamp(beam.DoFn):
    """DoFn to add detected_at timestamp at processing time."""
    def process(self, element):
        element["detected_at"] = datetime.now(timezone.utc).isoformat()
        yield element


# ─── BigQuery row converters (shared with batch) ────────────────────────
def silver_row(e: ErpInvoiceEvent) -> dict:
    return {
        "invoice_id":        e.invoice_id,
        "po_no":             e.po_no,
        "vendor_id":         e.vendor_id,
        "upc_no":            e.upc_no,
        "invoice_amount":    float(e.invoice_amount),
        "invoice_timestamp": e.invoice_timestamp.isoformat(),
        "bank_account_hash": e.bank_account_hash,
        "email_id":          e.email_id,
        "ingested_at":       None,  # Will be set by AddIngestedTimestamp DoFn
    }


def bronze_row(e: dict) -> dict:
    """Convert event to bronze row with JSON-serialized payload."""
    return {
        "event_uuid":      e["event_uuid"],
        "source_system":   e["source_system"],
        "event_type":      e["event_type"],
        "event_timestamp": e["event_timestamp"].isoformat(),
        "payload":         json.dumps(e["payload"]),
        "ingested_at":     e.get("ingested_at"),  # Added by AddIngestedTimestamp DoFn
    }


def gold_row(a: dict) -> dict:
    """Convert fraud alert dict to BigQuery gold row with proper types."""
    return {
        "invoice_id":   a["invoice_id"],
        "vendor_id":    a["vendor_id"],
        "rule_name":    a["rule_name"],
        "severity":     a["severity"],
        "reason":       a["reason"],
        "evidence":     json.dumps(a.get("evidence", {})),
        "window_start": a["window_start"].isoformat() if a.get("window_start") else None,
        "window_end":   a["window_end"].isoformat()   if a.get("window_end")   else None,
        "fraud_score":  a.get("fraud_score"),
        "alert_source": a.get("alert_source", "unknown"),
        "detected_at":  a.get("detected_at"),  # Added by AddDetectedTimestamp DoFn
    }


# ─── BigQuery baseline loader ───────────────────────────────────────────
def load_baseline_from_bq(project: str, dataset_fqn: str) -> dict:
    """One-shot read of vendor_90day_baseline."""
    from google.cloud import bigquery
    project_id, dataset = dataset_fqn.split(":")
    try:
        client = bigquery.Client(project=project_id)
        rows = client.query(f"""
            SELECT vendor_id, avg_invoice_amount, stddev_invoice_amount,
                   p99_invoice_amount, avg_daily_invoice_cnt
            FROM `{project_id}.{dataset}.vendor_90day_baseline`
        """).result()
        out = {
            r.vendor_id: {
                "avg":    float(r.avg_invoice_amount),
                "stddev": float(r.stddev_invoice_amount),
                "p99":    float(r.p99_invoice_amount),
                "daily":  int(r.avg_daily_invoice_cnt),
            }
            for r in rows
        }
        log.info("Loaded %d vendor baselines from BigQuery", len(out))
        return out
    except Exception as exc:  # noqa: BLE001
        log.critical("BQ baseline load FAILED — FALLBACK MODE: %s", exc)
        return {}


# ─── pipeline wiring ────────────────────────────────────────────────────
def build_argparser():
    p = argparse.ArgumentParser()
    p.add_argument("--wms_subscription", required=True, 
                   help="Pub/Sub subscription path: projects/PROJECT/subscriptions/SUB")
    p.add_argument("--erp_subscription", required=True, 
                   help="Pub/Sub subscription path: projects/PROJECT/subscriptions/SUB")
    p.add_argument("--bq_dataset", required=True, help="project:dataset")
    p.add_argument("--dedup_ttl_seconds",         type=int, default=3600)
    p.add_argument("--velocity_window_seconds",   type=int, default=600)
    p.add_argument("--velocity_period_seconds",   type=int, default=60)
    p.add_argument("--allowed_lateness_seconds",  type=int, default=3600)
    p.add_argument("--anomaly_stddev_threshold",  type=float, default=3.0)
    return p


def run(argv=None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s")
    parser = build_argparser()
    known_args, beam_args = parser.parse_known_args(argv)

    opts = PipelineOptions(beam_args)
    opts.view_as(SetupOptions).save_main_session = True
    opts.view_as(StandardOptions).streaming = True  # CRITICAL for Pub/Sub

    project = opts.get_all_options()["project"]
    bronze_table = f"{known_args.bq_dataset}.bronze_raw_events"
    silver_table = f"{known_args.bq_dataset}.silver_deduplicated_invoices"
    gold_table   = f"{known_args.bq_dataset}.gold_fraud_alerts"
    behavioral_features_table = f"{known_args.bq_dataset}.vendor_daily_behavioral_features"
    risk_features_table = f"{known_args.bq_dataset}.vendor_daily_risk_features"

    baseline_dict = load_baseline_from_bq(project, known_args.bq_dataset)

    with beam.Pipeline(options=opts) as p:
        baseline_si = p | "BaselineSI" >> beam.Create([baseline_dict])

        # WMS stream from Pub/Sub
        wms_raw = (
            p
            | "ReadPubSubWMS" >> ReadFromPubSub(
                subscription=known_args.wms_subscription,
                with_attributes=False
            )
        )

        wms = (
            wms_raw
            | "DecodePubSubWMS" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "ParseWms" >> beam.ParDo(ParseWmsLine())
                              .with_outputs(ParseWmsLine.DEAD_LETTER, main="ok")
        )

        # ERP stream from Pub/Sub
        erp_raw = (
            p
            | "ReadPubSubERP" >> ReadFromPubSub(
                subscription=known_args.erp_subscription,
                with_attributes=False
            )
        )

        erp = (
            erp_raw
            | "DecodePubSubERP" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "ParseErp" >> beam.ParDo(ParseErpLine())
                              .with_outputs(ParseErpLine.DEAD_LETTER, main="ok")
        )

        # BRONZE — all raw events from both WMS and ERP
        (
            (wms["ok"], erp["ok"])
            | "FlatBronze"        >> beam.Flatten()
            | "ToBronzeDict"      >> beam.Map(event_to_bronze_row)
            | "AddIngestedTime"   >> beam.ParDo(AddIngestedTimestamp())  # ✅ Add timestamp at processing time
            | "FmtBronze"         >> beam.Map(bronze_row)
            | "WriteBronze"       >> WriteToBigQuery(
                table=bronze_table,
                schema=BRONZE_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )

        # SILVER — stateful dedup + write
        deduped = (
            erp["ok"]
            | "GlobalForDedup" >> beam.WindowInto(beam.window.GlobalWindows())
            | "Dedup"          >> DeduplicateInvoices(known_args.dedup_ttl_seconds)
        )

        (
            deduped["unique"]
            | "FmtSilver"          >> beam.Map(silver_row)
            | "AddSilverTimestamp" >> beam.ParDo(AddIngestedTimestamp())  # ✅ Add timestamp at processing time
            | "WriteSilver"        >> WriteToBigQuery(
                table=silver_table,
                schema=SILVER_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )

        # GOLD — velocity + anomaly + fallback alerts
        timed = deduped["unique"] | "AssignTs" >> beam.ParDo(AssignEventTimestamp())

        velocity = (
            timed
            | "Velocity"    >> VelocityFraudCheck(
                window_s=known_args.velocity_window_seconds,
                period_s=known_args.velocity_period_seconds,
                lateness_s=known_args.allowed_lateness_seconds,
            )
            | "VelToGlobal" >> beam.WindowInto(
                beam.window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterProcessingTime(60)),  # Emit every 60s
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

        anomaly = (
            timed
            | "AnomGlobal" >> beam.WindowInto(
                beam.window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterProcessingTime(60)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
            | "Anomaly"    >> beam.ParDo(
                AnomalyCheckDoFn(known_args.anomaly_stddev_threshold),
                baseline=beam.pvalue.AsSingleton(baseline_si),
            )
        )

        (
            (velocity, anomaly)
            | "FlatGold"        >> beam.Flatten()
            | "AddDetectedTime" >> beam.ParDo(AddDetectedTimestamp())  # ✅ Add timestamp at processing time
            | "FmtGold"         >> beam.Map(gold_row)
            | "WriteGold"       >> WriteToBigQuery(
                table=gold_table,
                schema=GOLD_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )

        # ─── FEATURE ENGINEERING (ML Platform - Refactored) ──────────────────────────────
        # PRODUCTION ARCHITECTURE:
        #   Feature engineering is INDEPENDENT from Gold fraud detection layer.
        #   Streaming mode: Features computed continuously as events arrive.
        #
        # OUTPUTS:
        #   1. vendor_daily_behavioral_features (behavioral signals only)
        #   2. vendor_daily_risk_features (fraud labels only)
        #   3. vendor_daily_features (VIEW joining 1 + 2)
        
        # ─── BEHAVIORAL FEATURES (from silver invoices only) ────────────────────
        behavioral_features = (
            deduped["unique"]
            | "BuildBehavioralFeatures" >> BuildVendorDailyBehavioralFeatures()
        )
        
        (
            behavioral_features
            | "WriteBehavioralFeatures" >> WriteToBigQuery(
                table=behavioral_features_table,
                schema=VENDOR_DAILY_BEHAVIORAL_FEATURES_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )
        
        # ─── RISK FEATURES (from gold alerts only) ────────────────────────────
        gold_alerts = (
            (velocity, anomaly)
            | "FlatAlertsForRiskFeatures" >> beam.Flatten()
        )
        
        risk_features = (
            gold_alerts
            | "BuildRiskFeatures" >> BuildVendorDailyRiskFeatures()
        )
        
        (
            risk_features
            | "WriteRiskFeatures" >> WriteToBigQuery(
                table=risk_features_table,
                schema=VENDOR_DAILY_RISK_FEATURES_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )

    log.info("Streaming job will continue until manually stopped.")


if __name__ == "__main__":
    run(sys.argv[1:])
