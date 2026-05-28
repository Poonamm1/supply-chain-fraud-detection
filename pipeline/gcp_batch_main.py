"""
pipeline.gcp_batch_main — GCP variant of the Phase 1 pipeline.
==============================================================
Identical fraud-detection logic to pipeline.main, but:
    * Reads from GCS instead of local files
    * Writes to BigQuery instead of PostgreSQL
    * Runs as a BATCH job on Dataflow (exits when done -> no idle bills)
    * Loads vendor baseline from BigQuery (one-shot at job start)

Run via gcp/trigger_pipeline.sh — never invoke directly in production.

Cost model:
    * Dataflow batch on e2-small with max_num_workers=2:
        ~$0.05 - $0.10 per run for our row volumes
    * BigQuery load jobs: FREE (vs $0.05/GB for streaming inserts)
    * GCS lifecycle deletes inputs after 7 days
"""
import argparse
import logging
import os
import sys

# Bootstrap: when Dataflow runs this script directly (not via `python -m`),
# the parent directory isn't on sys.path. Fix it so `from pipeline.transforms`
# works. This ONLY runs when __package__ is None (i.e., direct execution).
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# We reuse the EXACT same transforms — that's the whole point of clean
# separation between business logic and I/O. Only the edges change.
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


log = logging.getLogger(__name__)


# ─── BigQuery row converters ────────────────────────────────────────────
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
    }


def bronze_row(e: dict) -> dict:
    """event_to_bronze_row already gives us 90% — just stringify timestamp."""
    return {
        "event_uuid":      e["event_uuid"],
        "source_system":   e["source_system"],
        "event_type":      e["event_type"],
        "event_timestamp": e["event_timestamp"].isoformat(),
        "payload":         e["payload"],  # BigQuery JSON column accepts dict
    }


def gold_row(a: dict) -> dict:
    return {
        "invoice_id":   a["invoice_id"],
        "vendor_id":    a["vendor_id"],
        "rule_name":    a["rule_name"],
        "severity":     a["severity"],
        "reason":       a["reason"],
        "evidence":     a.get("evidence"),
        "window_start": a["window_start"].isoformat() if a.get("window_start") else None,
        "window_end":   a["window_end"].isoformat()   if a.get("window_end")   else None,
        "fraud_score":  a.get("fraud_score"),
        "alert_source": a.get("alert_source", "unknown"),
    }


# ─── BigQuery baseline loader (replaces psycopg2 version) ───────────────
def load_baseline_from_bq(project: str, dataset_fqn: str) -> dict:
    """One-shot read of vendor_90day_baseline. On failure: empty dict -> FALLBACK
    mode kicks in (same robust behavior as Phase 1)."""
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
    p.add_argument("--wms_input",  required=True, help="GCS path to WMS JSONL")
    p.add_argument("--erp_input",  required=True, help="GCS path to ERP JSONL")
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

    project = opts.get_all_options()["project"]
    bronze_table = f"{known_args.bq_dataset}.bronze_raw_events"
    silver_table = f"{known_args.bq_dataset}.silver_deduplicated_invoices"
    gold_table   = f"{known_args.bq_dataset}.gold_fraud_alerts"

    baseline_dict = load_baseline_from_bq(project, known_args.bq_dataset)

    with beam.Pipeline(options=opts) as p:
        baseline_si = p | "BaselineSI" >> beam.Create([baseline_dict])

        # WMS stream
        wms = (
            p
            | "ReadWms"  >> beam.io.ReadFromText(known_args.wms_input)
            | "ParseWms" >> beam.ParDo(ParseWmsLine())
                              .with_outputs(ParseWmsLine.DEAD_LETTER, main="ok")
        )

        # ERP stream
        erp = (
            p
            | "ReadErp"  >> beam.io.ReadFromText(known_args.erp_input)
            | "ParseErp" >> beam.ParDo(ParseErpLine())
                              .with_outputs(ParseErpLine.DEAD_LETTER, main="ok")
        )

        # BRONZE — all raw events (idempotent via WRITE_APPEND + load jobs)
        (
            (wms["ok"], erp["ok"])
            | "FlatBronze"   >> beam.Flatten()
            | "ToBronzeDict" >> beam.Map(event_to_bronze_row)
            | "FmtBronze"    >> beam.Map(bronze_row)
            | "WriteBronze"  >> WriteToBigQuery(
                table=bronze_table,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.FILE_LOADS,   # FREE (vs streaming)
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
            | "FmtSilver"   >> beam.Map(silver_row)
            | "WriteSilver" >> WriteToBigQuery(
                table=silver_table,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.FILE_LOADS,
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
            | "VelToGlobal" >> beam.WindowInto(beam.window.GlobalWindows())
        )

        anomaly = (
            timed
            | "AnomGlobal" >> beam.WindowInto(beam.window.GlobalWindows())
            | "Anomaly"    >> beam.ParDo(
                AnomalyCheckDoFn(known_args.anomaly_stddev_threshold),
                baseline=beam.pvalue.AsSingleton(baseline_si),
            )
        )

        (
            (velocity, anomaly)
            | "FlatGold"  >> beam.Flatten()
            | "FmtGold"   >> beam.Map(gold_row)
            | "WriteGold" >> WriteToBigQuery(
                table=gold_table,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.FILE_LOADS,
            )
        )

    log.info("Batch job finished ✅ — Dataflow worker will now scale to zero.")


if __name__ == "__main__":
    run(sys.argv[1:])
