"""
pipeline.main — Phase 1 (Steps 4.1 → 5 wired up)
=================================================
Reads two JSONL streams, deduplicates invoices, scores velocity + anomaly
fraud, and lands everything in PostgreSQL across bronze/silver/gold layers.

Run from project root with venv active:
    export PG_PORT=5433
    python -m pipeline.main
"""
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from .config import PipelineConfig
from .transforms import (
    AnomalyCheckDoFn,
    AssignEventTimestamp,
    DeduplicateInvoices,
    ParseErpLine,
    ParseWmsLine,
    VelocityFraudCheck,
    WriteBronzeEvent,
    WriteGoldAlert,
    WriteSilverInvoice,
    event_to_bronze_row,
    load_vendor_baseline,
)


log = logging.getLogger(__name__)


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
    )
    cfg = PipelineConfig()
    log.info("Starting pipeline with WMS=%s ERP=%s pg_port=%s",
             cfg.wms_input_path, cfg.erp_input_path, cfg.postgres.port)

    # Side-input data is loaded ONCE up front. If Postgres is down we still
    # build the pipeline — every record will route to FALLBACK (by design).
    baseline_dict = load_vendor_baseline(cfg.postgres)

    opts = PipelineOptions()
    opts.view_as(StandardOptions).runner = "DirectRunner"

    with beam.Pipeline(options=opts) as p:

        # ===== Side input: vendor baseline =====
        baseline_si = p | "CreateBaselineSI" >> beam.Create([baseline_dict])

        # ===================================================================
        # STREAM 1 — WMS Receiving
        # ===================================================================
        wms = (
            p
            | "ReadWms"  >> beam.io.ReadFromText(cfg.wms_input_path)
            | "ParseWms" >> beam.ParDo(ParseWmsLine())
                              .with_outputs(ParseWmsLine.DEAD_LETTER, main="ok")
        )
        wms_events = wms["ok"]

        # ===================================================================
        # STREAM 2 — ERP Invoice
        # ===================================================================
        erp = (
            p
            | "ReadErp"  >> beam.io.ReadFromText(cfg.erp_input_path)
            | "ParseErp" >> beam.ParDo(ParseErpLine())
                              .with_outputs(ParseErpLine.DEAD_LETTER, main="ok")
        )
        erp_events = erp["ok"]

        # ===================================================================
        # BRONZE — land EVERY raw event from both sources
        # ===================================================================
        (
            (wms_events, erp_events)
            | "FlattenForBronze" >> beam.Flatten()
            | "ToBronzeRow"      >> beam.Map(event_to_bronze_row)
            | "WriteBronze"      >> beam.ParDo(WriteBronzeEvent(cfg.postgres))
        )

        # ===================================================================
        # SILVER — deduplicated invoices
        #   Stateful DoFn needs keyed input in a single window scope.
        # ===================================================================
        deduped = (
            erp_events
            | "GlobalWinForDedup" >> beam.WindowInto(beam.window.GlobalWindows())
            | "DedupInvoices"     >> DeduplicateInvoices(cfg.dedup_ttl_seconds)
        )
        unique_invoices = deduped["unique"]

        # Visibility: log every suppressed duplicate
        (
            deduped["duplicates"]
            | "LogDuplicates" >> beam.Map(
                lambda e: log.info("Duplicate invoice suppressed: %s", e.invoice_id) or e)
        )

        # Silver sink (idempotent on invoice_id)
        (
            unique_invoices
            | "WriteSilver" >> beam.ParDo(WriteSilverInvoice(cfg.postgres))
        )

        # ===================================================================
        # GOLD — fraud alerts (velocity + anomaly + fallback)
        # ===================================================================
        timed_invoices = unique_invoices | "AssignTs" >> beam.ParDo(AssignEventTimestamp())

        velocity_alerts = (
            timed_invoices
            | "VelocityCheck" >> VelocityFraudCheck(
                window_s=cfg.velocity_window_seconds,
                period_s=cfg.velocity_period_seconds,
                lateness_s=cfg.allowed_lateness_seconds,
            )
            # Re-window back to global so we can Flatten with anomaly_alerts.
            # All inputs to Flatten must share the same windowing strategy.
            | "VelToGlobal" >> beam.WindowInto(beam.window.GlobalWindows())
        )

        anomaly_alerts = (
            timed_invoices
            # Back to global window so anomaly scoring sees every record once
            | "GlobalWinForAnomaly" >> beam.WindowInto(beam.window.GlobalWindows())
            | "AnomalyCheck"        >> beam.ParDo(
                AnomalyCheckDoFn(cfg.anomaly_stddev_threshold),
                baseline=beam.pvalue.AsSingleton(baseline_si),
            )
        )

        (
            (velocity_alerts, anomaly_alerts)
            | "FlattenAlerts" >> beam.Flatten()
            | "WriteGold"     >> beam.ParDo(WriteGoldAlert(cfg.postgres))
        )

    log.info("Pipeline finished cleanly ✅")


if __name__ == "__main__":
    run()
