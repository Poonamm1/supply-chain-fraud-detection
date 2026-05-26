"""
pipeline.main
=============
Entry-point for Phase 1 of the Supply Chain Fraud Detection pipeline.

Run locally:
    python -m pipeline.main \\
        --wms_input  data/wms_receiving.jsonl \\
        --erp_input  data/erp_invoices.jsonl

Pipeline DAG (ASCII)
--------------------
                                 ┌──── (dead_letter) ──► [log]
    WMS JSONL ─► ParseWms ───────┤
                                 └─► AssignTs ─┐
                                               ├─► to_bronze ─► WriteBronze
                                               │
    ERP JSONL ─► ParseErp ───────┐             │
                                 └─► AssignTs ─┤
                                               ├─► to_bronze ─► WriteBronze
                                               │
                                               └─► Dedup (stateful) ─► unique
                                                                       │
                                              ┌────────────────────────┤
                                              │                        │
                                              ▼                        ▼
                                       WriteSilver         ┌─ VelocityCheck ─┐
                                                           │                 │
                                                           └─ AnomalyCheck ──┤ (+side input)
                                                                             ▼
                                                                    WriteGoldAlert
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    DirectOptions,
    SetupOptions,
)

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
    load_vendor_baseline,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Supply Chain Fraud Detection — Phase 1")
    p.add_argument("--wms_input", default=None,
                   help="Path to WMS receiving JSONL (overrides WMS_INPUT_PATH env)")
    p.add_argument("--erp_input", default=None,
                   help="Path to ERP invoice JSONL (overrides ERP_INPUT_PATH env)")
    p.add_argument("--log_level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    # Catch everything else (Beam flags) and forward to PipelineOptions
    args, beam_args = p.parse_known_args(argv)
    args.beam_args = beam_args
    return args


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------
def build_pipeline_options(beam_args: List[str], cfg: PipelineConfig) -> PipelineOptions:
    """
    DirectRunner with streaming semantics enabled.

    Notes:
        * `--streaming` flips the windowing / watermark machinery on. Without
          it, ReadFromText is treated as bounded and your SlidingWindows fire
          exactly once at the end — defeating the whole exercise.
        * `direct_num_workers` parallelizes bundle processing. Mirrors the
          Dataflow worker count knob for parity.
    """
    opts = PipelineOptions(beam_args)
    opts.view_as(StandardOptions).runner = "DirectRunner"
    opts.view_as(StandardOptions).streaming = True
    opts.view_as(DirectOptions).direct_num_workers = cfg.direct_num_workers
    opts.view_as(DirectOptions).direct_running_mode = "multi_threading"
    # Pickle by value so closures over `cfg` survive serialization
    opts.view_as(SetupOptions).save_main_session = True
    return opts


def run(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
    )

    cfg = PipelineConfig()
    if args.wms_input:
        cfg = PipelineConfig(wms_input_path=args.wms_input,
                             erp_input_path=args.erp_input or cfg.erp_input_path)
    elif args.erp_input:
        cfg = PipelineConfig(erp_input_path=args.erp_input)

    logger.info("Starting pipeline with config: %s", cfg)

    # -------------------------------------------------------------------
    # Side-input data (loaded once, before the pipeline graph is built so
    # we can detect Postgres outages early and emit a CRITICAL log).
    # -------------------------------------------------------------------
    baseline_dict = load_vendor_baseline(cfg.postgres)

    options = build_pipeline_options(args.beam_args, cfg)
    with beam.Pipeline(options=options) as p:

        # ------------------------- Side input -------------------------
        # A singleton PCollection containing the baseline dict. Passing
        # via `pvalue.AsSingleton` materializes it on every worker once.
        baseline_si = (
            p
            | "CreateBaselineSI" >> beam.Create([baseline_dict])
        )

        # =====================================================================
        # STREAM 1 — WMS Receiving
        # =====================================================================
        wms_parsed = (
            p
            | "ReadWms"  >> beam.io.ReadFromText(cfg.wms_input_path)
            | "ParseWms" >> beam.ParDo(ParseWmsLine()).with_outputs(
                ParseWmsLine.DEAD_LETTER, main="ok")
        )
        wms_events = (
            wms_parsed.ok
            | "AssignTsWms" >> beam.ParDo(AssignEventTimestamp())
        )
        (
            wms_parsed[ParseWmsLine.DEAD_LETTER]
            | "LogBadWms" >> beam.Map(
                lambda e: logger.warning("WMS dead-letter: %s", e) or e)
        )

        # =====================================================================
        # STREAM 2 — ERP Invoices
        # =====================================================================
        erp_parsed = (
            p
            | "ReadErp"  >> beam.io.ReadFromText(cfg.erp_input_path)
            | "ParseErp" >> beam.ParDo(ParseErpLine()).with_outputs(
                ParseErpLine.DEAD_LETTER, main="ok")
        )
        erp_events = (
            erp_parsed.ok
            | "AssignTsErp" >> beam.ParDo(AssignEventTimestamp())
        )
        (
            erp_parsed[ParseErpLine.DEAD_LETTER]
            | "LogBadErp" >> beam.Map(
                lambda e: logger.warning("ERP dead-letter: %s", e) or e)
        )

        # =====================================================================
        # BRONZE — land everything raw, both sources.
        # =====================================================================
        (
            (wms_events, erp_events)
            | "FlattenBronze" >> beam.Flatten()
            | "ToBronzeRow"   >> beam.Map(lambda e: e.to_bronze_row())
            | "WriteBronze"   >> beam.ParDo(WriteBronzeEvent(cfg.postgres))
        )

        # =====================================================================
        # SILVER — deduplicated invoices.
        # Stateful DoFn requires keyed input + a global window (state survives
        # window boundaries). We re-window into FixedWindows downstream for the
        # velocity check.
        # =====================================================================
        deduped = (
            erp_events
            | "GlobalWindowForDedup" >> beam.WindowInto(beam.window.GlobalWindows())
            | "DedupInvoices" >> DeduplicateInvoices(cfg.dedup_ttl_seconds)
        )
        unique_invoices = deduped["unique"]

        # Duplicates -> log + metric. In Phase 2 these go to a Pub/Sub DLQ.
        (
            deduped["duplicates"]
            | "LogDuplicates" >> beam.Map(
                lambda e: logger.info("Duplicate invoice suppressed: %s", e.invoice_id) or e)
        )

        # Silver sink
        (
            unique_invoices
            | "ToSilverRow"  >> beam.Map(lambda e: e.to_silver_row())
            | "WriteSilver"  >> beam.ParDo(WriteSilverInvoice(cfg.postgres))
        )

        # =====================================================================
        # GOLD — fraud alerts (velocity + anomaly + fallback)
        # =====================================================================
        velocity_alerts = (
            unique_invoices
            | "VelocityCheck" >> VelocityFraudCheck(
                window_seconds=cfg.velocity_window_seconds,
                period_seconds=cfg.velocity_period_seconds,
                allowed_lateness=cfg.allowed_lateness_seconds,
            )
        )

        anomaly_alerts = (
            unique_invoices
            # Anomaly scoring is per-record — back to global window so we
            # don't accidentally drop late records inside a fixed window.
            | "GlobalWindowForAnomaly" >> beam.WindowInto(beam.window.GlobalWindows())
            | "AnomalyCheck" >> beam.ParDo(
                AnomalyCheckDoFn(cfg.anomaly_stddev_threshold),
                baseline=beam.pvalue.AsSingleton(baseline_si),
            )
        )

        (
            (velocity_alerts, anomaly_alerts)
            | "FlattenAlerts" >> beam.Flatten()
            | "WriteGold"     >> beam.ParDo(WriteGoldAlert(cfg.postgres))
        )

    logger.info("Pipeline finished cleanly ✅")
    return 0


if __name__ == "__main__":
    sys.exit(run())
