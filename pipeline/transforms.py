"""
pipeline.transforms
===================
The business-logic core of the fraud detection pipeline.

Design goals
------------
* **SRP** — one transform, one responsibility. Compose, don't conflate.
* **Stateless where possible, stateful only when justified** (dedup needs
  per-key memory; anomaly detection does not).
* **Side-effects are isolated** to the sink DoFns at the bottom — every other
  transform is a pure function of its input, which makes the whole graph
  trivially unit-testable.
* **Fail soft, alert loud** — we never let a single bad record kill the
  pipeline. Errors get routed to dead-letter PCollections or fallback paths.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms import window
from apache_beam.coders import StrUtf8Coder
from apache_beam.utils.timestamp import Duration, Timestamp

from .config import PipelineConfig, PostgresConfig
from .schemas import ErpInvoiceEvent, WmsReceivingEvent


logger = logging.getLogger(__name__)


# ===========================================================================
# 1. PARSE & ENVELOPE
# ===========================================================================
class ParseWmsLine(beam.DoFn):
    """Parse a raw JSONL line into a WmsReceivingEvent.

    Bad records are tagged to a dead-letter output instead of crashing the
    worker — a single malformed line should NEVER stall a 10k-msg/sec pipeline.
    """
    DEAD_LETTER = "dead_letter"

    def process(self, line: str):
        try:
            yield WmsReceivingEvent.from_json(line)
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Bad WMS payload: %s | err=%s", line[:200], exc)
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {"raw": line, "err": str(exc)})


class ParseErpLine(beam.DoFn):
    """Same pattern as ParseWmsLine — see above for rationale."""
    DEAD_LETTER = "dead_letter"

    def process(self, line: str):
        try:
            yield ErpInvoiceEvent.from_json(line)
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Bad ERP payload: %s | err=%s", line[:200], exc)
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {"raw": line, "err": str(exc)})


class AssignEventTimestamp(beam.DoFn):
    """Promote the *event time* embedded in the payload to Beam's event time.

    DirectRunner can't pull this from PubSub metadata (we have no PubSub yet)
    so we set it explicitly. This is the single most important step for any
    streaming pipeline — get this wrong and your windowing is a lie.
    """
    def process(self, element):
        ts = element.event_time
        yield window.TimestampedValue(element, ts.timestamp())


# ===========================================================================
# 2. STATEFUL DEDUPLICATION (invoice_id)
# ===========================================================================
class DeduplicateInvoicesDoFn(beam.DoFn):
    """
    Stateful per-key dedup with a TTL timer.

    Why stateful (not just GroupByKey)?
        * GBK only dedupes *within a window*. Real-world duplicates from
          retries can land hours apart — we need cross-window memory.
        * A ReadModifyWriteState + timer gives us O(1) lookups and bounded
          memory (state is GC'd after `dedup_ttl_seconds`).

    Input  : KV[invoice_id, ErpInvoiceEvent]
    Output : ErpInvoiceEvent  (first sighting only)
             pvalue.TaggedOutput('duplicates', ErpInvoiceEvent)
    """
    DUPLICATES = "duplicates"

    # State cell: a single bool "have we seen this invoice_id before?"
    SEEN_STATE = ReadModifyWriteStateSpec("seen", StrUtf8Coder())
    EXPIRY_TIMER = TimerSpec("expiry", beam.TimeDomain.WATERMARK)

    def __init__(self, ttl_seconds: int):
        self._ttl_seconds = ttl_seconds

    def process(
        self,
        element: Tuple[str, ErpInvoiceEvent],
        seen_state=beam.DoFn.StateParam(SEEN_STATE),
        expiry_timer=beam.DoFn.TimerParam(EXPIRY_TIMER),
        timestamp=beam.DoFn.TimestampParam,
    ):
        invoice_id, event = element
        seen = seen_state.read()
        if seen == "1":
            # Duplicate detected — route to side output for audit + metrics
            yield pvalue.TaggedOutput(self.DUPLICATES, event)
            return

        # First sighting — mark seen and schedule garbage collection
        seen_state.write("1")
        expiry_timer.set(Timestamp(timestamp.micros / 1_000_000) + Duration(self._ttl_seconds))
        yield event

    @on_timer(EXPIRY_TIMER)
    def _on_expiry(self, seen_state=beam.DoFn.StateParam(SEEN_STATE)):
        # Free the state cell — bounded memory FTW
        seen_state.clear()


class DeduplicateInvoices(beam.PTransform):
    """Convenience wrapper so main.py reads like English."""
    def __init__(self, ttl_seconds: int):
        super().__init__()
        self._ttl = ttl_seconds

    def expand(self, pcoll):
        return (
            pcoll
            | "KeyByInvoiceId" >> beam.Map(lambda e: (e.invoice_id, e))
            | "StatefulDedup"  >> beam.ParDo(
                DeduplicateInvoicesDoFn(self._ttl)
            ).with_outputs(DeduplicateInvoicesDoFn.DUPLICATES, main="unique")
        )


# ===========================================================================
# 3. VELOCITY FRAUD CHECK (10-minute sliding window, by vendor_id)
# ===========================================================================
class FlagDuplicateAmounts(beam.DoFn):
    """
    Inside a single (vendor_id, window) group, look for identical
    invoice_amounts emitted in quick succession. This is the classic
    "test charge" or "split-invoice fraud" pattern.

    A single vendor legitimately repeating an amount is OK; we only flag
    when the same amount appears `>= MIN_REPEATS` times within the window.
    """
    MIN_REPEATS = 3

    def process(
        self,
        element: Tuple[str, Iterable[ErpInvoiceEvent]],
        window_param=beam.DoFn.WindowParam,
    ):
        vendor_id, events = element
        events_list: List[ErpInvoiceEvent] = list(events)

        # Bucket by amount — using rounded float keys to avoid FP drift
        buckets: Dict[str, List[ErpInvoiceEvent]] = {}
        for ev in events_list:
            key = f"{ev.invoice_amount:.2f}"
            buckets.setdefault(key, []).append(ev)

        for amount_key, group in buckets.items():
            if len(group) < self.MIN_REPEATS:
                continue

            # Build one alert per offending invoice — so case management can
            # link 1:1 from alert -> invoice without a fan-out join.
            for ev in group:
                yield {
                    "invoice_id":   ev.invoice_id,
                    "vendor_id":    vendor_id,
                    "rule_name":    "VELOCITY",
                    "severity":     "HIGH",
                    "reason":       (
                        f"{len(group)} invoices of ${amount_key} within "
                        f"10-min window — possible split/test-charge fraud"
                    ),
                    "evidence": {
                        "amount":      float(amount_key),
                        "occurrences": len(group),
                        "invoice_ids": [e.invoice_id for e in group],
                    },
                    "window_start": _window_dt(window_param.start),
                    "window_end":   _window_dt(window_param.end),
                }


class VelocityFraudCheck(beam.PTransform):
    """
    10-minute sliding window, advancing every `period_seconds`.

    The sliding window is intentionally chatty — every invoice will appear in
    `window_size / period` windows, giving us low-latency detection at the
    cost of duplicate alerts that the gold layer (or a downstream Dataflow
    job in Phase 2) can collapse with a `latest by (invoice_id, rule_name)`
    materialized view.
    """
    def __init__(self, window_seconds: int, period_seconds: int, allowed_lateness: int):
        super().__init__()
        self._w = window_seconds
        self._p = period_seconds
        self._lateness = allowed_lateness

    def expand(self, pcoll):
        return (
            pcoll
            | "SlidingWindow" >> beam.WindowInto(
                window.SlidingWindows(size=self._w, period=self._p),
                allowed_lateness=self._lateness,
                trigger=None,                 # default trigger = on watermark
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
            )
            | "KeyByVendor"   >> beam.Map(lambda e: (e.vendor_id, e))
            | "GroupByVendor" >> beam.GroupByKey()
            | "DetectVelocity" >> beam.ParDo(FlagDuplicateAmounts())
        )


# ===========================================================================
# 4. ANOMALY DETECTION (side input from Postgres + fallback)
# ===========================================================================
def load_vendor_baseline(pg: PostgresConfig) -> Dict[str, Dict[str, float]]:
    """
    Pull the entire vendor_90day_baseline table into a dict for use as a
    side input. At 50k vendors x ~64 bytes/row this fits comfortably in
    worker memory (<5 MB) and avoids a per-record DB roundtrip.

    Side-input refresh is out of scope for Phase 1 — Phase 2 will swap this
    for a slowly-changing side input fed by a periodic Impulse.
    """
    import psycopg2                                              # local import — only the side input loader needs it
    out: Dict[str, Dict[str, float]] = {}
    try:
        with psycopg2.connect(pg.dsn) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT vendor_id, avg_invoice_amount, stddev_invoice_amount,
                       p99_invoice_amount, avg_daily_invoice_cnt
                FROM vendor_90day_baseline
            """)
            for vid, avg, sd, p99, daily in cur.fetchall():
                out[vid] = {
                    "avg":   float(avg),
                    "stddev": float(sd),
                    "p99":   float(p99),
                    "daily": int(daily),
                }
        logger.info("Loaded vendor baseline: %d vendors", len(out))
    except Exception as exc:                                     # noqa: BLE001
        # Empty baseline -> the AnomalyCheckDoFn will route everything to
        # fallback. That's the *correct* behavior: we'd rather over-flag
        # for human review than silently miss fraud.
        logger.critical(
            "Failed to load vendor baseline from Postgres: %s "
            "— pipeline will run in FALLBACK MODE for all records", exc,
        )
    return out


class AnomalyCheckDoFn(beam.DoFn):
    """
    Z-score anomaly detection vs the vendor's 90-day baseline.

    Three outcomes:
        * baseline OK + within threshold -> no alert
        * baseline OK + outside threshold -> 'ANOMALY' alert
        * baseline MISSING (key absent or load failed) -> 'FALLBACK' alert
          ("Review Required - Fallback Mode")
    """
    FALLBACK_REASON = "Review Required - Fallback Mode"

    def __init__(self, stddev_threshold: float):
        self._z = stddev_threshold

    def process(
        self,
        event: ErpInvoiceEvent,
        baseline: Dict[str, Dict[str, float]] = beam.DoFn.SideInputParam,
    ):
        # ------------------------------------------------------------------
        # Robust try/except around the entire scoring path. Even a malformed
        # baseline row (NaN stddev, etc.) must not crash the pipeline.
        # ------------------------------------------------------------------
        try:
            stats = baseline.get(event.vendor_id)
            if stats is None:
                # Unknown vendor OR baseline load failed -> fallback
                logger.warning(
                    "No baseline for vendor=%s invoice=%s — emitting FALLBACK alert",
                    event.vendor_id, event.invoice_id,
                )
                yield self._fallback_alert(event, reason="No baseline available for vendor")
                return

            avg, sd = stats["avg"], stats["stddev"]
            if sd <= 0:
                yield self._fallback_alert(event, reason="Degenerate stddev in baseline")
                return

            z = (event.invoice_amount - avg) / sd
            if abs(z) >= self._z:
                yield {
                    "invoice_id": event.invoice_id,
                    "vendor_id":  event.vendor_id,
                    "rule_name":  "ANOMALY",
                    "severity":   "CRITICAL" if abs(z) >= 5 else "HIGH",
                    "reason": (
                        f"Invoice amount ${event.invoice_amount:.2f} is "
                        f"{z:+.2f}σ from 90-day avg ${avg:.2f}"
                    ),
                    "evidence": {
                        "amount":   event.invoice_amount,
                        "baseline_avg":    avg,
                        "baseline_stddev": sd,
                        "z_score":         round(z, 3),
                    },
                    "window_start": None,
                    "window_end":   None,
                }
        except Exception as exc:                                  # noqa: BLE001
            # Belt-and-suspenders catch-all: log CRITICAL and fall back so
            # we never drop a record on the floor.
            logger.critical(
                "Anomaly scoring threw for invoice=%s vendor=%s: %s",
                event.invoice_id, event.vendor_id, exc,
            )
            yield self._fallback_alert(event, reason=f"Scoring error: {exc}")

    def _fallback_alert(self, event: ErpInvoiceEvent, reason: str) -> Dict[str, Any]:
        return {
            "invoice_id": event.invoice_id,
            "vendor_id":  event.vendor_id,
            "rule_name":  "FALLBACK",
            "severity":   "MEDIUM",
            "reason":     f"{self.FALLBACK_REASON}: {reason}",
            "evidence": {
                "amount":     event.invoice_amount,
                "po_no":      event.po_no,
                "email_id":   event.email_id,
            },
            "window_start": None,
            "window_end":   None,
        }


# ===========================================================================
# 5. POSTGRES SINKS — batched, idempotent, fail-soft
# ===========================================================================
class _BatchedPostgresWriter(beam.DoFn):
    """
    Base class for all Postgres-writing DoFns.

    * Opens ONE connection per worker bundle (psycopg2 isn't thread-safe but
      Beam bundles are single-threaded, so we're fine).
    * Batches inserts with `execute_values` for ~10-50x throughput vs row-at-a-time.
    * Uses ON CONFLICT DO NOTHING for idempotency — at-least-once delivery
      from upstream becomes effectively-once at the sink.
    """
    BATCH_SIZE = 500

    def __init__(self, pg: PostgresConfig):
        self._pg = pg
        self._conn = None
        self._buffer: List[Tuple] = []

    # SQL + row-projection are subclass concerns
    INSERT_SQL: str = ""
    def _to_row(self, element: Dict[str, Any]) -> Tuple:           # pragma: no cover
        raise NotImplementedError

    # ----- lifecycle ------------------------------------------------------
    def setup(self):
        import psycopg2
        self._conn = psycopg2.connect(self._pg.dsn)
        self._conn.autocommit = False

    def teardown(self):
        try:
            if self._buffer:
                self._flush()
        finally:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def finish_bundle(self):
        if self._buffer:
            self._flush()

    # ----- per-element ----------------------------------------------------
    def process(self, element):
        self._buffer.append(self._to_row(element))
        if len(self._buffer) >= self.BATCH_SIZE:
            self._flush()
        # We do NOT yield — sinks are terminal.

    # ----- flush ----------------------------------------------------------
    def _flush(self):
        from psycopg2.extras import execute_values
        rows, self._buffer = self._buffer, []
        try:
            with self._conn.cursor() as cur:
                execute_values(cur, self.INSERT_SQL, rows, page_size=self.BATCH_SIZE)
            self._conn.commit()
        except Exception as exc:                                  # noqa: BLE001
            logger.error("Postgres flush failed (%d rows): %s", len(rows), exc)
            self._conn.rollback()
            # In prod we'd push these to a DLQ topic; here we just log+drop.


class WriteBronzeEvent(_BatchedPostgresWriter):
    INSERT_SQL = """
        INSERT INTO bronze_raw_events
            (event_uuid, source_system, event_type, event_timestamp, payload)
        VALUES %s
        ON CONFLICT (event_uuid) DO NOTHING
    """
    def _to_row(self, e: Dict[str, Any]) -> Tuple:
        return (
            e["event_uuid"], e["source_system"], e["event_type"],
            e["event_timestamp"], json.dumps(e["payload"], default=str),
        )


class WriteSilverInvoice(_BatchedPostgresWriter):
    INSERT_SQL = """
        INSERT INTO silver_deduplicated_invoices
            (invoice_id, po_no, vendor_id, upc_no, invoice_amount,
             invoice_timestamp, bank_account_hash, email_id, dedup_window_start)
        VALUES %s
        ON CONFLICT (invoice_id) DO NOTHING
    """
    def _to_row(self, e: Dict[str, Any]) -> Tuple:
        return (
            e["invoice_id"], e["po_no"], e["vendor_id"], e.get("upc_no"),
            e["invoice_amount"], e["invoice_timestamp"],
            e["bank_account_hash"], e.get("email_id"),
            e.get("dedup_window_start"),
        )


class WriteGoldAlert(_BatchedPostgresWriter):
    INSERT_SQL = """
        INSERT INTO gold_fraud_alerts
            (invoice_id, vendor_id, rule_name, severity, reason,
             evidence, window_start, window_end)
        VALUES %s
    """
    def _to_row(self, a: Dict[str, Any]) -> Tuple:
        return (
            a["invoice_id"], a["vendor_id"], a["rule_name"], a["severity"],
            a["reason"], json.dumps(a["evidence"], default=str),
            a.get("window_start"), a.get("window_end"),
        )


# ===========================================================================
# 6. HELPERS
# ===========================================================================
def _window_dt(beam_ts) -> Optional[datetime]:
    """Convert a Beam Timestamp/IntervalWindow boundary to a tz-aware datetime."""
    try:
        return datetime.fromtimestamp(float(beam_ts), tz=timezone.utc)
    except Exception:                                            # noqa: BLE001
        return None
