"""
pipeline.transforms
===================
All DoFns and PTransforms for the fraud detection pipeline.

Roadmap (now complete):
    Step 4.1 → ParseWmsLine, ParseErpLine          (parsing + dead-letter)
    Step 4.2 → DeduplicateInvoicesDoFn             (stateful, with TTL timer)
    Step 4.3 → AssignEventTimestamp, VelocityFraudCheck (sliding window)
    Step 4.4 → load_vendor_baseline, AnomalyCheckDoFn   (side input + fallback)
    Step 5   → _BatchedPostgresWriter, WriteBronze/Silver/Gold (batched sinks)
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms import window
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.timestamp import Duration, Timestamp

from .schemas import ErpInvoiceEvent, WmsReceivingEvent


log = logging.getLogger(__name__)


# =============================================================================
# STEP 4.1 — PARSING (with dead-letter side outputs)
# =============================================================================
class ParseErpLine(beam.DoFn):
    DEAD_LETTER = "dead_letter"

    def process(self, line: str):
        try:
            yield ErpInvoiceEvent.from_json(line)
        except Exception as exc:                                  # noqa: BLE001
            log.warning("Bad ERP payload: %s | err=%s", line[:120], exc)
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {"raw": line, "err": str(exc)})


class ParseWmsLine(beam.DoFn):
    DEAD_LETTER = "dead_letter"

    def process(self, line: str):
        try:
            yield WmsReceivingEvent.from_json(line)
        except Exception as exc:                                  # noqa: BLE001
            log.warning("Bad WMS payload: %s | err=%s", line[:120], exc)
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {"raw": line, "err": str(exc)})


# =============================================================================
# STEP 4.2 — STATEFUL DEDUPLICATION (TTL-bounded memory per invoice_id)
# =============================================================================
class DeduplicateInvoicesDoFn(beam.DoFn):
    DUPLICATES   = "duplicates"
    SEEN_STATE   = ReadModifyWriteStateSpec("seen", StrUtf8Coder())
    EXPIRY_TIMER = TimerSpec("expiry", beam.TimeDomain.WATERMARK)

    def __init__(self, ttl_seconds: int):
        self._ttl = ttl_seconds

    def process(
        self,
        element,
        seen=beam.DoFn.StateParam(SEEN_STATE),
        timer=beam.DoFn.TimerParam(EXPIRY_TIMER),
        ts=beam.DoFn.TimestampParam,
    ):
        invoice_id, event = element
        if seen.read() == "1":
            yield pvalue.TaggedOutput(self.DUPLICATES, event)
            return
        seen.write("1")
        timer.set(Timestamp(ts.micros / 1_000_000) + Duration(self._ttl))
        yield event

    @on_timer(EXPIRY_TIMER)
    def _gc(self, seen=beam.DoFn.StateParam(SEEN_STATE)):
        seen.clear()        # free memory after TTL


class DeduplicateInvoices(beam.PTransform):
    """Convenience wrapper: keys by invoice_id, runs stateful dedup."""
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


# =============================================================================
# STEP 4.3 — EVENT-TIME PROMOTION + SLIDING WINDOW VELOCITY CHECK
# =============================================================================
class AssignEventTimestamp(beam.DoFn):
    """Promote payload event_time -> Beam event-time. #1 streaming must-do."""
    def process(self, element):
        yield window.TimestampedValue(element, element.event_time.timestamp())


class FlagDuplicateAmounts(beam.DoFn):
    """Within a vendor+window group, flag any amount appearing >= MIN_REPEATS times."""
    MIN_REPEATS = 3

    def process(self, element, w=beam.DoFn.WindowParam):
        vendor_id, events = element
        buckets: Dict[str, List[ErpInvoiceEvent]] = {}
        for ev in events:
            key = f"{ev.invoice_amount:.2f}"
            buckets.setdefault(key, []).append(ev)

        for amt, grp in buckets.items():
            if len(grp) < self.MIN_REPEATS:
                continue
            for ev in grp:
                yield {
                    "invoice_id": ev.invoice_id,
                    "vendor_id":  vendor_id,
                    "rule_name":  "VELOCITY",
                    "severity":   "HIGH",
                    "reason":     f"{len(grp)}x ${amt} in 10-min window",
                    "evidence":   {"amount": float(amt),
                                   "occurrences": len(grp),
                                   "invoice_ids": [e.invoice_id for e in grp]},
                    "window_start": _window_dt(w.start),
                    "window_end":   _window_dt(w.end),
                }


class VelocityFraudCheck(beam.PTransform):
    def __init__(self, window_s: int = 600, period_s: int = 60, lateness_s: int = 3600):
        super().__init__()
        self._w, self._p, self._l = window_s, period_s, lateness_s

    def expand(self, pcoll):
        return (
            pcoll
            | "Sliding"        >> beam.WindowInto(
                window.SlidingWindows(self._w, self._p),
                allowed_lateness=self._l,
            )
            | "KeyByVendor"    >> beam.Map(lambda e: (e.vendor_id, e))
            | "GroupByVendor"  >> beam.GroupByKey()
            | "DetectVelocity" >> beam.ParDo(FlagDuplicateAmounts())
        )


# =============================================================================
# STEP 4.4 — VENDOR BASELINE (side input) + ANOMALY DETECTION WITH FALLBACK
# =============================================================================
def load_vendor_baseline(pg) -> Dict[str, Dict[str, float]]:
    """One-shot load of vendor_90day_baseline. On failure: empty dict + CRITICAL log
    -> AnomalyCheckDoFn will route every record to the FALLBACK path."""
    import psycopg2
    out: Dict[str, Dict[str, float]] = {}
    try:
        with psycopg2.connect(pg.dsn) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT vendor_id, avg_invoice_amount, stddev_invoice_amount,
                       p99_invoice_amount, avg_daily_invoice_cnt
                FROM vendor_90day_baseline
            """)
            for vid, avg, sd, p99, daily in cur.fetchall():
                out[vid] = {"avg": float(avg), "stddev": float(sd),
                            "p99": float(p99), "daily": int(daily)}
        log.info("Loaded vendor baseline: %d vendors", len(out))
    except Exception as exc:                                      # noqa: BLE001
        log.critical("Baseline load FAILED — running in FALLBACK MODE: %s", exc)
    return out


class AnomalyCheckDoFn(beam.DoFn):
    FALLBACK = "Review Required - Fallback Mode"

    def __init__(self, stddev_threshold: float = 3.0):
        self._z = stddev_threshold

    def process(self, event, baseline=beam.DoFn.SideInputParam):
        try:
            stats = baseline.get(event.vendor_id)
            if stats is None:
                log.warning("No baseline for vendor=%s — FALLBACK", event.vendor_id)
                yield self._fallback(event, "No baseline available")
                return

            avg, sd = stats["avg"], stats["stddev"]
            if sd <= 0:
                yield self._fallback(event, "Degenerate stddev")
                return

            z = (event.invoice_amount - avg) / sd
            if abs(z) >= self._z:
                yield {
                    "invoice_id":   event.invoice_id,
                    "vendor_id":    event.vendor_id,
                    "rule_name":    "ANOMALY",
                    "severity":     "CRITICAL" if abs(z) >= 5 else "HIGH",
                    "reason":       f"{z:+.2f}σ from avg ${avg:.2f}",
                    "evidence":     {"amount": event.invoice_amount,
                                     "baseline_avg": avg,
                                     "z_score": round(z, 3)},
                    "window_start": None,
                    "window_end":   None,
                }
        except Exception as exc:                                  # noqa: BLE001
            log.critical("Scoring crashed for invoice=%s: %s",
                         event.invoice_id, exc)
            yield self._fallback(event, f"Scoring error: {exc}")

    def _fallback(self, event, reason: str) -> Dict[str, Any]:
        return {
            "invoice_id":   event.invoice_id,
            "vendor_id":    event.vendor_id,
            "rule_name":    "FALLBACK",
            "severity":     "MEDIUM",
            "reason":       f"{self.FALLBACK}: {reason}",
            "evidence":     {"amount": event.invoice_amount},
            "window_start": None,
            "window_end":   None,
        }


# =============================================================================
# STEP 5 — POSTGRES SINKS (batched, idempotent, fail-soft)
# =============================================================================
class _BatchedPostgresWriter(beam.DoFn):
    """Base class: batch rows + flush via psycopg2.extras.execute_values.

    Subclasses override INSERT_SQL and _to_row(). ON CONFLICT DO NOTHING in
    the SQL makes at-least-once delivery effectively-once at the sink.
    """
    BATCH_SIZE: int = 500
    INSERT_SQL: str = ""

    def __init__(self, pg):
        self._pg = pg
        self._conn = None
        self._buf: List[Tuple] = []

    def _to_row(self, element: Dict[str, Any]) -> Tuple:          # pragma: no cover
        raise NotImplementedError

    # ----- lifecycle -----
    def setup(self):
        import psycopg2
        self._conn = psycopg2.connect(self._pg.dsn)
        self._conn.autocommit = False

    def teardown(self):
        try:
            if self._buf:
                self._flush()
        finally:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def finish_bundle(self):
        if self._buf:
            self._flush()

    # ----- per-element -----
    def process(self, element):
        self._buf.append(self._to_row(element))
        if len(self._buf) >= self.BATCH_SIZE:
            self._flush()

    # ----- flush -----
    def _flush(self):
        from psycopg2.extras import execute_values
        rows, self._buf = self._buf, []
        try:
            with self._conn.cursor() as cur:
                execute_values(cur, self.INSERT_SQL, rows, page_size=self.BATCH_SIZE)
            self._conn.commit()
        except Exception as exc:                                  # noqa: BLE001
            log.error("Postgres flush failed (%d rows): %s", len(rows), exc)
            self._conn.rollback()


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
             invoice_timestamp, bank_account_hash, email_id)
        VALUES %s
        ON CONFLICT (invoice_id) DO NOTHING
    """

    def _to_row(self, e: ErpInvoiceEvent) -> Tuple:
        return (
            e.invoice_id, e.po_no, e.vendor_id, e.upc_no,
            e.invoice_amount, e.invoice_timestamp,
            e.bank_account_hash, e.email_id,
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


# =============================================================================
# helpers
# =============================================================================
def _window_dt(beam_ts) -> "datetime | None":
    try:
        return datetime.fromtimestamp(float(beam_ts), tz=timezone.utc)
    except Exception:                                             # noqa: BLE001
        return None


def event_to_bronze_row(e) -> Dict[str, Any]:
    """Project either event type into the bronze schema."""
    if isinstance(e, ErpInvoiceEvent):
        return {
            "event_uuid":      e.event_uuid,
            "source_system":   "ERP",
            "event_type":      "INVOICE",
            "event_timestamp": e.invoice_timestamp,
            "payload": {
                "invoice_id":        e.invoice_id,
                "po_no":             e.po_no,
                "vendor_id":         e.vendor_id,
                "invoice_amount":    e.invoice_amount,
                "upc_no":            e.upc_no,
                "email_id":          e.email_id,
                "bank_account_hash": e.bank_account_hash,
            },
        }
    if isinstance(e, WmsReceivingEvent):
        return {
            "event_uuid":      e.event_uuid,
            "source_system":   "WMS",
            "event_type":      "RECEIVING",
            "event_timestamp": e.received_timestamp,
            "payload": {
                "wh_id":        e.wh_id,
                "po_no":        e.po_no,
                "vendor_id":    e.vendor_id,
                "upc_no":       e.upc_no,
                "qty_received": e.qty_received,
            },
        }
    raise TypeError(f"Unknown event type: {type(e)}")
