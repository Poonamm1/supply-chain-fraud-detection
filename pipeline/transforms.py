import logging, apache_beam as beam
from apache_beam import pvalue
from .schemas import ErpInvoiceEvent, WmsReceivingEvent
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec, TimerSpec, on_timer
)
from apache_beam.coders import StrUtf8Coder
from apache_beam.utils.timestamp import Duration, Timestamp

"""
pipeline.transforms
===================
YOU WILL BUILD THIS INCREMENTALLY across Steps 4.1 → 4.4.

Roadmap:
    Step 4.1  → ParseWmsLine, ParseErpLine          (parsing + dead-letter)
    Step 4.2  → DeduplicateInvoicesDoFn             (stateful, with TTL timer)
    Step 4.3  → AssignEventTimestamp, VelocityFraudCheck (sliding window)
    Step 4.4  → load_vendor_baseline, AnomalyCheckDoFn   (side input + fallback)
    (Always) → Postgres sink DoFns (batched, idempotent)

Follow docs/phase1_walkthrough.html for ready-to-paste reference snippets.
"""

log = logging.getLogger(__name__)

class ParseErpLine(beam.DoFn):
    DEAD_LETTER = "dead_letter"
    def process(self, line:str):
        try:
            yield ErpInvoiceEvent.from_json(line)
        except Exception as exc:
            log.warning("Bad ERP payload: %s | err=%s", line[:120], exc)
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {"raw": line, "err": str(exc)})

class ParseWmsLine(beam.DoFn):
    DEAD_LETTER = "dead_letter"
    def process(self, line: str):
        try:
            yield WmsReceivingEvent.from_json(line)
        except Exception as exc:
            log.warning("Bad WMS payload: %s | err=%s", line[:120], exc)
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {"raw": line, "err": str(exc)})            



class DeduplicateInvoicesDoFn(beam.DoFn):
    DUPLICATES = "duplicates"
    SEEN_STATE   = ReadModifyWriteStateSpec("seen", StrUtf8Coder())
    EXPIRY_TIMER = TimerSpec("expiry", beam.TimeDomain.WATERMARK)

    def __init__(self, ttl_seconds: int): self._ttl = ttl_seconds

    def process(self, element,
                seen=beam.DoFn.StateParam(SEEN_STATE),
                timer=beam.DoFn.TimerParam(EXPIRY_TIMER),
                ts=beam.DoFn.TimestampParam):
        invoice_id, event = element
        if seen.read() == "1":
            yield pvalue.TaggedOutput(self.DUPLICATES, event); return
        seen.write("1")
        timer.set(Timestamp(ts.micros / 1_000_000) + Duration(self._ttl))
        yield event

    @on_timer(EXPIRY_TIMER)
    def _gc(self, seen=beam.DoFn.StateParam(SEEN_STATE)):
        seen.clear()                    # free memory after TTL


class DeduplicateInvoices(beam.PTransform):
    def __init__(self, ttl_seconds: int):
        super().__init__(); self._ttl = ttl_seconds
    def expand(self, pcoll):
        return (pcoll
            | "KeyByInvoiceId" >> beam.Map(lambda e: (e.invoice_id, e))
            | "StatefulDedup"  >> beam.ParDo(
                DeduplicateInvoicesDoFn(self._ttl)
              ).with_outputs(DeduplicateInvoicesDoFn.DUPLICATES, main="unique"))       