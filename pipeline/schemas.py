"""
pipeline.schemas
================
Typed payloads + JSON parsers for the two upstream event sources.
PII is hashed at the boundary — raw bank details never enter a PCollection.
"""
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from dateutil import parser as dt_parser


# ---------- helpers ----------
def _parse_ts(v: str) -> datetime:
    """ISO-8601 string -> tz-aware UTC datetime."""
    dt = dt_parser.isoparse(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sha256(v: str) -> str:
    """One-way hash for sensitive fields."""
    return hashlib.sha256(v.encode("utf-8")).hexdigest()


# ---------- WMS Receiving ----------
@dataclass(frozen=True)
class WmsReceivingEvent:
    wh_id: str
    po_no: str
    vendor_id: str
    upc_no: str
    qty_received: int
    received_timestamp: datetime
    event_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def from_json(cls, raw: str) -> "WmsReceivingEvent":
        d = json.loads(raw)
        return cls(
            wh_id              = str(d["wh_id"]),
            po_no              = str(d["po_no"]),
            vendor_id          = str(d["vendor_id"]),
            upc_no             = str(d["upc_no"]),
            qty_received       = int(d["qty_received"]),
            received_timestamp = _parse_ts(d["received_timestamp"]),
            event_uuid         = d.get("event_uuid", str(uuid.uuid4())),
        )

    @property
    def event_time(self) -> datetime:
        return self.received_timestamp


# ---------- ERP Invoice ----------
@dataclass(frozen=True)
class ErpInvoiceEvent:
    invoice_id: str
    po_no: str
    vendor_id: str
    invoice_amount: float
    invoice_timestamp: datetime
    bank_account_hash: str          # never raw!
    upc_no: str | None = None
    email_id: str | None = None
    event_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def from_json(cls, raw: str) -> "ErpInvoiceEvent":
        d = json.loads(raw)
        return cls(
            invoice_id        = str(d["invoice_id"]),
            po_no             = str(d["po_no"]),
            vendor_id         = str(d["vendor_id"]),
            invoice_amount    = float(d["invoice_amount"]),
            invoice_timestamp = _parse_ts(d["invoice_timestamp"]),
            bank_account_hash = _sha256(str(d.get("bank_account_details", ""))),
            upc_no            = d.get("upc_no"),
            email_id          = d.get("email_id"),
            event_uuid        = d.get("event_uuid", str(uuid.uuid4())),
        )

    @property
    def event_time(self) -> datetime:
        return self.invoice_timestamp
