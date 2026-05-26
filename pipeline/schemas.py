"""
pipeline.schemas
================
Typed payload definitions + JSON parsers for the two upstream event sources.

Why dataclasses and not Pydantic?
    * Beam pickles transforms aggressively; pure-stdlib dataclasses serialize
      cleanly with zero version-coupling to a third-party validator.
    * We keep validation logic *here* (one place) instead of scattered in DoFns.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from dateutil import parser as dt_parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 string into a tz-aware UTC datetime.

    All timestamps internal to the pipeline are UTC. Period.
    """
    dt = dt_parser.isoparse(value)
    if dt.tzinfo is None:                       # treat naive as UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sha256(value: str) -> str:
    """One-way hash for sensitive fields (bank account, etc.).

    Never log, never persist raw PII — defense in depth.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# WMS Receiving Log
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class WmsReceivingEvent:
    wh_id: str
    po_no: str
    vendor_id: str
    upc_no: str
    qty_received: int
    received_timestamp: datetime
    # Lineage / envelope fields
    event_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_system: str = "WMS"
    event_type: str = "RECEIVING"

    @classmethod
    def from_json(cls, raw: str) -> "WmsReceivingEvent":
        d = json.loads(raw)
        return cls(
            wh_id=str(d["wh_id"]),
            po_no=str(d["po_no"]),
            vendor_id=str(d["vendor_id"]),
            upc_no=str(d["upc_no"]),
            qty_received=int(d["qty_received"]),
            received_timestamp=_parse_ts(d["received_timestamp"]),
            event_uuid=d.get("event_uuid", str(uuid.uuid4())),
        )

    @property
    def event_time(self) -> datetime:
        return self.received_timestamp

    def to_bronze_row(self) -> Dict[str, Any]:
        return {
            "event_uuid":      self.event_uuid,
            "source_system":   self.source_system,
            "event_type":      self.event_type,
            "event_timestamp": self.event_time,
            "payload": {
                "wh_id":        self.wh_id,
                "po_no":        self.po_no,
                "vendor_id":    self.vendor_id,
                "upc_no":       self.upc_no,
                "qty_received": self.qty_received,
            },
        }


# ---------------------------------------------------------------------------
# ERP Invoice Event
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ErpInvoiceEvent:
    invoice_id: str
    po_no: str
    vendor_id: str
    invoice_amount: float
    invoice_timestamp: datetime
    bank_account_hash: str       # we never carry the raw account number
    upc_no: Optional[str] = None
    email_id: Optional[str] = None
    event_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_system: str = "ERP"
    event_type: str = "INVOICE"

    @classmethod
    def from_json(cls, raw: str) -> "ErpInvoiceEvent":
        d = json.loads(raw)
        # Hash sensitive fields at the boundary — anything past this point is safe.
        raw_bank = str(d.get("bank_account_details", ""))
        return cls(
            invoice_id=str(d["invoice_id"]),
            po_no=str(d["po_no"]),
            vendor_id=str(d["vendor_id"]),
            invoice_amount=float(d["invoice_amount"]),
            invoice_timestamp=_parse_ts(d["invoice_timestamp"]),
            bank_account_hash=_sha256(raw_bank) if raw_bank else _sha256("UNKNOWN"),
            upc_no=str(d["upc_no"]) if d.get("upc_no") else None,
            email_id=d.get("email_id"),
            event_uuid=d.get("event_uuid", str(uuid.uuid4())),
        )

    @property
    def event_time(self) -> datetime:
        return self.invoice_timestamp

    def to_bronze_row(self) -> Dict[str, Any]:
        return {
            "event_uuid":      self.event_uuid,
            "source_system":   self.source_system,
            "event_type":      self.event_type,
            "event_timestamp": self.event_time,
            "payload": {
                "invoice_id":        self.invoice_id,
                "po_no":             self.po_no,
                "vendor_id":         self.vendor_id,
                "invoice_amount":    self.invoice_amount,
                "upc_no":            self.upc_no,
                "email_id":          self.email_id,
                "bank_account_hash": self.bank_account_hash,
            },
        }

    def to_silver_row(self, window_start: Optional[datetime] = None) -> Dict[str, Any]:
        row = asdict(self)
        row["invoice_timestamp"] = self.invoice_timestamp
        row.pop("event_uuid", None)
        row.pop("source_system", None)
        row.pop("event_type", None)
        row["dedup_window_start"] = window_start
        return row
