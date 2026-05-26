"""Smoke test for the parsers — runs without Beam or Postgres."""
from datetime import datetime, timezone

import pytest

from pipeline.schemas import ErpInvoiceEvent, WmsReceivingEvent


def test_wms_parse_roundtrip():
    raw = (
        '{"wh_id":"WH-001","po_no":"PO-1","vendor_id":"V-1001",'
        '"upc_no":"0001","qty_received":10,'
        '"received_timestamp":"2026-05-25T12:00:00Z"}'
    )
    ev = WmsReceivingEvent.from_json(raw)
    assert ev.vendor_id == "V-1001"
    assert ev.qty_received == 10
    assert ev.event_time.tzinfo is not None
    assert ev.to_bronze_row()["source_system"] == "WMS"


def test_erp_parse_hashes_bank_account():
    raw = (
        '{"invoice_id":"INV-1","po_no":"PO-1","vendor_id":"V-1002",'
        '"invoice_amount":1234.56,'
        '"invoice_timestamp":"2026-05-25T12:00:00Z",'
        '"bank_account_details":"DE89370400440532013000",'
        '"upc_no":"0001","email_id":"a@b.com"}'
    )
    ev = ErpInvoiceEvent.from_json(raw)
    # Raw IBAN must NEVER appear in the parsed envelope
    assert "532013000" not in ev.bank_account_hash
    assert len(ev.bank_account_hash) == 64    # SHA-256
    assert ev.invoice_amount == pytest.approx(1234.56)


def test_erp_silver_row_has_no_envelope_fields():
    raw = (
        '{"invoice_id":"INV-2","po_no":"PO-2","vendor_id":"V-1002",'
        '"invoice_amount":10,"invoice_timestamp":"2026-05-25T12:00:00Z",'
        '"bank_account_details":"X","upc_no":"u","email_id":"a@b.com"}'
    )
    ev = ErpInvoiceEvent.from_json(raw)
    row = ev.to_silver_row()
    for forbidden in ("event_uuid", "source_system", "event_type"):
        assert forbidden not in row
