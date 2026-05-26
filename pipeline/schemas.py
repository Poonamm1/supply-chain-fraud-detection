"""
pipeline.schemas
================
TODO (Step 4.1): Define typed payloads for:
    - WmsReceivingEvent
    - ErpInvoiceEvent

Each should have a `from_json(raw: str)` classmethod and a `to_bronze_row()`
helper. Hash sensitive fields (bank_account_details) with SHA-256 at parse time.

See docs/phase1_walkthrough.html § Step 4.1 for the full spec.
"""
