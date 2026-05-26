"""
scripts/generate_mock_data.py
=============================
Produces deterministic-but-realistic JSONL streams for the two upstream
event sources. Includes intentional fraud signals so the pipeline has
something interesting to flag.

Usage:
    python scripts/generate_mock_data.py --wms-out data/wms_receiving.jsonl \\
                                         --erp-out data/erp_invoices.jsonl \\
                                         --num-invoices 5000
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker


KNOWN_VENDORS = ["V-1001", "V-1002", "V-1003", "V-1004", "V-1005"]
UNKNOWN_VENDORS = ["V-9001", "V-9002"]          # absent from baseline -> fallback path
VENDOR_AVG = {
    "V-1001":  4500.00,
    "V-1002": 12500.00,
    "V-1003":   780.00,
    "V-1004": 50000.00,
    "V-1005":   250.00,
}


def _jitter(amount: float, pct: float = 0.10) -> float:
    """Add ±pct% noise to an amount."""
    return round(amount * (1 + random.uniform(-pct, pct)), 2)


def gen_wms(path: Path, n: int, start: datetime) -> None:
    fake = Faker()
    Faker.seed(42)
    random.seed(42)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        ts = start
        for _ in range(n):
            ts += timedelta(seconds=random.randint(1, 4))
            row = {
                "event_uuid": str(uuid.uuid4()),
                "wh_id":      f"WH-{random.randint(1, 25):03d}",
                "po_no":      f"PO-{random.randint(100000, 999999)}",
                "vendor_id":  random.choice(KNOWN_VENDORS + UNKNOWN_VENDORS),
                "upc_no":     str(fake.ean13()),
                "qty_received": random.randint(1, 500),
                "received_timestamp": ts.isoformat(),
            }
            fh.write(json.dumps(row) + "\n")
    print(f"✅ Wrote {n} WMS events to {path}")


def gen_erp(path: Path, n: int, start: datetime) -> None:
    fake = Faker()
    Faker.seed(7)
    random.seed(7)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w") as fh:
        ts = start
        records_written = 0

        # ---- 1. Normal traffic --------------------------------------------
        for _ in range(int(n * 0.88)):
            ts += timedelta(seconds=random.randint(1, 5))
            vendor = random.choice(KNOWN_VENDORS + UNKNOWN_VENDORS)
            base = VENDOR_AVG.get(vendor, 1000.00)
            row = _invoice(fake, vendor, _jitter(base, 0.15), ts)
            fh.write(json.dumps(row) + "\n")
            records_written += 1

        # ---- 2. Duplicates (re-emit ~3% of recent rows verbatim) ---------
        # Forces the stateful dedup path to do work.
        fh_path = path
        with fh_path.open() as rd:
            recent = rd.readlines()[-300:]
        dup_count = max(1, int(n * 0.03))
        for line in random.sample(recent, k=min(dup_count, len(recent))):
            fh.write(line)
            records_written += 1

        # ---- 3. Velocity fraud burst -------------------------------------
        # Vendor V-1003 fires 6 identical $999.99 invoices in 90 seconds.
        burst_ts = ts + timedelta(minutes=1)
        for i in range(6):
            burst_ts += timedelta(seconds=15)
            row = _invoice(fake, "V-1003", 999.99, burst_ts)
            fh.write(json.dumps(row) + "\n")
            records_written += 1

        # ---- 4. Anomaly outlier ------------------------------------------
        # V-1005 normally invoices ~$250 -> single $25,000 invoice ≈ ~550σ.
        ts = burst_ts + timedelta(minutes=2)
        row = _invoice(fake, "V-1005", 25000.00, ts)
        fh.write(json.dumps(row) + "\n")
        records_written += 1

    print(f"✅ Wrote {records_written} ERP invoice events to {path}")


def _invoice(fake: Faker, vendor: str, amount: float, ts: datetime) -> dict:
    return {
        "event_uuid":           str(uuid.uuid4()),
        "invoice_id":           f"INV-{uuid.uuid4().hex[:12].upper()}",
        "po_no":                f"PO-{random.randint(100000, 999999)}",
        "vendor_id":            vendor,
        "invoice_amount":       amount,
        "invoice_timestamp":    ts.isoformat(),
        "bank_account_details": fake.iban(),
        "upc_no":               str(fake.ean13()),
        "email_id":             fake.company_email(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wms-out", default="data/wms_receiving.jsonl")
    ap.add_argument("--erp-out", default="data/erp_invoices.jsonl")
    ap.add_argument("--num-wms",      type=int, default=2000)
    ap.add_argument("--num-invoices", type=int, default=2000)
    args = ap.parse_args()

    start = datetime.now(tz=timezone.utc) - timedelta(hours=2)
    gen_wms(Path(args.wms_out), args.num_wms, start)
    gen_erp(Path(args.erp_out), args.num_invoices, start)


if __name__ == "__main__":
    main()
