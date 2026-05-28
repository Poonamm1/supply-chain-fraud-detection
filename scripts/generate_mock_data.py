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


# ═══════════════════════════════════════════════════════════════════════════
# VENDOR BASELINES — must match gcp/bq_schema.sql vendor_90day_baseline!
# ═══════════════════════════════════════════════════════════════════════════
KNOWN_VENDORS = ["V-1001", "V-1002", "V-1003", "V-1004", "V-1005"]
UNKNOWN_VENDORS = ["V-9001", "V-9002"]          # absent from baseline -> fallback path

# These MUST match BigQuery baseline for fraud detection to work!
VENDOR_BASELINE = {
    "V-1001": {"avg": 1200.00, "stddev": 180.00},  # BQ baseline
    "V-1002": {"avg": 4500.00, "stddev": 600.00},
    "V-1003": {"avg":  850.00, "stddev": 120.00},
    "V-1004": {"avg": 15000.00, "stddev": 2200.00},
    "V-1005": {"avg":  250.00, "stddev":  45.00},
}


def _jitter(amount: float, pct: float = 0.10) -> float:
    """Add ±pct% noise to an amount."""
    return round(amount * (1 + random.uniform(-pct, pct)), 2)


def _get_vendor_avg(vendor: str) -> float:
    """Get vendor baseline average, with fallback for unknown vendors."""
    return VENDOR_BASELINE.get(vendor, {"avg": 1000.00}).get("avg", 1000.00)


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
    """Generate ERP invoices with GUARANTEED fraud scenarios."""
    fake = Faker()
    Faker.seed(7)
    random.seed(7)
    path.parent.mkdir(parents=True, exist_ok=True)

    fraud_scenarios = []
    with path.open("w") as fh:
        ts = start
        records_written = 0

        # ═══════════════════════════════════════════════════════════════
        # 1. NORMAL TRAFFIC (~85% of volume)
        # ═══════════════════════════════════════════════════════════════
        normal_count = int(n * 0.85)
        for _ in range(normal_count):
            ts += timedelta(seconds=random.randint(2, 8))
            vendor = random.choice(KNOWN_VENDORS + UNKNOWN_VENDORS)
            base = _get_vendor_avg(vendor)
            # Normal traffic: within ±15% of baseline
            amount = _jitter(base, 0.15)
            row = _invoice(fake, vendor, amount, ts)
            fh.write(json.dumps(row) + "\n")
            records_written += 1

        # ═══════════════════════════════════════════════════════════════
        # 2. DUPLICATE INVOICES (exact same invoice_id)
        # ═══════════════════════════════════════════════════════════════
        print("\n🔄 Generating DUPLICATE invoice scenarios...")
        ts += timedelta(minutes=2)
        dup_id_1 = f"INV-DUP-{uuid.uuid4().hex[:8].upper()}"
        dup_id_2 = f"INV-DUP-{uuid.uuid4().hex[:8].upper()}"
        
        # Duplicate set 1: V-1001, invoice appears 3 times
        for i in range(3):
            ts += timedelta(seconds=20)
            row = _invoice(fake, "V-1001", 1350.00, ts)
            row["invoice_id"] = dup_id_1  # Force same ID
            fh.write(json.dumps(row) + "\n")
            records_written += 1
            if i == 0:
                fraud_scenarios.append(f"  • {dup_id_1}: appears 3x (dedup triggers)")
        
        # Duplicate set 2: V-1002, invoice appears 2 times
        for i in range(2):
            ts += timedelta(seconds=30)
            row = _invoice(fake, "V-1002", 4700.00, ts)
            row["invoice_id"] = dup_id_2
            fh.write(json.dumps(row) + "\n")
            records_written += 1
            if i == 0:
                fraud_scenarios.append(f"  • {dup_id_2}: appears 2x (dedup triggers)")

        # ═══════════════════════════════════════════════════════════════
        # 3. VELOCITY FRAUD — rapid-fire identical amounts
        # ═══════════════════════════════════════════════════════════════
        print("\n⚡ Generating VELOCITY fraud scenarios...")
        ts += timedelta(minutes=3)
        
        # Scenario A: V-1003 fires 7x $999.99 in 90 seconds
        velocity_amount = 999.99
        velocity_ids = []
        for i in range(7):
            ts += timedelta(seconds=15)  # 15s apart → all in 90s window
            row = _invoice(fake, "V-1003", velocity_amount, ts)
            velocity_ids.append(row["invoice_id"])
            fh.write(json.dumps(row) + "\n")
            records_written += 1
        fraud_scenarios.append(
            f"  • V-1003: 7x ${velocity_amount} in 90s → VELOCITY (HIGH)"
        )
        
        # Scenario B: V-1001 fires 5x $1,500 in 60 seconds
        ts += timedelta(minutes=1)
        velocity_amount_2 = 1500.00
        for i in range(5):
            ts += timedelta(seconds=12)  # 12s apart → all in 60s window
            row = _invoice(fake, "V-1001", velocity_amount_2, ts)
            fh.write(json.dumps(row) + "\n")
            records_written += 1
        fraud_scenarios.append(
            f"  • V-1001: 5x ${velocity_amount_2} in 60s → VELOCITY (HIGH)"
        )

        # ═══════════════════════════════════════════════════════════════
        # 4. ANOMALY FRAUD — z-score outliers
        # ═══════════════════════════════════════════════════════════════
        print("\n📊 Generating ANOMALY fraud scenarios...")
        ts += timedelta(minutes=2)
        
        # V-1005: avg=$250, stddev=$45
        # z >= 3.0 threshold → amount >= 250 + 3*45 = 385
        anomalies = [
            ("V-1005", 2500.00),   # z = (2500-250)/45 = 50.0σ → CRITICAL
            ("V-1005",  800.00),   # z = (800-250)/45 = 12.2σ → CRITICAL
            ("V-1005",  500.00),   # z = (500-250)/45 = 5.6σ → CRITICAL
            ("V-1005",  400.00),   # z = (400-250)/45 = 3.3σ → HIGH
        ]
        
        # V-1001: avg=$1200, stddev=$180
        anomalies.extend([
            ("V-1001", 3000.00),   # z = (3000-1200)/180 = 10.0σ → CRITICAL
            ("V-1001", 2100.00),   # z = (2100-1200)/180 = 5.0σ → CRITICAL
            ("V-1001", 1800.00),   # z = (1800-1200)/180 = 3.3σ → HIGH
        ])
        
        # V-1003: avg=$850, stddev=$120
        anomalies.extend([
            ("V-1003", 2500.00),   # z = (2500-850)/120 = 13.75σ → CRITICAL
            ("V-1003", 1600.00),   # z = (1600-850)/120 = 6.25σ → CRITICAL
        ])
        
        for vendor, amount in anomalies:
            ts += timedelta(seconds=random.randint(10, 30))
            row = _invoice(fake, vendor, amount, ts)
            fh.write(json.dumps(row) + "\n")
            records_written += 1
            
            baseline = VENDOR_BASELINE[vendor]
            z_score = (amount - baseline["avg"]) / baseline["stddev"]
            severity = "CRITICAL" if abs(z_score) >= 5 else "HIGH"
            fraud_scenarios.append(
                f"  • {vendor}: ${amount:,.2f} (z={z_score:.1f}σ) → ANOMALY ({severity})"
            )

        # ═══════════════════════════════════════════════════════════════
        # 5. FALLBACK PATH — unknown vendors
        # ═══════════════════════════════════════════════════════════════
        print("\n🔍 Generating FALLBACK scenarios (unknown vendors)...")
        ts += timedelta(minutes=1)
        for vendor in UNKNOWN_VENDORS:
            ts += timedelta(seconds=20)
            row = _invoice(fake, vendor, random.uniform(500, 5000), ts)
            fh.write(json.dumps(row) + "\n")
            records_written += 1
            fraud_scenarios.append(
                f"  • {vendor}: no baseline → FALLBACK (MEDIUM)"
            )

    print(f"\n✅ Wrote {records_written} ERP invoice events to {path}")
    print("\n" + "═" * 70)
    print("🎯 GUARANTEED FRAUD SCENARIOS INJECTED:")
    print("═" * 70)
    for scenario in fraud_scenarios:
        print(scenario)
    print("═" * 70)


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
