# Supply Chain Fraud Detection — Phase 1 🐶

End-to-end Apache Beam pipeline that detects fraud in a Walmart-scale
warehouse + ERP stream. Phase 1 runs **entirely on your laptop** via the
`DirectRunner` — no GCP credentials, no internet, no excuses.

| Production target           | Phase 1 emulation              |
| --------------------------- | ------------------------------ |
| GCP Pub/Sub topics          | `data/*.jsonl` files           |
| BigQuery sinks              | Local PostgreSQL (`fraud_detection` DB) |
| Dataflow streaming runner   | DirectRunner with `--streaming`|
| 2k msg/s avg, 10k peak      | ~few k msg/s on a laptop       |
| <1s e2e latency             | Same — windowing tuned for it  |

## Project layout

```
.
├── db/init_tables.sql            ← bronze / silver / gold / baseline DDL
├── pipeline/
│   ├── config.py                 ← 12-factor config object
│   ├── schemas.py                ← typed event payloads + parsers
│   ├── transforms.py             ← DoFns, PTransforms, sinks
│   └── main.py                   ← DAG wiring + DirectRunner entry point
├── scripts/generate_mock_data.py ← deterministic JSONL stream generator
├── docker-compose.yml            ← local Postgres
├── requirements.txt
└── README.md
```

## Quick start

```bash
# 0. Create venv
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt

# 1. Start Postgres + auto-apply DDL
docker compose up -d
docker compose exec postgres pg_isready -U fraud_app

# 2. Generate mock data (includes intentional fraud signals)
python scripts/generate_mock_data.py --num-wms 2000 --num-invoices 2000

# 3. Run the pipeline
python -m pipeline.main \
    --wms_input data/wms_receiving.jsonl \
    --erp_input data/erp_invoices.jsonl

# 4. Inspect results
docker compose exec postgres psql -U fraud_app -d fraud_detection -c \
  "SELECT rule_name, severity, COUNT(*) FROM gold_fraud_alerts GROUP BY 1,2;"
```

## What gets detected?

| Rule       | Mechanism                                                       |
| ---------- | --------------------------------------------------------------- |
| `VELOCITY` | 10-min sliding window grouped by `vendor_id`; ≥3 identical amounts |
| `ANOMALY`  | z-score vs `vendor_90day_baseline` (3σ threshold)               |
| `FALLBACK` | Triggered when baseline lookup fails OR vendor unknown          |

Duplicate `invoice_id`s are silently suppressed by a stateful DoFn with a
24-hour TTL — bounded memory regardless of stream volume.

## Phase 2 roadmap

* Swap `ReadFromText` → `ReadFromPubSub`
* Swap `WriteXxx` DoFns → `WriteToBigQuery` (schemas are already compatible)
* Promote `vendor_90day_baseline` side input to a slowly-changing
  `PeriodicImpulse` so it refreshes hourly without redeploys
* Add Dataflow autoscaling + structured streaming metrics → Cloud Monitoring
