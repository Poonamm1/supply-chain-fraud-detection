# 🎯 Fraud Detection Pipeline — Enhanced Deployment Guide

## What Changed?

This enhancement adds **guaranteed fraud scenarios** and **rich observability** to make your pipeline **portfolio-ready** for demos and interviews!

### ✨ Enhancements Overview

1. **Mock Data Generator** — creates DETERMINISTIC fraud triggers
2. **Structured Logging** — fraud detection events are logged with context
3. **Rich Evidence** — JSON evidence includes z-scores, patterns, fraud_score
4. **BigQuery Schema** — added `fraud_score` and `alert_source` columns
5. **Validation Queries** — 10+ SQL queries to inspect fraud alerts

---

## 🚀 Deployment Steps

### 1️⃣ Update BigQuery Schema

The `gold_fraud_alerts` table now has 2 new columns:
- `fraud_score` (INT64) — 0-100 risk score
- `alert_source` (STRING) — which fraud rule triggered

**If you already have the table**, run this DDL to add columns:

```sql
ALTER TABLE `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
ADD COLUMN IF NOT EXISTS fraud_score INT64,
ADD COLUMN IF NOT EXISTS alert_source STRING;
```

**OR** drop and recreate the table (loses existing data):

```bash
# From GCP Cloud Shell
cd ~/supply-chain-fraud-detection

# Set your project/dataset
export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"
export BQ_LOCATION="us-central1"

# Drop old gold table
bq rm -f -t ${BQ_PROJECT}:${BQ_DATASET}.gold_fraud_alerts

# Recreate with new schema
envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
```

---

### 2️⃣ Generate Enhanced Mock Data

The new generator creates **guaranteed fraud scenarios**:
- ✅ **7 velocity fraud invoices** (V-1003, $999.99 in 90s)
- ✅ **5 velocity fraud invoices** (V-1001, $1,500 in 60s)
- ✅ **9 anomaly fraud invoices** (z-scores from 3.3σ to 50σ)
- ✅ **5 duplicate invoices** (same invoice_id appears 2-3x)
- ✅ **2 fallback invoices** (unknown vendors V-9001, V-9002)

**Generate locally** (for testing):

```bash
cd ~/supply-chain-fraud-detection

python scripts/generate_mock_data.py \
  --wms-out data/wms_receiving.jsonl \
  --erp-out data/erp_invoices.jsonl \
  --num-wms 1000 \
  --num-invoices 1000
```

You'll see output like:
```
🔄 Generating DUPLICATE invoice scenarios...
⚡ Generating VELOCITY fraud scenarios...
📈 Generating ANOMALY fraud scenarios...
🔍 Generating FALLBACK scenarios (unknown vendors)...

══════════════════════════════════════════════════════════════════════
🎯 GUARANTEED FRAUD SCENARIOS INJECTED:
══════════════════════════════════════════════════════════════════════
  • INV-DUP-AB12CD34: appears 3x (dedup triggers)
  • INV-DUP-EF56GH78: appears 2x (dedup triggers)
  • V-1003: 7x $999.99 in 90s → VELOCITY (HIGH)
  • V-1001: 5x $1500.0 in 60s → VELOCITY (HIGH)
  • V-1005: $2,500.00 (z=50.0σ) → ANOMALY (CRITICAL)
  • V-1005: $800.00 (z=12.2σ) → ANOMALY (CRITICAL)
  • V-1005: $500.00 (z=5.6σ) → ANOMALY (CRITICAL)
  • V-1005: $400.00 (z=3.3σ) → ANOMALY (HIGH)
  • V-1001: $3,000.00 (z=10.0σ) → ANOMALY (CRITICAL)
  • V-1001: $2,100.00 (z=5.0σ) → ANOMALY (CRITICAL)
  • V-1001: $1,800.00 (z=3.3σ) → ANOMALY (HIGH)
  • V-1003: $2,500.00 (z=13.8σ) → ANOMALY (CRITICAL)
  • V-1003: $1,600.00 (z=6.2σ) → ANOMALY (CRITICAL)
  • V-9001: no baseline → FALLBACK (MEDIUM)
  • V-9002: no baseline → FALLBACK (MEDIUM)
══════════════════════════════════════════════════════════════════════
```

**Upload to GCS**:

```bash
gsutil cp data/wms_receiving.jsonl gs://fraud_detection_pipeline_bucket/
gsutil cp data/erp_invoices.jsonl gs://fraud_detection_pipeline_bucket/
```

---

### 3️⃣ Commit & Rebuild Flex Template

Since we modified the code, we need to rebuild the container image:

```bash
cd ~/supply-chain-fraud-detection

# Commit changes
git add scripts/generate_mock_data.py \
        pipeline/transforms.py \
        pipeline/gcp_batch_main.py \
        gcp/bq_schema.sql \
        gcp/validation_queries.sql

git commit -m "feat: add guaranteed fraud scenarios + rich observability

- Mock data generator creates deterministic fraud triggers
- Enhanced evidence JSON with z-scores, fraud patterns
- Added fraud_score (0-100) and alert_source to gold table
- Structured logging for all fraud detection events
- 10+ validation SQL queries for portfolio demos"

git push origin main
```

**Rebuild the Flex Template**:

```bash
cd ~/supply-chain-fraud-detection
./gcp/build_flex_template.sh
```

This will:
1. Build new Docker image with updated code
2. Push to Artifact Registry
3. Publish updated Flex Template spec to GCS
4. Takes ~5-8 minutes

---

### 4️⃣ Run the Pipeline

```bash
./gcp/run_flex_template.sh
```

Monitor the job in [Dataflow Console](https://console.cloud.google.com/dataflow).

You should see structured logs like:
```
VELOCITY_FRAUD_DETECTED | vendor=V-1003 amount=999.99 count=7 window_start=2026-05-28...
ANOMALY_DETECTED | vendor=V-1005 invoice=INV-... amount=2500.00 z_score=50.00 severity=CRITICAL
DUPLICATE_SUPPRESSED | invoice_id=INV-DUP-AB12CD34 vendor=V-1001 amount=1350.00
FALLBACK_TRIGGERED | vendor=V-9001 invoice=INV-... reason=No baseline available
```

---

### 5️⃣ Validate Results in BigQuery

Once the job completes, run the validation queries:

```bash
cd ~/supply-chain-fraud-detection/gcp

# Set your project/dataset
export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"

# Run the portfolio showcase query
envsubst < validation_queries.sql | bq query --use_legacy_sql=false
```

**Or in BigQuery Console**, copy/paste queries from `gcp/validation_queries.sql`.

#### 🎯 Key Queries to Run:

1. **Layer Health Check** — verify bronze → silver → gold flow
2. **Fraud Alerts by Rule** — see VELOCITY, ANOMALY, FALLBACK counts
3. **Detailed Fraud Alerts** — top 50 alerts with full evidence
4. **Velocity Deep Dive** — repeated amounts, burst patterns
5. **Anomaly Deep Dive** — z-scores, deviations
6. **Top Risky Vendors** — executive dashboard view

---

## 📊 Expected Results

After running the pipeline with the enhanced mock data, you should see:

### Gold Fraud Alerts Breakdown:

| Rule Name | Severity | Expected Count |
|-----------|----------|----------------|
| VELOCITY  | HIGH     | 12 (7 + 5 from two bursts) |
| ANOMALY   | CRITICAL | 7 |
| ANOMALY   | HIGH     | 2 |
| FALLBACK  | MEDIUM   | 2 |
| **TOTAL** | —        | **~23 alerts** |

### Sample Alert Output:

```sql
SELECT
  invoice_id,
  vendor_id,
  rule_name,
  severity,
  fraud_score,
  reason,
  evidence
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE DATE(detected_at) = CURRENT_DATE()
ORDER BY fraud_score DESC
LIMIT 5;
```

**Expected Row Example**:
```
invoice_id: INV-ABC123
vendor_id: V-1005
rule_name: ANOMALY
severity: CRITICAL
fraud_score: 100
reason: Invoice $2,500.00 is 50.0σ from vendor avg $250.00
evidence: {
  "amount": 2500.0,
  "baseline_avg": 250.0,
  "baseline_stddev": 45.0,
  "z_score": 50.0,
  "deviation_pct": 900.0,
  "fraud_pattern": "statistical_outlier"
}
```

---

## 🎤 Interview Demo Script

Use this flow to showcase your pipeline:

### 1. Explain the Architecture (30 seconds)
> "This is a **medallion architecture** running on GCP Dataflow with Apache Beam. Raw events flow through three layers:
> - **Bronze**: raw ERP/WMS events (immutable)
> - **Silver**: deduplicated invoices (stateful processing)
> - **Gold**: fraud alerts (anomaly + velocity detection)"

### 2. Show the Fraud Rules (1 minute)
> "I implemented two core fraud detection algorithms:
> 
> **Velocity Fraud**: Uses sliding windows to detect rapid-fire identical amounts from the same vendor in short time windows.
> 
> **Anomaly Detection**: Compares invoice amounts against vendor baselines using z-score statistics. Anything beyond 3 standard deviations triggers an alert.
> 
> Both use side inputs for vendor baselines and stateful processing for deduplication."

### 3. Run a Live Query (1 minute)
```sql
-- Show this in BigQuery Console
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  ROUND(AVG(fraud_score), 1) AS avg_score
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE DATE(detected_at) = CURRENT_DATE()
GROUP BY rule_name, severity
ORDER BY avg_score DESC;
```

### 4. Deep Dive into Evidence (2 minutes)
```sql
-- Pick a high-severity alert
SELECT
  invoice_id,
  vendor_id,
  fraud_score,
  reason,
  JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS z_score,
  JSON_EXTRACT_SCALAR(evidence, '$.baseline_avg') AS vendor_baseline,
  JSON_EXTRACT_SCALAR(evidence, '$.amount') AS invoice_amount
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE rule_name = 'ANOMALY'
  AND severity = 'CRITICAL'
ORDER BY fraud_score DESC
LIMIT 3;
```

> "As you can see, this invoice for $2,500 is **50 standard deviations** above the vendor's normal $250 average—a clear statistical outlier flagged by the anomaly detection algorithm."

### 5. Show the Pipeline Cost Efficiency
> "This runs as a **batch job** on Dataflow, so workers scale to zero when done. Total cost per run: **$0.05-$0.10** for our data volumes. BigQuery load jobs are free (vs streaming inserts), and all tables are partitioned + clustered for fast query performance."

---

## 🐛 Troubleshooting

### Gold table is still empty after job completes

**Check Dataflow logs**:
```
gcloud logging read "resource.type=dataflow_step AND severity>=WARNING" \
  --project=fraud-detect-260526-1750 \
  --limit=50 \
  --format=json
```

Look for:
- `BASELINE_MISSING` — vendor baseline table empty
- `ModuleNotFoundError` — image not rebuilt
- `PERMISSION_DENIED` — service account missing BQ write role

**Verify baseline table has data**:
```sql
SELECT COUNT(*) FROM `fraud-detect-260526-1750.fraud_detection.vendor_90day_baseline`;
-- Should return 5 (V-1001 through V-1005)
```

If empty, re-run the schema setup:
```bash
envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
```

---

### Mock data doesn't trigger alerts

**Check that baselines match**:
```python
# In scripts/generate_mock_data.py, this MUST match BQ schema:
VENDOR_BASELINE = {
    "V-1001": {"avg": 1200.00, "stddev": 180.00},  # ✅ Correct
    # NOT: {"avg": 4500.00, ...}  # ❌ Old value
}
```

**Regenerate data**:
```bash
rm data/*.jsonl
python scripts/generate_mock_data.py --num-invoices 1000
gsutil cp data/*.jsonl gs://fraud_detection_pipeline_bucket/
```

---

### Duplicate detection not working

Check if dedup TTL is too short:
```bash
# In gcp/run_flex_template.sh or your job submission:
--dedup_ttl_seconds=3600  # 1 hour (default)
```

Query for duplicates that slipped through:
```sql
SELECT invoice_id, COUNT(*) AS cnt
FROM `fraud-detect-260526-1750.fraud_detection.silver_deduplicated_invoices`
WHERE DATE(invoice_timestamp) = CURRENT_DATE()
GROUP BY invoice_id
HAVING cnt > 1;
-- Should return 0 rows if dedup is working!
```

---

## 📚 Reference

### Key Files Modified:
- `scripts/generate_mock_data.py` — deterministic fraud scenarios
- `pipeline/transforms.py` — structured logging + rich evidence
- `pipeline/gcp_batch_main.py` — new gold_row fields
- `gcp/bq_schema.sql` — fraud_score + alert_source columns
- `gcp/validation_queries.sql` — NEW: 10+ validation queries

### Fraud Detection Parameters:
```python
# Configurable via CLI args in gcp_batch_main.py:
--velocity_window_seconds=600       # 10-minute sliding window
--velocity_period_seconds=60        # 1-minute slide period
--dedup_ttl_seconds=3600            # 1-hour dedup memory
--anomaly_stddev_threshold=3.0      # Z-score threshold
--allowed_lateness_seconds=3600     # Late data tolerance
```

### Vendor Baselines (must match BigQuery):
| Vendor | Avg | Stddev | Z=3 Threshold | Z=5 Threshold |
|--------|-----|--------|---------------|---------------|
| V-1001 | $1,200 | $180 | $1,740 | $2,100 |
| V-1002 | $4,500 | $600 | $6,300 | $7,500 |
| V-1003 | $850 | $120 | $1,210 | $1,450 |
| V-1004 | $15,000 | $2,200 | $21,600 | $26,000 |
| V-1005 | $250 | $45 | $385 | $475 |

---

## ✅ Success Criteria

After deployment, you should be able to:

- ✅ Query `gold_fraud_alerts` and see 20+ alerts
- ✅ See VELOCITY, ANOMALY, and FALLBACK rule types
- ✅ See fraud_score values from 25 to 100
- ✅ Extract z-scores from evidence JSON
- ✅ Identify which invoices are duplicates
- ✅ Trace an alert back through silver → bronze
- ✅ Export results to CSV for stakeholder demos
- ✅ Explain the fraud logic in an interview setting

**You're now ready to showcase a production-grade fraud detection pipeline!** 🚀🐶
