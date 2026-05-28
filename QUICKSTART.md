# 🚀 Quick Start Guide — Fraud Detection Enhancement

## ✅ What Just Happened?

I've transformed your fraud detection pipeline from "no alerts" to "guaranteed fraud triggers" with rich observability! Here's what changed:

### 📦 Enhanced Files (8 total):
1. ✨ `scripts/generate_mock_data.py` — DETERMINISTIC fraud scenarios
2. ✨ `pipeline/transforms.py` — structured logging + rich evidence
3. ✨ `pipeline/gcp_batch_main.py` — fraud_score + alert_source fields
4. ✨ `gcp/bq_schema.sql` — updated gold table schema
5. 🆕 `gcp/validation_queries.sql` — 10+ SQL queries
6. 🆕 `gcp/test_fraud_detection.sh` — E2E test automation
7. 🆕 `docs/FRAUD_ENHANCEMENT_GUIDE.md` — full deployment guide
8. 🆕 `CHANGELOG.md` — complete change summary

### 🎯 Expected Results After Deployment:
- **~23 fraud alerts** in gold table
- **7 CRITICAL anomaly alerts** (z-scores 5σ to 50σ)
- **2 HIGH anomaly alerts** (z-scores 3.3σ)
- **12 HIGH velocity alerts** (rapid duplicate amounts)
- **2 MEDIUM fallback alerts** (unknown vendors)

---

## 🏃 Deploy in 5 Steps (GCP Cloud Shell)

### 1️⃣ Pull Latest Code
```bash
cd ~/supply-chain-fraud-detection
git pull origin main
```

### 2️⃣ Update BigQuery Schema
```bash
# Add new columns to gold table
bq query --use_legacy_sql=false <<'EOF'
ALTER TABLE `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
ADD COLUMN IF NOT EXISTS fraud_score INT64,
ADD COLUMN IF NOT EXISTS alert_source STRING;
EOF
```

**OR** drop and recreate (if you don't care about existing data):
```bash
bq rm -f -t fraud-detect-260526-1750:fraud_detection.gold_fraud_alerts

export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"
export BQ_LOCATION="us-central1"
envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
```

### 3️⃣ Rebuild Flex Template (5-8 min)
```bash
cd ~/supply-chain-fraud-detection
./gcp/build_flex_template.sh
```

**Wait for**: `✅ Flex Template published.`

### 4️⃣ Run E2E Test (One Command!)
```bash
./gcp/test_fraud_detection.sh
```

This will:
- ✅ Generate fraud data
- ✅ Upload to GCS
- ✅ Verify BigQuery baseline
- ✅ Trigger Dataflow pipeline
- ✅ Wait for completion (~3-5 min)
- ✅ Display fraud alert summary

### 5️⃣ Validate Results
```bash
# Quick validation query
bq query --use_legacy_sql=false <<'EOF'
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  ROUND(AVG(fraud_score), 1) AS avg_fraud_score
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE DATE(detected_at) = CURRENT_DATE()
GROUP BY rule_name, severity
ORDER BY avg_fraud_score DESC;
EOF
```

**Expected Output**:
```
+----------+----------+-------------+------------------+
| rule_name | severity | alert_count | avg_fraud_score |
+----------+----------+-------------+------------------+
| ANOMALY  | CRITICAL |           7 |             90.0 |
| VELOCITY | HIGH     |          12 |             60.0 |
| ANOMALY  | HIGH     |           2 |             35.0 |
| FALLBACK | MEDIUM   |           2 |             25.0 |
+----------+----------+-------------+------------------+
```

---

## 📊 Explore Your Fraud Alerts

### View in BigQuery Console
```
https://console.cloud.google.com/bigquery?project=fraud-detect-260526-1750&p=fraud-detect-260526-1750&d=fraud_detection&t=gold_fraud_alerts&page=table
```

### Run Validation Queries
```bash
# Copy queries from this file:
cat gcp/validation_queries.sql

# Paste into BigQuery Console (replace ${BQ_PROJECT} and ${BQ_DATASET})
# Or run directly:
export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"
envsubst < gcp/validation_queries.sql | bq query --use_legacy_sql=false
```

### Key Queries to Try:
1. **Portfolio Showcase** (query #11) — single comprehensive view
2. **Velocity Deep Dive** (query #4) — burst patterns
3. **Anomaly Deep Dive** (query #5) — z-score distribution
4. **Top Risky Vendors** (query #8) — executive dashboard

---

## 🎤 Interview Demo Script (2 Minutes)

### Opening (30 sec)
> "This is a **real-time fraud detection pipeline** running on **GCP Dataflow** with **Apache Beam**. It processes invoice and warehouse receiving events through a **medallion architecture**:
> - **Bronze**: raw event ingestion
> - **Silver**: stateful deduplication
> - **Gold**: fraud detection with two core algorithms"

### Show Fraud Alerts (30 sec)
```sql
-- Run this in BigQuery Console
SELECT
  invoice_id,
  vendor_id,
  rule_name,
  severity,
  fraud_score,
  reason
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE DATE(detected_at) = CURRENT_DATE()
ORDER BY fraud_score DESC
LIMIT 10;
```

### Deep Dive: Anomaly Detection (1 min)
```sql
-- Show z-score evidence
SELECT
  vendor_id,
  invoice_id,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.amount') AS FLOAT64), 2) AS invoice_amount,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.baseline_avg') AS FLOAT64), 2) AS vendor_avg,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS FLOAT64), 2) AS z_score,
  severity,
  fraud_score
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE rule_name = 'ANOMALY'
  AND DATE(detected_at) = CURRENT_DATE()
ORDER BY z_score DESC
LIMIT 5;
```

> "As you can see, this invoice for **$2,500** is **50 standard deviations** above the vendor's typical **$250** average—a clear statistical outlier flagged by the z-score algorithm."

### Closing (30 sec)
> "The pipeline runs as a **batch job**, so workers scale to zero after completion. Total cost per run: **$0.05-$0.10**. All tables are partitioned and clustered for fast queries, and BigQuery load jobs are free vs streaming inserts."

---

## 🐛 Troubleshooting

### No fraud alerts after job completes?

**Check Dataflow logs**:
```bash
gcloud logging read "resource.type=dataflow_step AND severity>=WARNING" \
  --project=fraud-detect-260526-1750 \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)"
```

**Verify baseline table**:
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`fraud-detect-260526-1750.fraud_detection.vendor_90day_baseline\`"
# Should return 5 vendors
```

**If baseline is empty**:
```bash
export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"
export BQ_LOCATION="us-central1"
envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
```

### Mock data not triggering alerts?

**Regenerate with aligned baselines**:
```bash
cd ~/supply-chain-fraud-detection
python scripts/generate_mock_data.py --num-invoices 1000
gsutil cp data/*.jsonl gs://fraud_detection_pipeline_bucket/
```

**Verify baselines match**:
```bash
# In scripts/generate_mock_data.py, these MUST match BQ schema:
grep -A 5 "VENDOR_BASELINE" scripts/generate_mock_data.py
# Should show: V-1001: avg=1200.00, V-1002: avg=4500.00, etc.
```

### Container not rebuilding?

**Force rebuild**:
```bash
cd ~/supply-chain-fraud-detection
git pull origin main  # Make sure you have latest code!
IMAGE_TAG="$(date +%Y%m%d-%H%M%S)" ./gcp/build_flex_template.sh
```

---

## 📚 Full Documentation

- **Deployment Guide**: `docs/FRAUD_ENHANCEMENT_GUIDE.md`
- **Validation Queries**: `gcp/validation_queries.sql`
- **Change Summary**: `CHANGELOG.md`
- **E2E Test Script**: `gcp/test_fraud_detection.sh`

---

## ✅ Success Checklist

After deployment, verify:
- [ ] Gold table has **20+ fraud alerts**
- [ ] Alerts include **VELOCITY**, **ANOMALY**, **FALLBACK** types
- [ ] Evidence JSON contains **z_score**, **fraud_pattern**, **deviation_pct**
- [ ] Fraud scores range from **25** (fallback) to **100** (extreme anomaly)
- [ ] Structured logs show **VELOCITY_FRAUD_DETECTED**, **ANOMALY_DETECTED**
- [ ] Duplicate invoices appear in logs as **DUPLICATE_SUPPRESSED**
- [ ] Validation queries run successfully in BigQuery Console

---

## 🎯 Portfolio Impact

**Before**: Empty gold table, no visible fraud detection

**After**:
- ✅ **23+ fraud alerts** with rich evidence
- ✅ **Structured logging** for all fraud events
- ✅ **Z-score statistics** from 3.3σ to 50σ
- ✅ **Velocity burst detection** with window analysis
- ✅ **Fallback path** for unknown vendors
- ✅ **10+ validation queries** ready for demos
- ✅ **E2E test automation** for CI/CD
- ✅ **Portfolio-grade observability** for interviews

**Your pipeline is now demo-ready!** 🚀🐶

---

## 🆘 Need Help?

1. Check `docs/FRAUD_ENHANCEMENT_GUIDE.md` for detailed troubleshooting
2. Review Dataflow logs for structured fraud events
3. Run validation queries from `gcp/validation_queries.sql`
4. Verify `CHANGELOG.md` for complete change summary

**Bark loud if you need assistance!** 🐕
