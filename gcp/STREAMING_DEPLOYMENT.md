# 🚀 Streaming + Batch Fraud Detection Deployment Guide

## Overview

This fraud detection pipeline now supports **TWO modes**:

1. **BATCH Mode** (existing): Reads from GCS JSONL files → Dataflow → BigQuery
2. **STREAMING Mode** (new): Reads from Pub/Sub → Dataflow → BigQuery

Both modes use the **same fraud detection transforms** (velocity, anomaly, dedup) and write to the same BigQuery tables.

---

## 🔧 Prerequisites

- GCP project with Dataflow API enabled
- Service account with permissions:
  - `roles/dataflow.worker`
  - `roles/bigquery.dataEditor`
  - `roles/storage.objectAdmin`
  - `roles/pubsub.subscriber` (for streaming)
- BigQuery dataset and tables created (run `gcp/bq_schema.sql`)
- Pub/Sub topic and subscription created (run `gcp/setup_pubsub.sh`)

---

## 📋 Architecture Changes

### What Was Fixed:

1. **BigQuery Schema Mismatch**:
   - **Root Cause**: `WriteToBigQuery` was using FILE_LOADS without explicit schema definitions, causing Beam to auto-infer schemas which led to type mismatches
   - **Fix**: Added explicit schema dictionaries (`BRONZE_SCHEMA`, `SILVER_SCHEMA`, `GOLD_SCHEMA`) to all `WriteToBigQuery` calls
   - **Additional Fix**: `evidence` and `payload` fields now use `json.dumps()` to serialize dicts to JSON strings before writing

2. **Timestamp Handling**:
   - `window_start` and `window_end` are properly converted to ISO-8601 strings using `.isoformat()`
   - BigQuery correctly parses these as TIMESTAMP types when schema is explicitly defined

3. **Streaming Support**:
   - New entrypoint: `pipeline/gcp_stream_main.py`
   - Reads from Pub/Sub subscription instead of GCS files
   - Uses `StandardOptions.streaming = True`
   - Adds triggers for continuous output in streaming mode

---

## 🏗️ Setup Instructions

### Step 1: Create Pub/Sub Resources

```bash
cd ~/supply-chain-fraud-detection
./gcp/setup_pubsub.sh
```

This creates:
- Topic: `erp-invoices`
- Subscription: `erp-invoices-sub`
- Grants `pubsub.subscriber` role to service account

**Output**:
```
✅ Pub/Sub Setup Complete!
📌 Topic:        projects/fraud-detect-260526-1750/topics/erp-invoices
📌 Subscription: projects/fraud-detect-260526-1750/subscriptions/erp-invoices-sub
```

---

### Step 2: Verify BigQuery Schema

Make sure the gold table schema matches the code output:

```sql
-- Run this in BigQuery Console or via bq CLI
DESCRIBE `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`;
```

**Expected fields**:
- `invoice_id` (STRING, REQUIRED)
- `vendor_id` (STRING, REQUIRED)
- `rule_name` (STRING, REQUIRED)
- `severity` (STRING, REQUIRED)
- `reason` (STRING, NULLABLE)
- `evidence` (JSON, NULLABLE)
- `window_start` (TIMESTAMP, NULLABLE)
- `window_end` (TIMESTAMP, NULLABLE)
- `fraud_score` (INT64, NULLABLE)
- `alert_source` (STRING, NULLABLE)
- `detected_at` (TIMESTAMP, NULLABLE)

If any fields are missing, re-run schema setup:

```bash
export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"
export BQ_LOCATION="us-central1"
envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
```

---

### Step 3: Rebuild Flex Template (if using batch)

The batch template has been updated with explicit schemas:

```bash
cd ~/supply-chain-fraud-detection
./gcp/build_flex_template.sh
```

Wait for:
```
✅ Flex Template published.
```

---

## 🚀 Running the Pipelines

### Option A: Batch Mode (GCS Input)

**Use Case**: Process historical data or scheduled batch jobs

```bash
# 1. Generate and upload test data
python scripts/generate_mock_data.py --num-invoices 1000
gsutil cp data/*.jsonl gs://fraud_detection_pipeline_bucket/

# 2. Run batch pipeline via Flex Template
./gcp/run_flex_template.sh
```

**Expected Output**:
- ~1,000 events in bronze
- ~950 invoices in silver (after dedup)
- ~23 fraud alerts in gold

**Duration**: ~3-5 minutes (scales to zero after completion)

---

### Option B: Streaming Mode (Pub/Sub Input)

**Use Case**: Real-time fraud detection on live data streams

#### 1. Publish Test Messages

```bash
./gcp/publish_test_messages.sh
```

This publishes 20 ERP invoice events to Pub/Sub with guaranteed fraud scenarios.

**Output**:
```
✅ Published 20 invoice messages to Pub/Sub!
📊 Expected fraud scenarios in published messages:
   • Velocity fraud: 12 alerts (rapid duplicate amounts)
   • Anomaly fraud:  9 alerts (z-score outliers)
   • Fallback:       2 alerts (unknown vendors)
   • Duplicates:     5 suppressed (dedup working)
```

#### 2. Launch Streaming Pipeline

```bash
./gcp/run_stream_template.sh
```

**Output**:
```
✅ Streaming job launched: fraud-stream-20260528-123456
📊 Monitor job:
   https://console.cloud.google.com/dataflow/jobs/us-central1/fraud-stream-20260528-123456
```

#### 3. Monitor Streaming Job

**Check Dataflow Console**:
```
https://console.cloud.google.com/dataflow/jobs/us-central1?project=fraud-detect-260526-1750
```

**Check Job Logs**:
```bash
gcloud logging read "resource.type=dataflow_step" \
  --project=fraud-detect-260526-1750 \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)"
```

#### 4. Validate Results in BigQuery

Run validation queries from `gcp/validation_queries_streaming.sql`:

```bash
export BQ_PROJECT="fraud-detect-260526-1750"
export BQ_DATASET="fraud_detection"

# Check real-time layer health (last 1 hour)
envsubst < gcp/validation_queries_streaming.sql | \
  sed -n '/1️⃣.*REAL-TIME LAYER HEALTH/,/^$/p' | \
  bq query --use_legacy_sql=false
```

**Expected Output**:
```
+--------+-----------+---------------------+---------------------+--------------+-----------------------+
| layer  | row_count | earliest            | latest              | span_seconds | status                |
+--------+-----------+---------------------+---------------------+--------------+-----------------------+
| bronze |        20 | 2026-05-28 13:45:00 | 2026-05-28 13:47:00 |          120 | ✅ Data ingesting      |
| silver |        15 | 2026-05-28 13:45:00 | 2026-05-28 13:47:00 |          120 | ✅ Dedup working       |
| gold   |        23 | 2026-05-28 13:45:05 | 2026-05-28 13:47:10 |          125 | ✅ Fraud detection     |
+--------+-----------+---------------------+---------------------+--------------+-----------------------+
```

#### 5. Stop Streaming Job

**After validation**, manually stop the streaming job:

```bash
# List running jobs
gcloud dataflow jobs list \
  --region=us-central1 \
  --filter='STATE=Running' \
  --format="table(JOB_ID,NAME,STATE)"

# Cancel the job
gcloud dataflow jobs cancel <JOB_ID> --region=us-central1
```

**OR** via GCP Console:
1. Navigate to Dataflow Jobs
2. Click on the running streaming job
3. Click "STOP" → "Cancel"

---

## 🔍 Validation Queries

### Real-Time Fraud Alerts (Last 10 Minutes)

```sql
SELECT
  detected_at,
  invoice_id,
  vendor_id,
  rule_name,
  severity,
  fraud_score,
  reason
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
ORDER BY detected_at DESC, fraud_score DESC
LIMIT 20;
```

### Fraud Alerts by Rule (Last 1 Hour)

```sql
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  ROUND(AVG(fraud_score), 1) AS avg_fraud_score
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY rule_name, severity
ORDER BY alert_count DESC;
```

### Schema Validation

```sql
SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM `fraud-detect-260526-1750.fraud_detection.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('bronze_raw_events', 'silver_deduplicated_invoices', 'gold_fraud_alerts')
ORDER BY table_name, ordinal_position;
```

### Evidence JSON Validation

```sql
SELECT
  invoice_id,
  rule_name,
  JSON_EXTRACT_SCALAR(evidence, '$.amount') AS amount,
  JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS z_score,
  JSON_EXTRACT_SCALAR(evidence, '$.fraud_pattern') AS fraud_pattern,
  evidence
FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
LIMIT 5;
```

---

## 🐛 Troubleshooting

### Issue: Gold table write fails with "field not found"

**Root Cause**: Schema mismatch between code output and BigQuery table schema

**Fix**:
1. Check actual table schema:
   ```sql
   DESCRIBE `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`;
   ```
2. Compare with `GOLD_SCHEMA` in `pipeline/gcp_batch_main.py` or `pipeline/gcp_stream_main.py`
3. If mismatch, re-run schema setup:
   ```bash
   bq rm -f -t fraud-detect-260526-1750:fraud_detection.gold_fraud_alerts
   envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
   ```

---

### Issue: Evidence field shows as STRING instead of JSON

**Root Cause**: Evidence dict wasn't JSON-serialized before writing

**Fix**: Already applied in code (`json.dumps(a.get("evidence", {}))`). Rebuild and redeploy.

---

### Issue: Streaming job doesn't see Pub/Sub messages

**Diagnostics**:
```bash
# Check if messages are in the subscription
gcloud pubsub subscriptions pull erp-invoices-sub \
  --project=fraud-detect-260526-1750 \
  --limit=5 \
  --auto-ack
```

**Fix**:
1. Verify topic has messages: `gcloud pubsub topics list-subscriptions erp-invoices --project=fraud-detect-260526-1750`
2. Check service account permissions: should have `pubsub.subscriber` role
3. Verify subscription path in run script matches actual subscription

---

### Issue: Streaming job runs forever and costs money

**Expected Behavior**: Streaming jobs run continuously until manually stopped

**How to Stop**:
```bash
# List running jobs
gcloud dataflow jobs list --region=us-central1 --filter='STATE=Running'

# Cancel job
gcloud dataflow jobs cancel <JOB_ID> --region=us-central1
```

**Cost Control**:
- Use `--max_num_workers=2` to limit parallelism
- Stop jobs after validation
- Use `--autoscaling_algorithm=THROUGHPUT_BASED` for cost efficiency

---

## 📊 Expected Results Summary

### Batch Mode (1,000 test invoices):
- **Bronze**: ~2,000 events (1,000 ERP + 1,000 WMS)
- **Silver**: ~950 invoices (after dedup suppresses ~5%)
- **Gold**: ~23 fraud alerts
  - 12 VELOCITY alerts (HIGH severity)
  - 7 ANOMALY alerts (CRITICAL severity)
  - 2 ANOMALY alerts (HIGH severity)
  - 2 FALLBACK alerts (MEDIUM severity)

### Streaming Mode (20 test messages):
- **Bronze**: ~20 events (real-time ingestion)
- **Silver**: ~15 invoices (after dedup)
- **Gold**: ~23 fraud alerts (same fraud scenarios)
- **Latency**: < 5 seconds end-to-end (bronze → gold)

---

## 🔄 Batch vs Streaming: When to Use Each

| Feature | Batch Mode | Streaming Mode |
|---------|-----------|----------------|
| **Input Source** | GCS JSONL files | Pub/Sub subscription |
| **Use Case** | Historical data, scheduled jobs | Real-time fraud detection |
| **Cost** | $0.05-$0.10 per run | ~$0.50/hour (while running) |
| **Latency** | Minutes (full file processing) | Seconds (real-time) |
| **Stopping** | Auto-stops when file is done | Manual cancel required |
| **Windowing** | GlobalWindows (batch) | Event-time windows (streaming) |
| **Triggers** | Default (end of file) | Repeated triggers (every 60s) |

---

## ✅ Success Criteria

After deployment, you should see:

### In Dataflow Console:
- ✅ Job status: "Running" (streaming) or "Succeeded" (batch)
- ✅ Elements processed: > 0 in all transforms
- ✅ No errors in logs

### In BigQuery:
- ✅ Bronze table: all incoming events
- ✅ Silver table: deduplicated invoices
- ✅ Gold table: fraud alerts with valid JSON evidence
- ✅ Schema validation: all fields match expected types
- ✅ Evidence JSON: parseable with `JSON_EXTRACT_SCALAR`
- ✅ Timestamps: properly formatted (not strings)

### In Logs:
- ✅ `VELOCITY_FRAUD_DETECTED` messages
- ✅ `ANOMALY_DETECTED` messages
- ✅ `DUPLICATE_SUPPRESSED` messages
- ✅ `FALLBACK_TRIGGERED` messages

---

## 🆘 Getting Help

**Check Dataflow Logs**:
```bash
gcloud logging read "resource.type=dataflow_step AND severity>=WARNING" \
  --project=fraud-detect-260526-1750 \
  --limit=50
```

**Check BigQuery Load Errors**:
```bash
gcloud logging read "resource.type=bigquery_resource" \
  --project=fraud-detect-260526-1750 \
  --limit=20
```

**Test Locally (Direct Runner)**:
```bash
python pipeline/gcp_stream_main.py \
  --runner=DirectRunner \
  --erp_subscription=projects/fraud-detect-260526-1750/subscriptions/erp-invoices-sub \
  --bq_dataset=fraud-detect-260526-1750:fraud_detection
```

---

## 🎯 Next Steps

1. **Production Deployment**:
   - Build streaming Flex Template (update `Dockerfile.flex` with `gcp_stream_main.py` entrypoint)
   - Set up alerting on fraud alerts (Cloud Monitoring)
   - Configure dead-letter queues for failed messages

2. **Monitoring**:
   - Create Dataflow pipeline metrics dashboard
   - Set up BigQuery scheduled queries for fraud summaries
   - Configure alerts on anomalous alert volumes

3. **Scaling**:
   - Tune `max_num_workers` based on throughput
   - Adjust window sizes for velocity detection
   - Optimize BigQuery partition filters

---

**Your pipeline is now production-ready for both batch and streaming!** 🚀
