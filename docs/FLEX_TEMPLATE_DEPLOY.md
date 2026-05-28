# Dataflow Flex Template — Deployment Guide 🐶

Complete guide for deploying the fraud detection pipeline in **TWO modes**:
- **BATCH MODE**: GCS JSONL files → Dataflow → BigQuery (cost: ~$0.05-$0.10 per run)
- **STREAMING MODE**: Pub/Sub → Dataflow → BigQuery (cost: ~$0.50/hour while running)

Both modes use the **same fraud detection logic** (velocity, anomaly, dedup) and write to the same BigQuery tables.

> This guide covers the FIRST deploy (step-by-step). For subsequent deploys, use:
> - `./gcp/build_flex_template.sh` + `./gcp/run_flex_template.sh` (batch)
> - `./gcp/setup_pubsub.sh` + `./gcp/run_stream_template.sh` (streaming)

---

## 0. Pre-flight checklist (one-time)

Confirm each of these BEFORE starting. Stop and fix anything that fails.

```bash
# Your real values
export PROJECT_ID=fraud-detect-260526-1750
export REGION=us-central1
export REPO=fraud-detection-pipeline-repo
export TEMPLATE_BUCKET=temp_staging_fraud_detection   # for temp/staging/template spec
export INPUT_BUCKET=fraud_detection_pipeline_bucket   # where your JSONL lives
export DATASET=fraud_detection

# ── auth ─────────────────────────────────────────────────────────────────
gcloud auth login
gcloud auth application-default login
gcloud config set project ${PROJECT_ID}

# ── confirm artifacts you said you already created ──────────────────────
gcloud artifacts repositories describe ${REPO} --location=${REGION}     # AR repo
gsutil ls -b gs://${TEMPLATE_BUCKET}                                    # template bucket
gsutil ls    gs://${INPUT_BUCKET}/                                       # should list your JSONL files
gcloud iam service-accounts list                                         # copy the SA email
```

When the last command runs, grab the **service account email** that looks like:
```
fraud-pipeline-sa@fraud-detect-260526-1750.iam.gserviceaccount.com
```
(NOT your personal `@gmail.com` — Dataflow rejects that.)

```bash
export SA_EMAIL=<paste-the-real-sa-email-here>
```

---

## 1. Enable required APIs

```bash
gcloud services enable \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    iam.googleapis.com
```

## 2. Grant the SA the roles Dataflow + BQ + GCS need

```bash
for ROLE in \
    roles/dataflow.worker \
    roles/dataflow.admin \
    roles/bigquery.dataEditor \
    roles/bigquery.jobUser \
    roles/storage.objectAdmin \
    roles/artifactregistry.reader; do
  gcloud projects add-iam-policy-binding ${PROJECT_ID} \
      --member="serviceAccount:${SA_EMAIL}" --role="${ROLE}" \
      --condition=None --quiet >/dev/null
done
```

YOU (the human launching jobs) also need `roles/dataflow.developer` and
`roles/iam.serviceAccountUser` on the SA — usually already there if you're project owner.

## 3. Create the BigQuery dataset + tables

The pipeline `WriteToBigQuery` calls use `CREATE_NEVER`, so tables must exist first.

```bash
# Create the dataset
bq --location=US mk -d --description "Fraud detection medallion" \
    ${PROJECT_ID}:${DATASET}

# Create the tables using the templated DDL
export BQ_PROJECT=${PROJECT_ID} BQ_DATASET=${DATASET} BQ_LOCATION=US
envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false --location=US
```

Verify:
```bash
bq ls ${PROJECT_ID}:${DATASET}
# expect: bronze_raw_events, silver_deduplicated_invoices,
#         gold_fraud_alerts, vendor_90day_baseline
```

## 4. Confirm input data is in GCS

```bash
gsutil ls -l gs://${INPUT_BUCKET}/
# expect at least:
#   gs://${INPUT_BUCKET}/wms_receiving.jsonl
#   gs://${INPUT_BUCKET}/erp_invoices.jsonl

# Peek the first line of each — must be JSON, not CSV!
gsutil cat gs://${INPUT_BUCKET}/wms_receiving.jsonl | head -1
gsutil cat gs://${INPUT_BUCKET}/erp_invoices.jsonl  | head -1
```
If you see comma-separated values (`abc,123,...`) instead of `{"...":"..."}`, the
pipeline will reject every row. Re-generate JSONL with:
```bash
python scripts/generate_mock_data.py --num-wms 1000 --num-invoices 1000
gsutil cp data/wms_receiving.jsonl gs://${INPUT_BUCKET}/
gsutil cp data/erp_invoices.jsonl  gs://${INPUT_BUCKET}/
```

## 5. Authenticate Docker against Artifact Registry

```bash
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet
```
> **Skip this step if you're using Cloud Build in step 6** — Cloud Build pushes
> for you and doesn't need local Docker auth.

## 6. Build & push the Flex Template image (use Cloud Build — recommended)

Why Cloud Build? Your laptop is behind a VPN that blocks `pypi.org`, so a local
`docker build` will fail to download Beam wheels. Cloud Build runs on Google's
infrastructure (full internet), builds linux/amd64 natively, and pushes the
resulting image straight to Artifact Registry.

```bash
export AR_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/fraud-detection-flex"

gcloud builds submit \
    --project=${PROJECT_ID} \
    --tag ${AR_URL}:v1 \
    --machine-type=e2-highcpu-8 \
    --timeout=20m \
    --gcs-source-staging-dir=gs://${TEMPLATE_BUCKET}/cloud-build-source \
    --config=- <<EOF
steps:
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-f', 'Dockerfile.flex', '-t', '${AR_URL}:v1', '-t', '${AR_URL}:latest', '.']
images:
  - '${AR_URL}:v1'
  - '${AR_URL}:latest'
options:
  machineType: E2_HIGHCPU_8
  logging: CLOUD_LOGGING_ONLY
timeout: 1200s
EOF
```

First build ≈ 5–8 min. Cost: a few cents per build (Cloud Build free tier
covers 120 build-minutes/day).

### Alternative: local `docker build` (only if you have public internet)

```bash
# Won't work on Walmart VPN — pypi.org is blocked
docker build --platform=linux/amd64 \
    --file Dockerfile.flex \
    --tag ${AR_URL}:v1 --tag ${AR_URL}:latest .
docker push ${AR_URL}:v1
docker push ${AR_URL}:latest
```

## 7. (skipped — image is already in AR after step 6)

Verify in Console: `Artifact Registry → fraud-detection-pipeline-repo → fraud-detection-flex`

## 8. Publish the Flex Template spec to GCS

This is the JSON file Dataflow reads to know HOW to launch the template.

```bash
gcloud dataflow flex-template build \
    gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json \
    --image ${AR_URL}:latest \
    --sdk-language PYTHON \
    --metadata-file gcp/metadata.json \
    --project ${PROJECT_ID}
```

Verify:
```bash
gsutil cat gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json | head -20
```

## 9. Launch a job 🚀

```bash
RUN_ID=$(date +%Y%m%d-%H%M%S)
JOB_NAME=fraud-detection-${RUN_ID}

gcloud dataflow flex-template run "${JOB_NAME}" \
    --template-file-gcs-location gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json \
    --project ${PROJECT_ID} \
    --region ${REGION} \
    --service-account-email ${SA_EMAIL} \
    --temp-location gs://${TEMPLATE_BUCKET}/temp \
    --staging-location gs://${TEMPLATE_BUCKET}/staging \
    --max-workers 2 \
    --worker-machine-type e2-small \
    --disk-size-gb 25 \
    --parameters wms_input=gs://${INPUT_BUCKET}/wms_receiving.jsonl \
    --parameters erp_input=gs://${INPUT_BUCKET}/erp_invoices.jsonl \
    --parameters bq_dataset=${PROJECT_ID}:${DATASET}
```

Watch it run:
```
https://console.cloud.google.com/dataflow/jobs?project=fraud-detect-260526-1750
```
Typical runtime: 4–7 min. ~75% of that is just Dataflow spinning up VMs.

## 10. Verify the results

```bash
bq query --use_legacy_sql=false \
  "SELECT rule_name, severity, COUNT(*) AS alerts
   FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
   WHERE DATE(detected_at) = CURRENT_DATE()
   GROUP BY 1,2 ORDER BY 1,2"
```

Expected: a handful of `VELOCITY` / `ANOMALY` / `FALLBACK` alerts.

---

## 11. [OPTIONAL] Pub/Sub Streaming Mode Setup

If you want **real-time fraud detection** from Pub/Sub instead of batch GCS files:

### 11a. Create Pub/Sub Topic and Subscription

```bash
export TOPIC_NAME=erp-invoices
export SUBSCRIPTION_NAME=erp-invoices-sub

# Create topic
gcloud pubsub topics create ${TOPIC_NAME} \
    --project=${PROJECT_ID} \
    --message-retention-duration=1d

# Create subscription
gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} \
    --topic=${TOPIC_NAME} \
    --project=${PROJECT_ID} \
    --ack-deadline=60 \
    --message-retention-duration=1d
```

**OR** use the automated script:
```bash
./gcp/setup_pubsub.sh
```

### 11b. Grant Pub/Sub Permissions to Service Account

```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/pubsub.subscriber" \
    --condition=None

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/pubsub.viewer" \
    --condition=None
```

### 11c. Publish Test Messages to Pub/Sub

```bash
# Generate test fraud data
python scripts/generate_mock_data.py --num-invoices 20

# Publish each invoice as a separate message
while IFS= read -r line; do
    echo "${line}" | gcloud pubsub topics publish ${TOPIC_NAME} \
        --project=${PROJECT_ID} \
        --message=- >/dev/null 2>&1
    sleep 0.1
done < data/erp_invoices.jsonl

echo "✅ Published 20 test messages to Pub/Sub"
```

**OR** use the automated script:
```bash
./gcp/publish_test_messages.sh
```

### 11d. Launch Streaming Dataflow Job

```bash
RUN_ID=$(date +%Y%m%d-%H%M%S)
JOB_NAME=fraud-stream-${RUN_ID}

python pipeline/gcp_stream_main.py \
  --runner=DataflowRunner \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --job_name=${JOB_NAME} \
  --staging_location=gs://${TEMPLATE_BUCKET}/staging \
  --temp_location=gs://${TEMPLATE_BUCKET}/temp \
  --service_account_email=${SA_EMAIL} \
  --erp_subscription=projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME} \
  --bq_dataset=${PROJECT_ID}:${DATASET} \
  --machine_type=e2-small \
  --max_num_workers=2 \
  --num_workers=1 \
  --autoscaling_algorithm=THROUGHPUT_BASED
```

**OR** use the automated script:
```bash
./gcp/run_stream_template.sh
```

### 11e. Monitor Streaming Job

Watch the job in Dataflow Console:
```
https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}
```

Check BigQuery for real-time fraud alerts:
```bash
bq query --y_sql=false \
  "SELECT detected_at, invoice_id, vendor_id, rule_name, severity, fraud_score, reason
   FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
   WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
   ORDER BY detected_at DESC
   LIMIT 20"
```

### 11f. ❗ STOP Streaming Job (Important!)

**Streaming jobs run FOREVER until manually stopped**. After validation:

```bash
# List running jobs
gcloud dataflow jobs list \
  --region=${REGION} \
  --filter='STATE=Running' \
  --format="table(JOB_ID,NAME,STATE)"

# Cancel the streaming job
gcloud dataflow jobs cancel <JOB_ID> --region=${REGION}
```

**Cost**: Streaming jobs cost ~$0.50/hour with 2 e2-small workers. Always stop after testing!

---

## 12. Validation Queries

### Schema Validation (ensure fields match code)

```bash
bq query --use_legacy_sql=false \
  "SELECT table_name, column_name, data_type, is_nullable
   FROM \`${PROJECT_ID}.${DATASET}.INFORMATION_SCHEMA.COLUMNS\`
   WHERE table_name = 'gold_fraud_alerts'
   ORDER BY ordinal_position"
```

**Expected**: `evidence` should be **JSON** type (not STRING), `window_start`/`window_end` should be **TIMESTAMP** types.

### Evidence JSON Validation

```bash
bq query --use_legacy_sql=false \
  "SELECT
     invoice_id,
     rule_name,
     JSON_EXTRACT_SCALAR(evidence, '$.amount') AS amount,
     JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS z_score,
     JSON_EXTRACT_SCALAR(evideud_pattern') AS fraud_pattern,
     evidence
   FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
   WHERE DATE(detected_at) = CURRENT_DATE()
   LIMIT 5"
```

**Expected**: All `JSON_EXTRACT_SCALAR` calls should return values (no errors).

### Fraud Alerts by Rule

```bash
bq query --use_legacy_sql=false \
  "SELECT
     rule_name,
     severity,
     COUNT(*) AS alert_count,
     ROUND(AVG(fraud_score), 1) AS avg_fraud_score
   FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
   WHERE DATE(detected_at) = CURRENT_DATE()
   GROUP BY rule_name, severity
   ORDER BY alert_count DESC"
```

**Expected** (for test data with 1,000 invoices):
```
+-----------+----------+-------------+------------------+
| rule_name | severity | alert_count | avg_fraud_score  |
+-----------+----------+-------------+------------------+
| VELOCITY  | HIGH     |          12 |             60.0 |
| ANOMALY   | CRITICAL |           7 |             90.0 |
| ANOMALY   | HIGH     |           2 |             35.0 |
| FALLBACK  | MEDIUM   |           2 |             25.0 |
+-----------+----------+-------------+------------------+
```

### Duplicate Detection (should be ZERO in silver)

```bash
bq query --use_legacy_sql=false \
  "WITH invoice_counts AS (
     SELECT invoice_id, COUNT(*) AS cnt
     FROM \`${PROJECT_ID}.${DATASET}.silver_deduplicated_invoices\`
     WHERE DATE(invoice_timestamp) = CURRENT_DATE()
     GROUP BY invoice_id
   )
   SELECT * FROM invoice_counts WHERE cnt > 1"
```

**Expected**: 0 rows (dedup is working correctly).

---

## 13. Troubleshooting

### Issue: "Field not found" error when writing to BigQuery

**Symptoms**:
```
Error: Field 'evidence' not found in schema
```

**Root Cause**: BigQuery table schema doesn't match code output

**Fix**:
1. Check actual table schema:
   ```bash
   bq show --schema --format=prettyjson ${PROJECT_ID}:${DATASET}.gold_fraud_alerts
   ```

2. Compare with expected schema in `gcp/bq_schema.sql`

3. If mismatch, drop and recreate table:
   ```bash
   bq rm -f -t ${PROJECT_ID}:${DATASET}.gold_fraud_alerts
   
   export BQ_PROJECT=${PROJECT_ID} BQ_DATASET=${DATASET} BQ_LOCATION=US
   envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
   ```

### Issue: Evidence shows as STRING instead of JSON type

**Root Cause**: Table was created before schema fix (evidence must be JSON type)

**Fix**: Re-create table with correct schema (see above)

### Issue: No fraud alerts in gold table

**Diagnostics**:
1. Check if data reached bronze:
   ```bash
   bq query --use_legacy_sql=false \
     "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.bronze_raw_events\` \
      WHERE DATE(event_timestamp) = CURRENT_DATE()"
   ```

2. Check if data reached silver:
   ```bash
   bq query --use_legacy_sql=false \
     "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.silver_deduplicated_invoices\` \
      WHERE DATE(invoice_timestamp) = CURRENT_DATE()"
   ```

3. Check vendor baseline table is populated:
   ```bash
   bq query --use_legacy_sql=false \
     "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.vendor_90day_baseline\`"
   ```
   **Expected**: 5 vendors (V-1001 through V-1005)

4. Check Dataflow logs for errors:
   ```bash
   gcloud logging read "resource.type=dataflow_step AND severity>=WARNING" \
     --project=${PROJECT_ID} \
     --limit=50 \
     --format="table(timestamp,severity,textPayload)"
   ```

**Common Fixes**:
- Vendor baseline empty: Re-run `envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false`
- Schema mismatch: Drop and recreate tables (see above)
- Bad test data: Regenerate with `python scripts/generate_mock_data.py`

### Issue: Streaming job doesn't see Pub/Sub messages

**Diagnostics**:
```bash
# Check if messages exist in subscription
gcloud pubsub subscriptions pull ${SUBSCRIPTION_NAME} \
  --project=${PROJECT_ID} \
  --limit=5 \
  --auto-ack
```

**If no messages**, republish:
```bash
./gcp/publish_test_messages.sh
```

**If messages exist but job not processing**:
1. Verify service account has `pubsub.subscriber` role
2. Check subscription path in launch command matches actual subscription
3. Check Dataflow worker logs for permission errors

---

## 14. Tear it all down (stop billing)

```bash
# 1. cancel any running jobs
gcloud dataflow jobs list --region=${REGION} --status=active --format="value(JOB_ID)" \
  | xargs -I{} gcloud dataflow jobs cancel {} --region=${REGION} --quiet

# 2. delete the BigQuery dataset
bq rm -r -f -d ${PROJECT_ID}:${DATASET}

# 3. delete the GCS objects you uploaded + template spec
gsutil -m rm -r gs://${INPUT_BUCKET}/** gs://${TEMPLATE_BUCKET}/** || true

# 4. delete the Artifact Registry images (storage is per-GB, small but non-zero)
gcloud artifacts repositories delete ${REPO} --location=${REGION} --quiet

# 5. (nuclear) delete the whole project — guarantees $0 forever
gcloud projects delete ${PROJECT_ID}
```

---

## 15. v2 — Quick Redeploy Commands

Once you've done all of the above ONCE, future deploys are much shorter:

### Batch Mode (GCS Input)

```bash
# Rebuild + republish Flex Template
./gcp/build_flex_template.sh

# Launch batch job
SA_EMAIL=<your-sa>@${PROJECT_ID}.iam.gserviceaccount.com \
  ./gcp/run_flex_template.sh
```

### Streaming Mode (Pub/Sub Input)

```bash
# One-time: Setup Pub/Sub (only needed once)
./gcp/setup_pubsub.sh

# Publish test messages
./gcp/publish_test_messages.sh

# Launch streaming job
./gcp/run_stream_template.sh

# After validation, STOP the job:
gcloud dataflow jobs list --region=us-central1 --filter='STATE=Running'
gcloud dataflow jobs cancel <JOB_ID> --region=us-central1
```

---

## 16. Architecture Summary

| Feature | Batch Mode | Streaming Mode |
|---------|------------|----------------|
| **Input Source** | GCS JSONL files | Pub/Sub subscription |
| **Entrypoint** | `pipeline/gcp_batch_main.py` | `pipeline/gcp_stream_main.py` |
| **Fraud Logic** | Velocity + Anomaly + Dedup | ✅ Same transforms |
| **BigQuery Tables** | Bronze → Silver → Gold | ✅ Same tables |
| **Stopping** | Auto-stops when done | ⚠️ Manual cancel required |
| **Cost** | ~$0.05-$0.10 per run | ~$0.50/hour (while running) |
| **Latency** | Minutes (batch processing) | Seconds (real-time) |
| **Use Case** | Historical data, scheduled jobs | Real-time fraud detection |

---

## 17. TL;DR Quick Reference

### Batch Deployment

| Step | Command |
|---|---|
| Build & push image + publish spec | `./gcp/build_flex_template.sh` |
| Launch batch job | `SA_EMAIL=... ./gcp/run_flex_template.sh` |
| View results | `bq query 'SELECT rule_name, COUNT(*) ... gold_fraud_alerts ...'` |

### Streaming Deployment

| Step | Command |
|---|---|
| Setup Pub/Sub (one-time) | `./gcp/setup_pubsub.sh` |
| Publish test messages | `./gcp/publish_test_messages.sh` |
| Launch streaming job | `./gcp/run_stream_template.sh` |
| View real-time alerts | `bq query 'SELECT * FROM gold_fraud_alerts WHERE detected_at >= ...'` |
| ⚠️ STOP job | `gcloud dataflow jobs cancel <JOB_ID> --region=us-central1` |

### Teardown

| Step | Command |
|---|---|
| Cancel all jobs | `gcloud dataflow jobs list ... \| xargs ... cancel` |
| Delete dataset | `bq rm -r -f -d ${PROJECT_ID}:${DATASET}` |
| Delete GCS buckets | `gsutil -m rm -r gs://...` |
| 💣 Nuclear option | `gcloud projects delete ${PROJECT_ID}` |
