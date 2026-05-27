# Dataflow Flex Template — Manual Deploy Guide 🐶

End-to-end, copy-paste-able. Use this for the FIRST deploy so you understand
each step; use `./gcp/build_flex_template.sh` + `./gcp/run_flex_template.sh`
for the SECOND deploy (v2) which does the same thing in 2 commands.

> This guide assumes the BigQuery code path (not AlloyDB). The pipeline writes
> to `bronze_raw_events`, `silver_deduplicated_invoices`, `gold_fraud_alerts`,
> and reads from `vendor_90day_baseline` — all in BigQuery.

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

## 11. Tear it all down in 2-3 days (so Google doesn't bill you)

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

## v2 — re-deploy with one command

Once you've done all of the above ONCE, future deploys are this short:

```bash
# rebuild + repush + republish template spec
./gcp/build_flex_template.sh

# launch a fresh job
SA_EMAIL=<your-sa>@${PROJECT_ID}.iam.gserviceaccount.com \
  ./gcp/run_flex_template.sh
```

The scripts have all your real values baked in as defaults. Override anything
with env vars if needed.

---

## TL;DR

| Step | Command |
|---|---|
| Build & push image + publish spec | `./gcp/build_flex_template.sh` |
| Launch a job | `SA_EMAIL=... ./gcp/run_flex_template.sh` |
| See results | `bq query 'SELECT rule_name, COUNT(*) ... gold_fraud_alerts ...'` |
| Stop the bleeding | `gcloud projects delete fraud-detect-260526-1750` |
