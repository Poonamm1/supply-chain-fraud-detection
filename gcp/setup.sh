#!/usr/bin/env bash
# gcp/setup.sh — one-shot GCP project setup with cost guardrails.
#
# Idempotent: safe to re-run. Creates:
#   * GCS bucket for staging + input data
#   * BigQuery dataset + tables (via bq_schema.sql)
#   * Service account with least-privilege roles
#   * Budget alert at $5 / $10 / $25 USD  <-- COST SAFETY NET
#
# Pre-req: gcloud auth login && gcloud config set project YOUR_PROJECT_ID

set -euo pipefail

# ── CONFIGURATION ───────────────────────────────────────────────────────
: "${PROJECT_ID:?PROJECT_ID env var is required}"
: "${BILLING_ACCOUNT_ID:?BILLING_ACCOUNT_ID env var is required (gcloud billing accounts list)}"
REGION="${REGION:-us-central1}"
BQ_LOCATION="${BQ_LOCATION:-US}"
BUCKET="${BUCKET:-${PROJECT_ID}-fraud-pipeline}"
DATASET="${DATASET:-fraud_detection}"
SA_NAME="${SA_NAME:-fraud-pipeline-sa}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
BUDGET_NAME="${BUDGET_NAME:-fraud-pipeline-budget}"

echo "▶ Setting project=${PROJECT_ID} region=${REGION}"
gcloud config set project "${PROJECT_ID}" -q

# ── ENABLE APIS (only what we actually need) ────────────────────────────
echo "▶ Enabling required APIs..."
gcloud services enable \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    iam.googleapis.com \
    cloudbilling.googleapis.com \
    billingbudgets.googleapis.com \
    -q

# ── GCS BUCKET (input + staging) ────────────────────────────────────────
echo "▶ Ensuring GCS bucket gs://${BUCKET}"
if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
    gsutil mb -p "${PROJECT_ID}" -l "${REGION}" -b on "gs://${BUCKET}"
    # Lifecycle: delete temp files after 7 days = no surprise storage bills
    cat > /tmp/lifecycle.json <<'EOF'
{ "lifecycle": { "rule": [
    { "action": {"type": "Delete"},
      "condition": {"age": 7, "matchesPrefix": ["staging/", "temp/", "input/"]}}
]}}
EOF
    gsutil lifecycle set /tmp/lifecycle.json "gs://${BUCKET}"
fi

# ── BIGQUERY DATASET + TABLES ───────────────────────────────────────────
echo "▶ Creating BigQuery objects in ${PROJECT_ID}:${DATASET}"
# envsubst lets us parameterize the DDL safely
export BQ_PROJECT="${PROJECT_ID}" BQ_DATASET="${DATASET}" BQ_LOCATION
envsubst < "$(dirname "$0")/bq_schema.sql" | \
    bq query --use_legacy_sql=false --location="${BQ_LOCATION}" -q

# ── SERVICE ACCOUNT (least privilege) ───────────────────────────────────
echo "▶ Service account ${SA_EMAIL}"
if ! gcloud iam service-accounts describe "${SA_EMAIL}" >/dev/null 2>&1; then
    gcloud iam service-accounts create "${SA_NAME}" \
        --display-name="Fraud Pipeline (Dataflow worker)"
fi
for ROLE in roles/dataflow.worker \
            roles/bigquery.dataEditor \
            roles/bigquery.jobUser \
            roles/storage.objectAdmin; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" --role="${ROLE}" -q --condition=None >/dev/null
done

# ── BUDGET ALERT (the safety net you actually care about) ───────────────
echo "▶ Creating budget alerts at \$5 / \$10 / \$25 USD"
# Idempotency: skip if a budget with the same display name already exists
if ! gcloud billing budgets list --billing-account="${BILLING_ACCOUNT_ID}" \
        --format="value(displayName)" 2>/dev/null | grep -q "^${BUDGET_NAME}$"; then
    gcloud billing budgets create \
        --billing-account="${BILLING_ACCOUNT_ID}" \
        --display-name="${BUDGET_NAME}" \
        --budget-amount=25USD \
        --threshold-rule=percent=20  \
        --threshold-rule=percent=40  \
        --threshold-rule=percent=80  \
        --threshold-rule=percent=100 \
        --filter-projects="projects/${PROJECT_ID}" || \
        echo "⚠ Budget creation failed — set up manually in Console"
fi

echo ""
echo "✅ GCP setup complete."
echo "   Bucket   : gs://${BUCKET}"
echo "   Dataset  : ${PROJECT_ID}:${DATASET}"
echo "   SA email : ${SA_EMAIL}"
echo "   Budget   : \$25 USD with alerts at 20/40/80/100%"
echo ""
echo "Next: ./gcp/trigger_pipeline.sh   (manual on-demand run)"
