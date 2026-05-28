#!/usr/bin/env bash
# =============================================================================
# gcp/run_flex_template.sh
# =============================================================================
# Launches a Dataflow BATCH job from the published Flex Template.
# The job exits when it's done — no idle worker bills.
#
# Cost cap: --max-workers=2 --worker-machine-type=e2-small  → ≤ $0.10 per run
#
# Required env vars (must be exported OR baked in below):
#   SA_EMAIL  → Dataflow worker service account
#               Example: fraud-pipeline-sa@fraud-detect-260526-1750.iam.gserviceaccount.com
#
# Usage:
#   SA_EMAIL=foo@... ./gcp/run_flex_template.sh
# =============================================================================
set -euo pipefail

# ── Defaults (baked from your actual GCP setup) ──────────────────────────
PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
REGION="${REGION:-us-central1}"
TEMPLATE_BUCKET="${TEMPLATE_BUCKET:-temp_staging_fraud_detection}"
INPUT_BUCKET="${INPUT_BUCKET:-fraud_detection_pipeline_bucket}"
DATASET="${DATASET:-fraud_detection}"
TEMPLATE_GCS_PATH="${TEMPLATE_GCS_PATH:-gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json}"

# JSONL inputs (you said both already uploaded to ${INPUT_BUCKET})
WMS_INPUT="${WMS_INPUT:-gs://${INPUT_BUCKET}/wms_receiving.jsonl}"
ERP_INPUT="${ERP_INPUT:-gs://${INPUT_BUCKET}/erp_invoices.jsonl}"

# REQUIRED — no sane default possible
: "${SA_EMAIL:?Set SA_EMAIL=<your-sa>@${PROJECT_ID}.iam.gserviceaccount.com   (gcloud iam service-accounts list)}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
JOB_NAME="fraud-detection-${RUN_ID}"

echo "================================================================"
echo " Launching Dataflow Flex Template Job"
echo "================================================================"
echo " Job name    : ${JOB_NAME}"
echo " Project     : ${PROJECT_ID}"
echo " Region      : ${REGION}"
echo " Template    : ${TEMPLATE_GCS_PATH}"
echo " WMS input   : ${WMS_INPUT}"
echo " ERP input   : ${ERP_INPUT}"
echo " BQ dataset  : ${PROJECT_ID}:${DATASET}"
echo " SA          : ${SA_EMAIL}"
echo "================================================================"

gcloud dataflow flex-template run "${JOB_NAME}" \
    --template-file-gcs-location "${TEMPLATE_GCS_PATH}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --service-account-email "${SA_EMAIL}" \
    --temp-location "gs://${TEMPLATE_BUCKET}/temp" \
    --staging-location "gs://${TEMPLATE_BUCKET}/staging" \
    --max-workers 2 \
    --worker-machine-type e2-small \
#    --disk-size-gb 25 \
    --parameters "wms_input=${WMS_INPUT}" \
    --parameters "erp_input=${ERP_INPUT}" \
    --parameters "bq_dataset=${PROJECT_ID}:${DATASET}"

echo ""
echo "✅ Job submitted. Track at:"
echo "   https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "▶ When it finishes, query results:"
echo "   bq query --use_legacy_sql=false \\"
echo "     'SELECT rule_name, severity, COUNT(*) AS alerts"
echo "      FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`"
echo "      WHERE DATE(detected_at) = CURRENT_DATE()"
echo "      GROUP BY 1,2 ORDER BY 1,2'"
