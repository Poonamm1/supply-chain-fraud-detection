#!/usr/bin/env bash
# =============================================================================
# gcp/run_flex_template.sh
# =============================================================================
# Launches a Dataflow BATCH job from the published Flex Template.
# The job exits when it's done — no idle worker bills.
# =============================================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
REGION="${REGION:-us-central1}"
TEMPLATE_BUCKET="${TEMPLATE_BUCKET:-temp_staging_fraud_detection}"
INPUT_BUCKET="${INPUT_BUCKET:-fraud_detection_pipeline_bucket}"
DATASET="${DATASET:-fraud_detection}"

TEMPLATE_GCS_PATH="${TEMPLATE_GCS_PATH:-gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json}"
TEMP_LOCATION="${TEMP_LOCATION:-gs://${TEMPLATE_BUCKET}/temp}"
STAGING_LOCATION="${STAGING_LOCATION:-gs://${TEMPLATE_BUCKET}/staging}"

# Set this explicitly if you want, otherwise it must be exported in the shell.
: "${SA_EMAIL:?Set SA_EMAIL to your Dataflow service account email}"

# JSONL inputs
WMS_INPUT="${WMS_INPUT:-gs://${INPUT_BUCKET}/wms_receiving.jsonl}"
ERP_INPUT="${ERP_INPUT:-gs://${INPUT_BUCKET}/erp_invoices.jsonl}"

BQ_DATASET="${BQ_DATASET:-${PROJECT_ID}:${DATASET}}"

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
echo " BQ dataset  : ${BQ_DATASET}"
echo " SA          : ${SA_EMAIL}"
echo "================================================================"

gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location="${TEMPLATE_GCS_PATH}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --service-account-email="${SA_EMAIL}" \
  --temp-location="${TEMP_LOCATION}" \
  --staging-location="${STAGING_LOCATION}" \
  --max-workers=2 \
  --worker-machine-type=e2-small \
  --parameters="mode=batch,wms_input=${WMS_INPUT},erp_input=${ERP_INPUT},bq_dataset=${BQ_DATASET}"

echo ""
echo "✅ Job submitted. Track at:"
echo "   https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "▶ When it finishes, query results:"
echo "   bq query --use_legacy_sql=false \\"
echo "     'SELECT rule_name, severity, COUNT(*) AS alerts"
echo "      FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`"
echo "      GROUP BY 1,2 ORDER BY 1,2'"