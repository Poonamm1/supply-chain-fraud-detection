#!/usr/bin/env bash
# =============================================================================
# gcp/run_stream_template.sh
# =============================================================================
# Launches the STREAMING Dataflow job that reads from Pub/Sub subscription.
#
# Usage:
#   ./gcp/run_stream_template.sh
#
# Stop the job:
#   gcloud dataflow jobs list --region=us-central1 --filter='STATE=Running'
#   gcloud dataflow jobs cancel JOB_ID --region=us-central1
# =============================================================================
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
REGION="${REGION:-us-central1}"
BQ_DATASET="${BQ_DATASET:-${PROJECT_ID}:fraud_detection}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-raud-detection-sa@${PROJECT_ID}.iam.gserviceaccount.com}"
STAGING_LOCATION="${STAGING_LOCATION:-gs://temp_staging_fraud_detection/staging}"
TEMP_LOCATION="${TEMP_LOCATION:-gs://temp_staging_fraud_detection/temp}"

# Pub/Sub subscriptions (updated names)
WMS_SUBSCRIPTION="${WMS_SUBSCRIPTION:-projects/${PROJECT_ID}/subscriptions/wms-events-sub}"
ERP_SUBSCRIPTION="${ERP_SUBSCRIPTION:-projects/${PROJECT_ID}/subscriptions/erp-events-sub}"

JOB_NAME="fraud-stream-$(date +%Y%m%d-%H%M%S)"

echo "════════════════════════════════════════════════════════════════════════"
echo " Launching Streaming Dataflow Job"
echo "════════════════════════════════════════════════════════════════════════"
echo " Project:          ${PROJECT_ID}"
echo " Region:           ${REGION}"
echo " Job Name:         ${JOB_NAME}"
echo " WMS Subscription: ${WMS_SUBSCRIPTION}"
echo " ERP Subscription: ${ERP_SUBSCRIPTION}"
echo " BQ Dataset:       ${BQ_DATASET}"
echo " Service Account:  ${SERVICE_ACCOUNT}"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

command -v gcloud >/dev/null || { echo "❌ gcloud not installed"; exit 1; }

echo "▶ Running Python streaming pipeline directly (for testing)..."
echo "  (In production, use a Flex Template with gcp_stream_main.py as entrypoint)"
echo ""

# For testing, run directly with DirectRunner or DataflowRunner
python pipeline/gcp_stream_main.py \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --staging_location="${STAGING_LOCATION}" \
  --temp_location="${TEMP_LOCATION}" \
  --service_account_email="${SERVICE_ACCOUNT}" \
  --wms_subscription="${WMS_SUBSCRIPTION}" \
  --erp_subscription="${ERP_SUBSCRIPTION}" \
  --bq_dataset="${BQ_DATASET}" \
  --machine_type=e2-small \
  --max_num_workers=2 \
  --num_workers=1 \
  --autoscaling_algorithm=THROUGHPUT_BASED

echo ""
echo "✅ Streaming job launched: ${JOB_NAME}"
echo ""
echo "📊 Monitor job:"
echo "   https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "🛑 To stop the job after validation:"
echo "   gcloud dataflow jobs list --region=${REGION} --filter='STATE=Running'"
echo "   gcloud dataflow jobs cancel <JOB_ID> --region=${REGION}"
echo ""
