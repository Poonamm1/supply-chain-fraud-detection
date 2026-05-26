#!/usr/bin/env bash
# gcp/trigger_pipeline.sh — MANUAL on-demand pipeline run.
#
# This is THE script you run when you want fraud detection to happen.
# Nothing automatic, no streaming, no piling up.
#
# Workflow:
#   1. Generate mock data locally (offline, free)
#   2. Upload to GCS
#   3. Launch a Dataflow BATCH job (reads files, writes BQ, exits)
#   4. Job dies as soon as data is processed -> no idle worker bills
#
# Cost cap: --max_num_workers=2 --machine_type=e2-small
# Realistic cost per run for ~5K rows: < $0.10

set -euo pipefail

: "${PROJECT_ID:?PROJECT_ID env var is required}"
REGION="${REGION:-us-central1}"
BUCKET="${BUCKET:-${PROJECT_ID}-fraud-pipeline}"
DATASET="${DATASET:-fraud_detection}"
SA_EMAIL="${SA_EMAIL:-fraud-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com}"

NUM_WMS="${NUM_WMS:-1000}"
NUM_INVOICES="${NUM_INVOICES:-1000}"
RUN_ID="$(date +%Y%m%d-%H%M%S)"

echo "▶ Run ID: ${RUN_ID}"

# ── STEP 1: Generate mock data LOCALLY (free) ───────────────────────────
echo "▶ Generating ${NUM_WMS} WMS + ${NUM_INVOICES} ERP events locally"
python scripts/generate_mock_data.py \
    --num-wms "${NUM_WMS}" \
    --num-invoices "${NUM_INVOICES}"

# ── STEP 2: Upload to GCS (cents per GB) ────────────────────────────────
GCS_INPUT="gs://${BUCKET}/input/${RUN_ID}"
echo "▶ Uploading to ${GCS_INPUT}/"
gsutil -q cp data/wms_receiving.jsonl "${GCS_INPUT}/wms_receiving.jsonl"
gsutil -q cp data/erp_invoices.jsonl  "${GCS_INPUT}/erp_invoices.jsonl"

# ── STEP 3: Launch Dataflow BATCH job ───────────────────────────────────
# BATCH = job exits when done. STREAMING = forever-running $$$.
echo "▶ Launching Dataflow batch job (this finishes & exits — no idle cost)"
python -m pipeline.gcp_batch_main \
    --runner=DataflowRunner \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --service_account_email="${SA_EMAIL}" \
    --temp_location="gs://${BUCKET}/temp" \
    --staging_location="gs://${BUCKET}/staging" \
    --job_name="fraud-detection-${RUN_ID}" \
    --wms_input="${GCS_INPUT}/wms_receiving.jsonl" \
    --erp_input="${GCS_INPUT}/erp_invoices.jsonl" \
    --bq_dataset="${PROJECT_ID}:${DATASET}" \
    --max_num_workers=2 \
    --machine_type=e2-small \
    --disk_size_gb=25 \
    --save_main_session \
    --setup_file=./setup.py

echo ""
echo "✅ Job submitted. Track at:"
echo "   https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}"
echo ""
echo "After completion, query results:"
echo "   bq query --use_legacy_sql=false \\"
echo "     'SELECT rule_name, severity, COUNT(*) AS alerts"
echo "      FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`"
echo "      WHERE DATE(detected_at) = CURRENT_DATE()"
echo "      GROUP BY 1,2 ORDER BY 1,2'"
