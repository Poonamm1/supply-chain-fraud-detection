#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# End-to-End Test Scenario for Fraud Detection Pipeline
# ═══════════════════════════════════════════════════════════════════════════
# This script validates that fraud detection is working end-to-end by:
# 1. Generating mock data with guaranteed fraud scenarios
# 2. Uploading to GCS
# 3. Triggering the Dataflow pipeline
# 4. Waiting for completion
# 5. Running validation queries in BigQuery
# 6. Exporting results summary
#
# Usage:
#   ./gcp/test_fraud_detection.sh
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
DATASET="${DATASET:-fraud_detection}"
BUCKET="${BUCKET:-fraud_detection_pipeline_bucket}"
REGION="${REGION:-us-central1}"

echo "════════════════════════════════════════════════════════════════════════"
echo " 🐶 Fraud Detection Pipeline — E2E Test Scenario"
echo "════════════════════════════════════════════════════════════════════════"
echo " Project:  ${PROJECT_ID}"
echo " Dataset:  ${DATASET}"
echo " Bucket:   ${BUCKET}"
echo " Region:   ${REGION}"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

# ── Step 1: Generate Mock Data ───────────────────────────────────────────
echo "📦 Step 1: Generating mock data with fraud scenarios..."
python scripts/generate_mock_data.py \
  --wms-out data/wms_receiving.jsonl \
  --erp-out data/erp_invoices.jsonl \
  --num-wms 1000 \
  --num-invoices 1000

echo ""
echo "✅ Mock data generated:"
echo "   • WMS events:     $(wc -l < data/wms_receiving.jsonl) lines"
echo "   • ERP invoices:   $(wc -l < data/erp_invoices.jsonl) lines"
echo ""

# ── Step 2: Upload to GCS ────────────────────────────────────────────────
echo "📤 Step 2: Uploading data to GCS..."
gsutil -q cp data/wms_receiving.jsonl "gs://${BUCKET}/"
gsutil -q cp data/erp_invoices.jsonl "gs://${BUCKET}/"

echo "✅ Data uploaded to gs://${BUCKET}/"
echo ""

# ── Step 3: Verify BigQuery Baseline ─────────────────────────────────────
echo "🔍 Step 3: Verifying vendor baseline table..."
BASELINE_COUNT=$(bq query --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.vendor_90day_baseline\`" \
  | tail -n 1)

if [ "$BASELINE_COUNT" -lt 5 ]; then
  echo "❌ ERROR: Vendor baseline table has only ${BASELINE_COUNT} rows!"
  echo "   Expected: 5 vendors (V-1001 through V-1005)"
  echo ""
  echo "   Fixing: Re-running schema setup..."
  export BQ_PROJECT="${PROJECT_ID}"
  export BQ_DATASET="${DATASET}"
  export BQ_LOCATION="${REGION}"
  envsubst < gcp/bq_schema.sql | bq query --use_legacy_sql=false
  echo "✅ Baseline table repopulated."
else
  echo "✅ Vendor baseline table has ${BASELINE_COUNT} vendors"
fi
echo ""

# ── Step 4: Trigger Pipeline ─────────────────────────────────────────────
echo "🚀 Step 4: Triggering Dataflow pipeline..."
echo "   (This will take ~3-5 minutes to complete)"
echo ""

JOB_NAME="fraud-test-$(date +%Y%m%d-%H%M%S)"
gcloud dataflow flex-template run "${JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --template-file-gcs-location="gs://temp_staging_fraud_detection/templates/fraud-detection.json" \
  --parameters="wms_input=gs://${BUCKET}/wms_receiving.jsonl" \
  --parameters="erp_input=gs://${BUCKET}/erp_invoices.jsonl" \
  --parameters="bq_dataset=${PROJECT_ID}:${DATASET}" \
  --parameters="staging_location=gs://temp_staging_fraud_detection/staging" \
  --parameters="temp_location=gs://temp_staging_fraud_detection/temp" \
  --parameters="machine_type=e2-small" \
  --parameters="max_num_workers=2" \
  --parameters="service_account_email=raud-detection-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo ""
echo "✅ Job submitted: ${JOB_NAME}"
echo "   Monitor at: https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""

# ── Step 5: Wait for Completion ──────────────────────────────────────────
echo "⏳ Step 5: Waiting for job completion..."
echo "   (Checking every 30 seconds...)"
echo ""

while true; do
  JOB_STATE=$(gcloud dataflow jobs list \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --filter="name:${JOB_NAME}" \
    --format="value(state)" \
    --limit=1)
  
  echo "   Job state: ${JOB_STATE}"
  
  if [ "${JOB_STATE}" = "JOB_STATE_DONE" ]; then
    echo ""
    echo "✅ Job completed successfully!"
    break
  elif [ "${JOB_STATE}" = "JOB_STATE_FAILED" ] || [ "${JOB_STATE}" = "JOB_STATE_CANCELLED" ]; then
    echo ""
    echo "❌ ERROR: Job failed or was cancelled!"
    echo "   Check logs: gcloud dataflow jobs show ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
    exit 1
  fi
  
  sleep 30
done
echo ""

# ── Step 6: Validate Results ─────────────────────────────────────────────
echo "📊 Step 6: Validating fraud detection results..."
echo ""

# Count alerts by rule
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 FRAUD ALERTS BY RULE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bq query --use_legacy_sql=false --format=pretty <<EOF
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  ROUND(AVG(fraud_score), 1) AS avg_fraud_score
FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
WHERE DATE(detected_at) >= CURRENT_DATE() - 1
GROUP BY rule_name, severity
ORDER BY
  CASE rule_name
    WHEN 'VELOCITY' THEN 1
    WHEN 'ANOMALY' THEN 2
    WHEN 'FALLBACK' THEN 3
  END,
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
  END;
EOF

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔝 TOP 5 HIGHEST-RISK ALERTS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bq query --use_legacy_sql=false --format=pretty <<EOF
SELECT
  invoice_id,
  vendor_id,
  rule_name,
  severity,
  fraud_score,
  reason
FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
WHERE DATE(detected_at) >= CURRENT_DATE() - 1
ORDER BY fraud_score DESC, detected_at DESC
LIMIT 5;
EOF

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 MEDALLION LAYER HEALTH"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bq query --use_legacy_sql=false --format=pretty <<EOF
WITH layer_counts AS (
  SELECT 'bronze' AS layer, COUNT(*) AS row_count
  FROM \`${PROJECT_ID}.${DATASET}.bronze_raw_events\`
  WHERE DATE(event_timestamp) >= CURRENT_DATE() - 1
  
  UNION ALL
  
  SELECT 'silver' AS layer, COUNT(*) AS row_count
  FROM \`${PROJECT_ID}.${DATASET}.silver_deduplicated_invoices\`
  WHERE DATE(invoice_timestamp) >= CURRENT_DATE() - 1
  
  UNION ALL
  
  SELECT 'gold' AS layer, COUNT(*) AS row_count
  FROM \`${PROJECT_ID}.${DATASET}.gold_fraud_alerts\`
  WHERE DATE(detected_at) >= CURRENT_DATE() - 1
)
SELECT * FROM layer_counts
ORDER BY CASE layer
  WHEN 'bronze' THEN 1
  WHEN 'silver' THEN 2
  WHEN 'gold' THEN 3
END;
EOF

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ TEST COMPLETE!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📌 Expected Results:"
echo "   • Bronze layer:   ~2,000 events (WMS + ERP)"
echo "   • Silver layer:   ~1,000 invoices (after dedup)"
echo "   • Gold layer:     ~20-25 fraud alerts"
echo ""
echo "   • VELOCITY alerts:  ~12 (HIGH)"
echo "   • ANOMALY alerts:   ~9 (CRITICAL + HIGH)"
echo "   • FALLBACK alerts:  ~2 (MEDIUM)"
echo ""
echo "📁 Next Steps:"
echo "   1. Review full alerts in BigQuery Console"
echo "   2. Run queries from gcp/validation_queries.sql"
echo "   3. Export results for portfolio/interview demos"
echo ""
echo "   BigQuery Console:"
echo "   https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&p=${PROJECT_ID}&d=${DATASET}&page=dataset"
echo ""
echo "════════════════════════════════════════════════════════════════════════"
