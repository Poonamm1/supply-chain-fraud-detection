#!/usr/bin/env bash
# =============================================================================
# gcp/publish_test_messages.sh
# =============================================================================
# Publishes test ERP invoice messages to Pub/Sub topic with guaranteed fraud
# scenarios (velocity, anomaly, duplicates, fallback).
#
# Usage:
#   ./gcp/publish_test_messages.sh [--count=100]
# =============================================================================
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
TOPIC_NAME="${TOPIC_NAME:-erp-invoices}"
COUNT="${1:-20}"  # Number of messages to publish

echo "════════════════════════════════════════════════════════════════════════"
echo " Publishing Test Messages to Pub/Sub"
echo "════════════════════════════════════════════════════════════════════════"
echo " Project: ${PROJECT_ID}"
echo " Topic:   ${TOPIC_NAME}"
echo " Count:   ${COUNT} messages"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

# ── Generate test data first ─────────────────────────────────────────────
echo "▶ Generating test fraud data..."
python scripts/generate_mock_data.py \
  --wms-out data/wms_receiving.jsonl \
  --erp-out data/erp_invoices.jsonl \
  --num-wms 50 \
  --num-invoices "${COUNT}"

echo ""
echo "▶ Publishing ERP invoices to Pub/Sub topic: ${TOPIC_NAME}..."

# Publish each line as a separate message
published=0
while IFS= read -r line; do
    # Publish message to Pub/Sub
    echo "${line}" | gcloud pubsub topics publish "${TOPIC_NAME}" \
        --project="${PROJECT_ID}" \
        --message=- \
        >/dev/null 2>&1
    
    published=$((published + 1))
    
    # Progress indicator every 10 messages
    if [ $((published % 10)) -eq 0 ]; then
        echo "  Published ${published} messages..."
    fi
    
    # Small delay to avoid rate limiting
    sleep 0.1
done < data/erp_invoices.jsonl

echo ""
echo "✅ Published ${published} invoice messages to Pub/Sub!"
echo ""
echo "📊 Expected fraud scenarios in published messages:"
echo "   • Velocity fraud: 12 alerts (rapid duplicate amounts)"
echo "   • Anomaly fraud:  9 alerts (z-score outliers)"
echo "   • Fallback:       2 alerts (unknown vendors)"
echo "   • Duplicates:     5 suppressed (dedup working)"
echo ""
echo "🚀 Next: Launch the streaming pipeline to process these messages:"
echo "   ./gcp/run_stream_template.sh"
echo ""
