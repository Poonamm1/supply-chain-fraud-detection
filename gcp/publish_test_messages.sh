#!/usr/bin/env bash
# =============================================================================
# gcp/publish_test_messages.sh
# =============================================================================
# Publishes test WMS and ERP messages to Pub/Sub topics with guaranteed fraud
# scenarios (velocity, anomaly, duplicates, fallback).
#
# Usage:
#   ./gcp/publish_test_messages.sh [--count=100]
# =============================================================================
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
WMS_TOPIC="${WMS_TOPIC:-wms-events-topic}"
ERP_TOPIC="${ERP_TOPIC:-erp-events-topic}"
COUNT="${1:-20}"  # Number of messages to publish

echo "════════════════════════════════════════════════════════════════════════"
echo " Publishing Test Messages to Pub/Sub"
echo "════════════════════════════════════════════════════════════════════════"
echo " Project:   ${PROJECT_ID}"
echo " WMS Topic: ${WMS_TOPIC}"
echo " ERP Topic: ${ERP_TOPIC}"
echo " Count:     ${COUNT} messages each"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

# ── Generate test data first ─────────────────────────────────────────────────────
echo "▶ Generating test fraud data..."
python scripts/generate_mock_data.py \
  --wms-out data/wms_receiving.jsonl \
  --erp-out data/erp_invoices.jsonl \
  --num-wms "${COUNT}" \
  --num-invoices "${COUNT}"

echo ""
echo "▶ Publishing WMS messages to Pub/Sub topic: ${WMS_TOPIC}..."

# Publish WMS messages
published_wms=0
while IFS= read -r line; do
    # Publish message to Pub/Sub
    echo "${line}" | gcloud pubsub topics publish "${WMS_TOPIC}" \
        --project="${PROJECT_ID}" \
        --message=- \
        >/dev/null 2>&1
    
    published_wms=$((published_wms + 1))
    
    # Progress indicator every 10 messages
    if [ $((published_wms % 10)) -eq 0 ]; then
        echo "  Published ${published_wms} WMS messages..."
    fi
    
    # Small delay to avoid rate limiting
    sleep 0.1
done < data/wms_receiving.jsonl

echo ""
echo "▶ Publishing ERP invoices to Pub/Sub topic: ${ERP_TOPIC}..."

# Publish ERP messages
published_erp=0
while IFS= read -r line; do
    # Publish message to Pub/Sub
    echo "${line}" | gcloud pubsub topics publish "${ERP_TOPIC}" \
        --project="${PROJECT_ID}" \
        --message=- \
        >/dev/null 2>&1
    
    published_erp=$((published_erp + 1))
    
    # Progress indicator every 10 messages
    if [ $((published_erp % 10)) -eq 0 ]; then
        echo "  Published ${published_erp} ERP messages..."
    fi
    
    # Small delay to avoid rate limiting
    sleep 0.1
done < data/erp_invoices.jsonl

echo ""
echo "✅ Published ${published_wms} WMS messages to ${WMS_TOPIC}"
echo "✅ Published ${published_erp} ERP invoice messages to ${ERP_TOPIC}!"
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
