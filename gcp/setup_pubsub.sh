#!/usr/bin/env bash
# =============================================================================
# gcp/setup_pubsub.sh
# =============================================================================
# Creates Pub/Sub topics and subscriptions for streaming fraud detection.
# Supports both WMS and ERP event streams + dead-letter queue.
#
# Usage:
#   ./gcp/setup_pubsub.sh
# =============================================================================
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
WMS_TOPIC="${WMS_TOPIC:-wms-events-topic}"
ERP_TOPIC="${ERP_TOPIC:-erp-events-topic}"
DLQ_TOPIC="${DLQ_TOPIC:-fraud-dead-letter-topic}"
WMS_SUBSCRIPTION="${WMS_SUBSCRIPTION:-wms-events-sub}"
ERP_SUBSCRIPTION="${ERP_SUBSCRIPTION:-erp-events-sub}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-raud-detection-sa@${PROJECT_ID}.iam.gserviceaccount.com}"

echo "═══════════════════════════════════════════════════════════════════════"
echo " Setting up Pub/Sub for Streaming Pipeline"
echo "═══════════════════════════════════════════════════════════════════════"
echo " Project:         ${PROJECT_ID}"
echo " WMS Topic:       ${WMS_TOPIC}"
echo " ERP Topic:       ${ERP_TOPIC}"
echo " DLQ Topic:       ${DLQ_TOPIC}"
echo " WMS Subscription: ${WMS_SUBSCRIPTION}"
echo " ERP Subscription: ${ERP_SUBSCRIPTION}"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""

# ── 1. Create Topics ─────────────────────────────────────────────────────
echo "▶ Creating Pub/Sub topics..."

# Dead-letter topic (must exist first for DLQ configuration)
if gcloud pubsub topics describe "${DLQ_TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ DLQ topic already exists: ${DLQ_TOPIC}"
else
    gcloud pubsub topics create "${DLQ_TOPIC}" \
        --project="${PROJECT_ID}" \
        --message-retention-duration=7d
    echo "✅ Created DLQ topic: ${DLQ_TOPIC}"
fi

# WMS events topic
if gcloud pubsub topics describe "${WMS_TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ WMS topic already exists: ${WMS_TOPIC}"
else
    gcloud pubsub topics create "${WMS_TOPIC}" \
        --project="${PROJECT_ID}" \
        --message-retention-duration=1d
    echo "✅ Created WMS topic: ${WMS_TOPIC}"
fi

# ERP events topic
if gcloud pubsub topics describe "${ERP_TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ ERP topic already exists: ${ERP_TOPIC}"
else
    gcloud pubsub topics create "${ERP_TOPIC}" \
        --project="${PROJECT_ID}" \
        --message-retention-duration=1d
    echo "✅ Created ERP topic: ${ERP_TOPIC}"
fi

# ── 2. Create Subscriptions with DLQ ────────────────────────────────────
echo ""
echo "▶ Creating Pub/Sub subscriptions with dead-letter queue..."

# WMS subscription
if gcloud pubsub subscriptions describe "${WMS_SUBSCRIPTION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ WMS subscription already exists: ${WMS_SUBSCRIPTION}"
else
    gcloud pubsub subscriptions create "${WMS_SUBSCRIPTION}" \
        --topic="${WMS_TOPIC}" \
        --project="${PROJECT_ID}" \
        --ack-deadline=60 \
        --message-retention-duration=1d \
        --dead-letter-topic="projects/${PROJECT_ID}/topics/${DLQ_TOPIC}" \
        --max-delivery-attempts=5
    echo "✅ Created WMS subscription: ${WMS_SUBSCRIPTION}"
fi

# ERP subscription
if gcloud pubsub subscriptions describe "${ERP_SUBSCRIPTION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ ERP subscription already exists: ${ERP_SUBSCRIPTION}"
else
    gcloud pubsub subscriptions create "${ERP_SUBSCRIPTION}" \
        --topic="${ERP_TOPIC}" \
        --project="${PROJECT_ID}" \
        --ack-deadline=60 \
        --message-retention-duration=1d \
        --dead-letter-topic="projects/${PROJECT_ID}/topics/${DLQ_TOPIC}" \
        --max-delivery-attempts=5
    echo "✅ Created ERP subscription: ${ERP_SUBSCRIPTION}"
fi

# ── 3. Grant Permissions ─────────────────────────────────────────────────────────
echo ""
echo "▶ Granting Pub/Sub permissions to service account..."

# Grant Pub/Sub Subscriber role
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/pubsub.subscriber" \
    --condition=None \
    >/dev/null 2>&1 || true

# Grant Pub/Sub Viewer role (for listing subscriptions)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/pubsub.viewer" \
    --condition=None \
    >/dev/null 2>&1 || true

# Grant Pub/Sub Publisher role (for DLQ topic)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/pubsub.publisher" \
    --condition=None \
    >/dev/null 2>&1 || true

echo "✅ Granted Pub/Sub permissions to ${SERVICE_ACCOUNT}"

# ── 4. Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════════════════"
echo "✅ Pub/Sub Setup Complete!"
echo "═══════════════════════════════════════════════════════════════════════════"
echo ""
echo "📌 WMS Topic:        projects/${PROJECT_ID}/topics/${WMS_TOPIC}"
echo "📌 WMS Subscription: projects/${PROJECT_ID}/subscriptions/${WMS_SUBSCRIPTION}"
echo ""
echo "📌 ERP Topic:        projects/${PROJECT_ID}/topics/${ERP_TOPIC}"
echo "📌 ERP Subscription: projects/${PROJECT_ID}/subscriptions/${ERP_SUBSCRIPTION}"
echo ""
echo "📌 DLQ Topic:        projects/${PROJECT_ID}/topics/${DLQ_TOPIC}"
echo "   (Max delivery attempts: 5, then message goes to DLQ)"
echo ""
echo "🚀 Next Steps:"
echo "   1. Publish test messages:"
echo "      ./gcp/publish_test_messages.sh"
echo ""
echo "   2. Launch streaming pipeline:"
echo "      ./gcp/run_stream_template.sh"
echo ""
echo "   3. Stop streaming job when done:"
echo "      gcloud dataflow jobs list --region=us-central1 --filter='STATE=Running'"
echo "      gcloud dataflow jobs cancel <JOB_ID> --region=us-central1"
echo ""
