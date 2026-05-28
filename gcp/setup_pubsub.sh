#!/usr/bin/env bash
# =============================================================================
# gcp/setup_pubsub.sh
# =============================================================================
# Creates Pub/Sub topic and subscription for streaming fraud detection.
#
# Usage:
#   ./gcp/setup_pubsub.sh
# =============================================================================
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
TOPIC_NAME="${TOPIC_NAME:-erp-invoices}"
SUBSCRIPTION_NAME="${SUBSCRIPTION_NAME:-erp-invoices-sub}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-raud-detection-sa@${PROJECT_ID}.iam.gserviceaccount.com}"

echo "════════════════════════════════════════════════════════════════════════"
echo " Setting up Pub/Sub for Streaming Pipeline"
echo "════════════════════════════════════════════════════════════════════════"
echo " Project:      ${PROJECT_ID}"
echo " Topic:        ${TOPIC_NAME}"
echo " Subscription: ${SUBSCRIPTION_NAME}"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

# ── 1. Create Topic ──────────────────────────────────────────────────────
echo "▶ Creating Pub/Sub topic: ${TOPIC_NAME}..."
if gcloud pubsub topics describe "${TOPIC_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ Topic already exists: ${TOPIC_NAME}"
else
    gcloud pubsub topics create "${TOPIC_NAME}" \
        --project="${PROJECT_ID}" \
        --message-retention-duration=1d
    echo "✅ Created topic: ${TOPIC_NAME}"
fi

# ── 2. Create Subscription ───────────────────────────────────────────────
echo ""
echo "▶ Creating Pub/Sub subscription: ${SUBSCRIPTION_NAME}..."
if gcloud pubsub subscriptions describe "${SUBSCRIPTION_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "✅ Subscription already exists: ${SUBSCRIPTION_NAME}"
else
    gcloud pubsub subscriptions create "${SUBSCRIPTION_NAME}" \
        --topic="${TOPIC_NAME}" \
        --project="${PROJECT_ID}" \
        --ack-deadline=60 \
        --message-retention-duration=1d
    echo "✅ Created subscription: ${SUBSCRIPTION_NAME}"
fi

# ── 3. Grant Permissions ─────────────────────────────────────────────────
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

echo "✅ Granted Pub/Sub permissions to ${SERVICE_ACCOUNT}"

# ── 4. Summary ───────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "✅ Pub/Sub Setup Complete!"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
echo "📌 Topic:        projects/${PROJECT_ID}/topics/${TOPIC_NAME}"
echo "📌 Subscription: projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}"
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
