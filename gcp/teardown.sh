#!/usr/bin/env bash
# gcp/teardown.sh — NUCLEAR OPTION. Kill everything to stop ALL billing.
#
# Run this when:
#   * You're done experimenting and want zero ongoing cost
#   * You're going on vacation and don't want surprise bills
#   * Something looks weird in the billing dashboard
#
# This does NOT delete the project itself (so you keep your project ID).
# To fully delete the project: gcloud projects delete PROJECT_ID

set -euo pipefail
: "${PROJECT_ID:?PROJECT_ID required}"
REGION="${REGION:-us-central1}"
BUCKET="${BUCKET:-${PROJECT_ID}-fraud-pipeline}"
DATASET="${DATASET:-fraud_detection}"

read -r -p "⚠  Delete bucket gs://${BUCKET} and dataset ${DATASET}? [yes/N] " ans
[[ "${ans}" == "yes" ]] || { echo "Aborted."; exit 1; }

echo "▶ Cancelling any running Dataflow jobs..."
gcloud dataflow jobs list --region="${REGION}" --status=active \
    --format="value(JOB_ID)" 2>/dev/null | \
    xargs -I{} gcloud dataflow jobs cancel {} --region="${REGION}" -q 2>/dev/null || true

echo "▶ Deleting BigQuery dataset (and all tables)..."
bq rm -r -f -d "${PROJECT_ID}:${DATASET}" || true

echo "▶ Deleting GCS bucket..."
gsutil -m rm -r "gs://${BUCKET}" 2>/dev/null || true

echo "✅ Teardown done. Recurring cost = \$0."
echo "   (Project, service account, and budget alert preserved.)"
