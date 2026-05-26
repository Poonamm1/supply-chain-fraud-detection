#!/usr/bin/env bash
# gcp/build_and_push.sh — Build the pipeline image and push to Artifact Registry.
#
# Idempotent: creates the AR repo if missing.
# Tags with both :latest AND :<git-sha> for traceability.
#
# Usage:
#   PROJECT_ID=my-proj ./gcp/build_and_push.sh         # tag = git short-sha
#   PROJECT_ID=my-proj IMAGE_TAG=v1.0 ./gcp/build_and_push.sh
#
# Always builds for linux/amd64 (GCP workers don't run arm64 images even
# when you're on an M-series Mac).

set -euo pipefail

: "${PROJECT_ID:?PROJECT_ID env var required}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-fraud-pipeline-repo}"
IMAGE_NAME="${IMAGE_NAME:-fraud-detection-pipeline}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || echo "$(date +%Y%m%d-%H%M%S)")}"
FULL_IMAGE_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}"
FULL_IMAGE_TAG="${FULL_IMAGE_BASE}:${IMAGE_TAG}"
FULL_IMAGE_LATEST="${FULL_IMAGE_BASE}:latest"

echo "▶ Image: ${FULL_IMAGE_TAG}"

# ── Step 1: Ensure Artifact Registry repo exists ────────────────────────
echo "▶ Ensuring Artifact Registry repo: ${REPO}"
gcloud artifacts repositories describe "${REPO}" \
    --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
gcloud artifacts repositories create "${REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Fraud detection pipeline images"

# ── Step 2: Authenticate docker against AR ──────────────────────────────
echo "▶ Configuring docker to auth with ${REGION}-docker.pkg.dev"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ── Step 3: Build linux/amd64 image ─────────────────────────────────────
# --platform is critical: M-series Macs default to arm64 which GCP rejects.
echo "▶ Building linux/amd64 image (this takes ~3-5 min first time)..."
docker build \
    --platform=linux/amd64 \
    --tag "${FULL_IMAGE_TAG}" \
    --tag "${FULL_IMAGE_LATEST}" \
    --file Dockerfile \
    .

# ── Step 4: Push both tags ──────────────────────────────────────────────
echo "▶ Pushing :${IMAGE_TAG} + :latest"
docker push "${FULL_IMAGE_TAG}"
docker push "${FULL_IMAGE_LATEST}"

echo ""
echo "✅ Pushed:"
echo "   ${FULL_IMAGE_TAG}"
echo "   ${FULL_IMAGE_LATEST}"
echo ""
echo "Next steps:"
echo "  • Cloud Run job   :  gcloud run jobs deploy fraud-pipeline --image=${FULL_IMAGE_LATEST} --region=${REGION}"
echo "  • Cloud Composer  :  reference image in airflow/dags/fraud_pipeline_dag.py"
echo "  • Run locally     :  docker run --rm ${FULL_IMAGE_LATEST}"
