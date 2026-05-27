#!/usr/bin/env bash
# =============================================================================
# gcp/build_flex_template.sh
# =============================================================================
# Builds the Flex Template image and publishes the template spec to GCS.
#
# Two modes:
#   USE_CLOUD_BUILD=true   (default) → runs on Google Cloud Build
#                          ✅ no VPN/network issues
#                          ✅ no local Docker push needed
#                          ✅ auto-pushes to Artifact Registry
#                          ⏱  ~5-8 min total
#
#   USE_CLOUD_BUILD=false  → builds locally with `docker build` then pushes
#                          Requires public-internet egress (pypi.org reachable)
#
# Idempotent: safe to re-run.
#
# Usage:
#   ./gcp/build_flex_template.sh                          # Cloud Build (default)
#   IMAGE_TAG=v2 ./gcp/build_flex_template.sh             # custom tag
#   USE_CLOUD_BUILD=false ./gcp/build_flex_template.sh    # local docker build
# =============================================================================
set -euo pipefail

# ── Defaults (baked from your actual GCP setup) ──────────────────────────
PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-fraud-detection-pipeline-repo}"
IMAGE_NAME="${IMAGE_NAME:-fraud-detection-flex}"
TEMPLATE_BUCKET="${TEMPLATE_BUCKET:-temp_staging_fraud_detection}"
TEMPLATE_GCS_PATH="${TEMPLATE_GCS_PATH:-gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json}"
METADATA_FILE="${METADATA_FILE:-gcp/metadata.json}"
USE_CLOUD_BUILD="${USE_CLOUD_BUILD:-true}"

IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d-%H%M%S)}"
AR_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}"
AR_TAG="${AR_BASE}:${IMAGE_TAG}"
AR_LATEST="${AR_BASE}:latest"

echo "================================================================"
echo " Flex Template Build & Publish"
echo "================================================================"
echo " Project        : ${PROJECT_ID}"
echo " Region         : ${REGION}"
echo " AR repo        : ${REPO}"
echo " Image (tag)    : ${AR_TAG}"
echo " Image (latest) : ${AR_LATEST}"
echo " Template spec  : ${TEMPLATE_GCS_PATH}"
echo " Build mode     : $([[ ${USE_CLOUD_BUILD} == true ]] && echo 'Cloud Build' || echo 'Local Docker')"
echo "================================================================"

command -v gcloud >/dev/null || { echo "❌ gcloud not installed"; exit 1; }
[[ -f "${METADATA_FILE}" ]]  || { echo "❌ ${METADATA_FILE} missing";  exit 1; }
[[ -f "Dockerfile.flex" ]]   || { echo "❌ Dockerfile.flex missing";   exit 1; }

# ── 1. Ensure AR repo exists ─────────────────────────────────────────────
gcloud artifacts repositories describe "${REPO}" \
    --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
gcloud artifacts repositories create "${REPO}" \
    --repository-format=docker --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Fraud detection Dataflow Flex Template images"

# ── 2. Build & push the image ────────────────────────────────────────────
if [[ "${USE_CLOUD_BUILD}" == "true" ]]; then
    echo "▶ Submitting image build to Cloud Build (runs on Google infra)"
    # Cloud Build: builds linux/amd64 natively, no VPN issues, pushes to AR
    gcloud builds submit \
        --project="${PROJECT_ID}" \
        --tag "${AR_TAG}" \
        --machine-type=e2-highcpu-8 \
        --timeout=20m \
        --gcs-source-staging-dir="gs://${TEMPLATE_BUCKET}/cloud-build-source" \
        --config=- <<EOF
steps:
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-f', 'Dockerfile.flex', '-t', '${AR_TAG}', '-t', '${AR_LATEST}', '.']
images:
  - '${AR_TAG}'
  - '${AR_LATEST}'
options:
  machineType: E2_HIGHCPU_8
  logging: CLOUD_LOGGING_ONLY
timeout: 1200s
EOF
else
    echo "▶ Building image locally (requires pypi.org egress)"
    command -v docker >/dev/null || { echo "❌ docker not installed"; exit 1; }
    gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
    docker build \
        --platform=linux/amd64 \
        --file Dockerfile.flex \
        --tag "${AR_TAG}" \
        --tag "${AR_LATEST}" \
        .
    docker push "${AR_TAG}"
    docker push "${AR_LATEST}"
fi

# ── 3. Publish the Flex Template spec to GCS ─────────────────────────────
echo "▶ Publishing template spec to ${TEMPLATE_GCS_PATH}"
gcloud dataflow flex-template build "${TEMPLATE_GCS_PATH}" \
    --image "${AR_LATEST}" \
    --sdk-language "PYTHON" \
    --metadata-file "${METADATA_FILE}" \
    --project "${PROJECT_ID}"

echo ""
echo "✅ Flex Template published."
echo "   Image    : ${AR_TAG}"
echo "   Image    : ${AR_LATEST}"
echo "   Template : ${TEMPLATE_GCS_PATH}"
echo ""
echo "▶ Launch a job with:"
echo "   SA_EMAIL=<sa>@${PROJECT_ID}.iam.gserviceaccount.com ./gcp/run_flex_template.sh"
