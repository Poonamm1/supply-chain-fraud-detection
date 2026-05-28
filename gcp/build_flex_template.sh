#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-fraud-detection-pipeline-repo}"
IMAGE_NAME="${IMAGE_NAME:-fraud-detection-flex}"
TEMPLATE_BUCKET="${TEMPLATE_BUCKET:-temp_staging_fraud_detection}"
TEMPLATE_GCS_PATH="${TEMPLATE_GCS_PATH:-gs://${TEMPLATE_BUCKET}/templates/fraud-detection.json}"
METADATA_FILE="${METADATA_FILE:-gcp/metadata.json}"

IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d-%H%M%S)}"
AR_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}"
AR_TAG="${AR_BASE}:${IMAGE_TAG}"
AR_LATEST="${AR_BASE}:latest"

echo "Flex Template Build & Publish"
echo "Project       : ${PROJECT_ID}"
echo "Region        : ${REGION}"
echo "Artifact Repo : ${REPO}"
echo "Image (tag)   : ${AR_TAG}"
echo "Template spec  : ${TEMPLATE_GCS_PATH}"

command -v gcloud >/dev/null || { echo "gcloud not installed"; exit 1; }
[[ -f "${METADATA_FILE}" ]] || { echo "Missing ${METADATA_FILE}"; exit 1; }
[[ -f "Dockerfile.flex" ]] || { echo "Missing Dockerfile.flex"; exit 1; }
[[ -f "gcp/cloudbuild.yaml" ]] || { echo "Missing gcp/cloudbuild.yaml"; exit 1; }

gcloud builds submit \
  --project="${PROJECT_ID}" \
  --config="gcp/cloudbuild.yaml" \
  --substitutions="_AR_TAG=${AR_TAG},_AR_LATEST=${AR_LATEST}"

gcloud dataflow flex-template build "${TEMPLATE_GCS_PATH}" \
  --image "${AR_LATEST}" \
  --sdk-language PYTHON \
  --metadata-file "${METADATA_FILE}" \
  --project "${PROJECT_ID}"

echo "Flex Template published: ${TEMPLATE_GCS_PATH}"
echo "Image: ${AR_LATEST}"