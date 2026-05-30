#!/bin/bash
# gcp/deploy_production.sh
# ═══════════════════════════════════════════════════════════════════════════
# Production Deployment Script - Supply Chain Fraud Detection
# ═══════════════════════════════════════════════════════════════════════════
# Purpose: End-to-end production deployment with proper table creation
# Usage: ./deploy_production.sh
# Prerequisites: gcloud authenticated, VPN connected
# ═══════════════════════════════════════════════════════════════════════════

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# ───────────────────────────────────────────────────────────────────────────
# Configuration
# ───────────────────────────────────────────────────────────────────────────

PROJECT_ID="fraud-detect-260526-1750"
DATASET="fraud_detection"
REGION="us-central1"
LOCATION="us-central1"

echo "════════════════════════════════════════════════════════════════════════════"
echo "🚀 PRODUCTION DEPLOYMENT - Supply Chain Fraud Detection"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Project: ${PROJECT_ID}"
echo "Dataset: ${DATASET}"
echo "Region: ${REGION}"
echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 1: Verify Prerequisites
# ───────────────────────────────────────────────────────────────────────────

echo "📋 Step 1: Verifying prerequisites..."

# Check if authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "❌ ERROR: Not authenticated with gcloud"
    echo "   Run: gcloud auth login"
    exit 1
fi
echo "   ✅ gcloud authenticated"

# Check if project exists
if ! gcloud projects describe "${PROJECT_ID}" &>/dev/null; then
    echo "❌ ERROR: Project ${PROJECT_ID} not found"
    exit 1
fi
echo "   ✅ Project ${PROJECT_ID} exists"

# Set active project
gcloud config set project "${PROJECT_ID}"
echo "   ✅ Active project set to ${PROJECT_ID}"

echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 2: Create BigQuery Dataset (if not exists)
# ───────────────────────────────────────────────────────────────────────────

echo "📦 Step 2: Creating BigQuery dataset..."

if bq ls -d "${PROJECT_ID}:${DATASET}" &>/dev/null; then
    echo "   ℹ️  Dataset ${DATASET} already exists (skipping)"
else
    bq mk \
        --location="${LOCATION}" \
        --description="Fraud detection medallion architecture with ML feature store" \
        "${PROJECT_ID}:${DATASET}"
    echo "   ✅ Dataset ${DATASET} created"
fi

echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 3: Create BigQuery Tables (Infrastructure as Code)
# ───────────────────────────────────────────────────────────────────────────

echo "🗄️  Step 3: Creating BigQuery tables from bq_schema.sql..."

# Export variables for SQL substitution
export BQ_PROJECT="${PROJECT_ID}"
export BQ_DATASET="${DATASET}"
export BQ_LOCATION="${LOCATION}"

# Substitute variables in SQL and execute
# Note: BigQuery doesn't support variable substitution natively,
# so we use envsubst or sed

if command -v envsubst &>/dev/null; then
    # Use envsubst if available (preferred)
    envsubst < bq_schema.sql | bq query --use_legacy_sql=false --project_id="${PROJECT_ID}"
else
    # Fallback: use sed for substitution
    sed -e "s/\${BQ_PROJECT}/${BQ_PROJECT}/g" \
        -e "s/\${BQ_DATASET}/${BQ_DATASET}/g" \
        -e "s/\${BQ_LOCATION}/${BQ_LOCATION}/g" \
        bq_schema.sql | bq query --use_legacy_sql=false --project_id="${PROJECT_ID}"
fi

echo "   ✅ BigQuery tables created:"
echo "      - bronze_raw_events (partitioned, clustered)"
echo "      - silver_deduplicated_invoices (partitioned, clustered)"
echo "      - gold_fraud_alerts (partitioned, clustered)"
echo "      - vendor_daily_behavioral_features (partitioned, clustered)"
echo "      - vendor_daily_risk_features (partitioned, clustered)"
echo "      - vendor_daily_features (VIEW)"
echo "      - vendor_90day_baseline (dimension table)"

echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 4: Verify Tables Created Successfully
# ───────────────────────────────────────────────────────────────────────────

echo "🔍 Step 4: Verifying table creation..."

REQUIRED_TABLES=(
    "bronze_raw_events"
    "silver_deduplicated_invoices"
    "gold_fraud_alerts"
    "vendor_daily_behavioral_features"
    "vendor_daily_risk_features"
    "vendor_90day_baseline"
)

ALL_EXIST=true
for table in "${REQUIRED_TABLES[@]}"; do
    if bq show "${PROJECT_ID}:${DATASET}.${table}" &>/dev/null; then
        echo "   ✅ ${table}"
    else
        echo "   ❌ ${table} MISSING"
        ALL_EXIST=false
    fi
done

# Check VIEW separately
if bq show --view "${PROJECT_ID}:${DATASET}.vendor_daily_features" &>/dev/null; then
    echo "   ✅ vendor_daily_features (VIEW)"
else
    echo "   ❌ vendor_daily_features (VIEW) MISSING"
    ALL_EXIST=false
fi

if [ "$ALL_EXIST" = false ]; then
    echo ""
    echo "❌ ERROR: Some tables failed to create"
    echo "   Check bq_schema.sql for syntax errors"
    exit 1
fi

echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 5: Build Flex Template
# ───────────────────────────────────────────────────────────────────────────

echo "🐋 Step 5: Building Flex Template..."

./build_flex_template.sh

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Flex Template build failed"
    exit 1
fi

echo "   ✅ Flex Template built successfully"

echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 6: Schedule Rolling Features Query
# ───────────────────────────────────────────────────────────────────────────

echo "⏰ Step 6: Scheduling rolling features query..."

# Check if scheduled query already exists
SCHEDULED_QUERY_NAME="compute-rolling-features"
if bq ls --transfer_config --project_id="${PROJECT_ID}" | grep -q "${SCHEDULED_QUERY_NAME}"; then
    echo "   ℹ️  Scheduled query '${SCHEDULED_QUERY_NAME}' already exists"
    echo "      To update: bq update transfer_config <config_id>"
else
    # Create scheduled query
    bq query \
        --use_legacy_sql=false \
        --schedule='every day 02:00' \
        --display_name="${SCHEDULED_QUERY_NAME}" \
        --project_id="${PROJECT_ID}" \
        --target_dataset="${DATASET}" \
        < compute_rolling_features.sql
    
    echo "   ✅ Scheduled query created (runs daily at 2 AM)"
fi

echo ""

# ───────────────────────────────────────────────────────────────────────────
# Step 7: Deployment Summary
# ───────────────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════════════════════"
echo "✅ PRODUCTION DEPLOYMENT COMPLETE"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📊 BigQuery Tables:"
echo "   https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&d=${DATASET}"
echo ""
echo "🐋 Flex Template:"
echo "   Image: gcr.io/${PROJECT_ID}/fraud-detection-flex:latest"
echo "   Metadata: gs://temp_staging_fraud_detection/fraud-detection-flex-template.json"
echo ""
echo "⏰ Scheduled Query:"
echo "   Name: ${SCHEDULED_QUERY_NAME}"
echo "   Schedule: Every day at 2:00 AM"
echo "   Purpose: Compute rolling window features (7d, 30d)"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "🚀 NEXT STEPS"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "BATCH MODE (one-time processing):"
echo "  ./run_flex_template.sh"
echo ""
echo "STREAMING MODE (continuous processing):"
echo "  ./run_stream_template.sh"
echo ""
echo "MONITORING:"
echo "  gcloud dataflow jobs list --region=${REGION}"
echo "  https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}"
echo ""
echo "VALIDATION:"
echo "  Run validation queries after first job completes"
echo "  See: compute_rolling_features.sql (bottom section)"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
