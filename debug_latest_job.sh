#!/usr/bin/env bash
# =============================================================================
# debug_latest_job.sh - Get detailed error logs from latest failed Dataflow job
# =============================================================================

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-fraud-detect-260526-1750}"
REGION="${REGION:-us-central1}"

echo "════════════════════════════════════════════════════════════════════════"
echo " Getting Latest Dataflow Job Logs"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

# Get the latest job
echo "📊 Fetching latest job..."
LATEST_JOB=$(gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --filter="createTime>=\"$(date -u -v-1H '+%Y-%m-%dT%H:%M:%S')\"" \
  --format="value(JOB_ID)" \
  --limit=1)

if [ -z "$LATEST_JOB" ]; then
    echo "❌ No recent jobs found in the last hour."
    echo ""
    echo "💡 Try listing all jobs:"
    echo "   gcloud dataflow jobs list --region=${REGION} --limit=5"
    exit 1
fi

echo "✅ Latest job: ${LATEST_JOB}"
echo ""

# Get job details
echo "📋 Job Details:"
gcloud dataflow jobs describe "${LATEST_JOB}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format="yaml(currentState,createTime,location,stageStates)"
echo ""

# Get error logs
echo "════════════════════════════════════════════════════════════════════════"
echo " ERROR LOGS (Last 50 lines)"
echo "════════════════════════════════════════════════════════════════════════"
gcloud logging read "resource.type=dataflow_step AND resource.labels.job_id=${LATEST_JOB} AND severity>=ERROR" \
  --project="${PROJECT_ID}" \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)" \
  --freshness=1h

echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo " INFO/WARNING LOGS (Last 100 lines - shows execution flow)"
echo "════════════════════════════════════════════════════════════════════════"
gcloud logging read "resource.type=dataflow_step AND resource.labels.job_id=${LATEST_JOB}" \
  --project="${PROJECT_ID}" \
  --limit=100 \
  --format="table(timestamp,severity,textPayload)" \
  --freshness=1h

echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "💡 To view full logs in Console:"
echo "   https://console.cloud.google.com/dataflow/jobs/${REGION}/${LATEST_JOB}?project=${PROJECT_ID}"
echo "════════════════════════════════════════════════════════════════════════"
