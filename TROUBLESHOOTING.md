# Dataflow Flex Template Troubleshooting Guide

## 🚨 ERROR: "Template launch failed. See console logs."

This is a **generic error** from the Flex Template launcher. The real error is in the console logs.

---

## ✅ THE `/template/` PATH IS CORRECT!

If you see errors like:
```
File "/template/pipeline/gcp_batch_main.py", line 28
ModuleNotFoundError: No module named 'pipeline'
```

**The `/template/` path is CORRECT!** This is how Dataflow Flex Templates work:

```
Dockerfile.flex copies files to:
  /template/pipeline/gcp_main.py       ← Flex Template entrypoint
  /template/pipeline/gcp_batch_main.py
  /template/pipeline/gcp_stream_main.py
  /template/pipeline/schemas.py
  /template/pipeline/transforms.py
  /template/setup.py
  /template/requirements-flex.txt
```

The Dataflow launcher runs:
```bash
python /template/pipeline/gcp_main.py --mode=batch ...
```

This is the **standard Flex Template structure**. DO NOT change it!

---

## 🔍 HOW TO GET THE ACTUAL ERROR LOGS

### **Method 1: Use the Debug Script** (Easiest)

```bash
cd ~/supply-chain-fraud-detection
./debug_latest_job.sh
```

This shows:
- Job details
- All ERROR logs
- Last 100 INFO/WARNING logs (shows execution flow)

### **Method 2: Manual `gcloud` Commands**

```bash
# Get latest job ID
JOB_ID=$(gcloud dataflow jobs list --region=us-central1 --limit=1 --format="value(JOB_ID)")

# Get error logs
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=${JOB_ID} AND severity>=ERROR" \
  --limit=50 \
  --freshness=1h

# Get all logs (shows full execution)
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=${JOB_ID}" \
  --limit=100 \
  --freshness=1h
```

### **Method 3: Cloud Console**

1. Go to: https://console.cloud.google.com/dataflow/jobs
2. Click on your failed job
3. Click "LOGS" tab
4. Look for ERROR or WARNING severity

---

## 🐛 COMMON ERRORS & FIXES

### **Error 1: `ModuleNotFoundError: No module named 'pipeline'`**

**Error:**
```
File "/template/pipeline/gcp_batch_main.py", line 28
from pipeline.transforms import ...
ModuleNotFoundError: No module named 'pipeline'
```

**Root Cause:**
When Dataflow runs the script directly (not via `python -m`), Python doesn't know where to find the `pipeline` package.

**Fix:**
Add this to the TOP of `gcp_main.py`, `gcp_batch_main.py`, and `gcp_stream_main.py`:

```python
import os
import sys

# Bootstrap sys.path for Flex Template execution
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

**Then:**
```bash
# Commit the fix
git add pipeline/gcp_main.py pipeline/gcp_batch_main.py pipeline/gcp_stream_main.py
git commit -m "fix: add sys.path bootstrap for Flex Template execution"
git push origin main

# Rebuild image (CRITICAL!)
cd ~/supply-chain-fraud-detection/gcp
./build_flex_template.sh

# Retry job
SA_EMAIL=raud-detection-sa@fraud-detect-260526-1750.iam.gserviceaccount.com \
  ./run_flex_template.sh
```

---

### **Error 2: `No module named 'apache_beam'` or other dependencies**

**Error:**
```
ModuleNotFoundError: No module named 'apache_beam'
```

**Root Cause:**
Dependencies not installed in the Docker image.

**Fix:**
Check that `requirements-flex.txt` contains all dependencies:

```bash
cat requirements-flex.txt
```

Should include:
```
apache-beam[gcp]==2.73.0
google-cloud-bigquery==3.28.0
google-cloud-storage==2.18.2
google-cloud-pubsub==2.27.1
```

Then rebuild:
```bash
cd ~/supply-chain-fraud-detection/gcp
./build_flex_template.sh
```

---

### **Error 3: `FileNotFoundError: gs://bucket/file.jsonl`**

**Error:**
```
FileNotFoundError: gs://fraud_detection_pipeline_bucket/wms_receiving.jsonl
```

**Root Cause:**
Input files don't exist in GCS.

**Fix:**
Upload test data:
```bash
cd ~/supply-chain-fraud-detection/gcp
./publish_test_messages.sh  # For streaming (Pub/Sub)

# OR for batch mode, copy local data to GCS:
gsutil cp ../data/wms_receiving.jsonl gs://fraud_detection_pipeline_bucket/
gsutil cp ../data/erp_invoices.jsonl gs://fraud_detection_pipeline_bucket/
```

---

### **Error 4: Permission denied on GCS/BigQuery**

**Error:**
```
403 Forbidden: Service account does not have permission
```

**Root Cause:**
Service account lacks IAM roles.

**Fix:**
Grant required roles:
```bash
PROJECT_ID=fraud-detect-260526-1750
SA_EMAIL=raud-detection-sa@${PROJECT_ID}.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"
```

---

### **Error 5: `Template not found: gs://bucket/templates/fraud-detection.json`**

**Error:**
```
404 Not Found: Template file does not exist
```

**Root Cause:**
Template not built/published yet.

**Fix:**
Build and publish the template:
```bash
cd ~/supply-chain-fraud-detection/gcp
./build_flex_template.sh
```

---

## 🔄 COMPLETE FIX WORKFLOW

If you're getting "Template launch failed", follow this workflow:

### **1. Get the Actual Error**
```bash
cd ~/supply-chain-fraud-detection
./debug_latest_job.sh
```

Look for lines like:
```
ModuleNotFoundError: No module named 'pipeline'
FileNotFoundError: gs://...
403 Forbidden: ...
```

### **2. Apply the sys.path Fix (if ModuleNotFoundError)**
```bash
# Already applied in latest code!
# Just pull, commit, and rebuild:
git pull origin main
git status  # Should show gcp_main.py, gcp_batch_main.py, gcp_stream_main.py modified
git add pipeline/
git commit -m "fix: add sys.path bootstrap"
git push origin main
```

### **3. Rebuild the Docker Image** (CRITICAL!)
```bash
cd ~/supply-chain-fraud-detection/gcp
./build_flex_template.sh
```

**⏱️ Takes 3-5 minutes**

This:
- Builds new Docker image with latest code
- Pushes to Artifact Registry
- Creates new template metadata JSON in GCS

### **4. Verify Template Published**
```bash
gsutil cat gs://temp_staging_fraud_detection/templates/fraud-detection.json
```

Should show updated timestamp.

### **5. Run the Job Again**
```bash
cd ~/supply-chain-fraud-detection/gcp

# Batch mode
SA_EMAIL=raud-detection-sa@fraud-detect-260526-1750.iam.gserviceaccount.com \
  ./run_flex_template.sh

# OR streaming mode
./run_stream_template.sh
```

### **6. Monitor Job Progress**
```bash
# Watch job in console
gcloud dataflow jobs list --region=us-central1 --filter='STATE=Running'

# Tail logs
gcloud logging tail "resource.type=dataflow_step" --format=json
```

---

## ✅ VALIDATION CHECKLIST

After deploying, verify:

- [ ] Docker image built successfully
- [ ] Template JSON exists in GCS
- [ ] Job launches without "Template launch failed" error
- [ ] Workers start (check Dataflow console)
- [ ] Data flows through pipeline (check BigQuery)
- [ ] No errors in Cloud Logging

**BigQuery Validation:**
```bash
bq query --use_legacy_sql=false "
  SELECT COUNT(*) AS total_events 
  FROM \`fraud-detect-260526-1750.fraud_detection.bronze_events\`
"

bq query --use_legacy_sql=false "
  SELECT rule_name, COUNT(*) AS alerts
  FROM \`fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts\`
  GROUP BY rule_name
"
```

---

## 📊 UNDERSTANDING FLEX TEMPLATE STRUCTURE

```
┌─────────────────────────────────────────────────────────────┐
│ GCS Template JSON                                           │
│ gs://temp_staging_fraud_detection/templates/               │
│   fraud-detection.json                                      │
│                                                             │
│ Points to:                                                  │
│   image: us-central1-docker.pkg.dev/.../fraud-detection:*  │
│   metadata: parameter definitions from metadata.json       │
└─────────────────────────────────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Docker Image (Artifact Registry)                            │
│                                                             │
│ /template/                                                  │
│   ├── pipeline/                                             │
│   │   ├── gcp_main.py        ← ENTRYPOINT                  │
│   │   ├── gcp_batch_main.py  ← Batch logic                 │
│   │   ├── gcp_stream_main.py ← Streaming logic             │
│   │   ├── schemas.py                                       │
│   │   └── transforms.py                                    │
│   ├── setup.py                                             │
│   └── requirements-flex.txt                                │
│                                                             │
│ ENV:                                                        │
│   FLEX_TEMPLATE_PYTHON_PY_FILE=/template/pipeline/gcp_main.py │
└─────────────────────────────────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Dataflow Job Launch                                         │
│                                                             │
│ gcloud dataflow flex-template run JOB_NAME \                │
│   --template-file-gcs-location=gs://.../fraud-detection.json\│
│   --parameters="mode=batch,wms_input=gs://..."             │
│                                                             │
│ Launcher executes:                                          │
│   python /template/pipeline/gcp_main.py \                   │
│     --mode=batch \                                          │
│     --wms_input=gs://... \                                  │
│     --erp_input=gs://... \                                  │
│     --bq_dataset=... \                                      │
│     --runner=DataflowRunner                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🆘 STILL FAILING?

### **Share These Logs:**

1. **Full error logs:**
   ```bash
   ./debug_latest_job.sh > error_logs.txt
   ```

2. **Template metadata:**
   ```bash
   gsutil cat gs://temp_staging_fraud_detection/templates/fraud-detection.json > template.json
   ```

3. **Docker image details:**
   ```bash
   gcloud artifacts docker images describe \
     us-central1-docker.pkg.dev/fraud-detect-260526-1750/fraud-detection-repo/fraud-detection:latest
   ```

4. **Last 5 commits:**
   ```bash
   git log --oneline -5
   ```

---

## 📝 SUMMARY

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| "Template launch failed" | Generic error, check logs | Run `./debug_latest_job.sh` |
| `ModuleNotFoundError: No module named 'pipeline'` | sys.path not bootstrapped | Add sys.path.insert() to gcp_*.py files |
| `/template/` path errors | This is CORRECT path structure | Do NOT change! |
| Dependencies missing | Not in requirements-flex.txt | Add deps, rebuild image |
| Files not found in GCS | Not uploaded yet | Upload test data |
| Permission denied | Service account lacks roles | Grant IAM roles |
| Template not found | Not built yet | Run `./build_flex_template.sh` |

**CRITICAL:** After ANY code changes, you MUST rebuild the Docker image:
```bash
cd ~/supply-chain-fraud-detection/gcp
./build_flex_template.sh
```

Otherwise, Dataflow will keep using the old code!
