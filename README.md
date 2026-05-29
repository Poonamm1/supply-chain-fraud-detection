# Supply Chain Fraud Detection Pipeline

**Production-grade Apache Beam pipeline** for detecting fraud in supply chain ERP/WMS data streams.

Supports both **batch** (GCS) and **streaming** (Pub/Sub) modes on **Google Cloud Dataflow**.

---

## 🏗️ Architecture

```
Batch Mode:
  GCS (JSONL files) → Dataflow → BigQuery (bronze/silver/gold)

Streaming Mode:
  Pub/Sub → Dataflow → BigQuery (bronze/silver/gold)
```

### **Fraud Detection Rules**

| Rule | Mechanism |
|------|----------|
| `VELOCITY` | 10-min sliding window grouped by vendor_id; ≥3 identical amounts |
| `ANOMALY` | z-score vs vendor 90-day baseline (3σ threshold) |
| `FALLBACK` | Triggered when baseline lookup fails or vendor unknown |

### **Data Layers**

- **Bronze**: Raw events with ingestion timestamp
- **Silver**: Deduplicated invoices with event timestamp
- **Gold**: Fraud alerts with detection timestamp and evidence JSON

---

## 📁 Project Structure

```
.
├── pipeline/
│   ├── gcp_main.py           ← Unified Flex Template entrypoint
│   ├── gcp_batch_main.py     ← Batch pipeline (GCS → BigQuery)
│   ├── gcp_stream_main.py    ← Streaming pipeline (Pub/Sub → BigQuery)
│   ├── schemas.py            ← Dataclass models for events
│   └── transforms.py         ← Fraud detection DoFns and PTransforms
├── gcp/
│   ├── build_flex_template.sh    ← Build and publish Flex Template
│   ├── run_flex_template.sh      ← Launch batch job
│   ├── run_stream_template.sh    ← Launch streaming job
│   ├── setup_pubsub.sh           ← Create Pub/Sub topics/subscriptions
│   ├── bq_schema.sql             ← BigQuery table DDL
│   └── scripts/generate_mock_data.py ← Test data generator
├── Dockerfile.flex           ← Production Flex Template image
├── setup.py                  ← Python package setup
├── requirements-flex.txt     ← Production dependencies
└── README.md                 ← This file
```

---

## 🚀 Quick Start (GCP Deployment)

### **Prerequisites**

- Google Cloud Project with billing enabled
- Dataflow API, BigQuery API, Pub/Sub API, Artifact Registry API enabled
- Service account with required IAM roles:
  - `roles/dataflow.worker`
  - `roles/storage.objectAdmin`
  - `roles/bigquery.dataEditor`
  - `roles/pubsub.editor`

### **1. Clone and Setup**

```bash
git clone https://github.com/Poonamm1/supply-chain-fraud-detection.git
cd supply-chain-fraud-detection
```

### **2. Configure Environment**

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export SA_EMAIL="your-service-account@${PROJECT_ID}.iam.gserviceaccount.com"
```

### **3. Create GCP Resources**

```bash
cd gcp/

# Create BigQuery tables
bq mk --dataset ${PROJECT_ID}:fraud_detection
bq query --use_legacy_sql=false < bq_schema.sql

# Create Pub/Sub topics and subscriptions (for streaming mode)
./setup_pubsub.sh

# Create GCS buckets
gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://fraud_detection_pipeline_bucket
gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://temp_staging_fraud_detection
```

### **4. Build Flex Template**

```bash
cd gcp/
./build_flex_template.sh
```

⏱️ Takes 3-5 minutes to build and publish the Docker image.

### **5. Run Batch Job**

```bash
# Upload test data to GCS
gsutil cp ../data/wms_receiving.jsonl gs://fraud_detection_pipeline_bucket/
gsutil cp ../data/erp_invoices.jsonl gs://fraud_detection_pipeline_bucket/

# Launch batch job
SA_EMAIL=${SA_EMAIL} ./run_flex_template.sh
```

### **6. Run Streaming Job**

```bash
# Publish test messages to Pub/Sub
./publish_test_messages.sh

# Launch streaming job
./run_stream_template.sh
```

---

## 📊 Validation

### **Check Bronze Layer**
```sql
SELECT COUNT(*) AS total_events 
FROM `your-project.fraud_detection.bronze_events`;
```

### **Check Silver Layer**
```sql
SELECT COUNT(*) AS deduplicated_invoices 
FROM `your-project.fraud_detection.silver_invoices`;
```

### **Check Fraud Alerts**
```sql
SELECT rule_name, severity, COUNT(*) AS alerts
FROM `your-project.fraud_detection.gold_fraud_alerts`
GROUP BY rule_name, severity
ORDER BY rule_name, severity;
```

---

## 🔧 Configuration

### **Template Parameters**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `mode` | Yes | `batch` or `streaming` |
| `wms_input` | Batch only | GCS path to WMS events JSONL |
| `erp_input` | Batch only | GCS path to ERP invoices JSONL |
| `wms_subscription` | Streaming only | Pub/Sub subscription path for WMS events |
| `erp_subscription` | Streaming only | Pub/Sub subscription path for ERP events |
| `bq_dataset` | Yes | BigQuery dataset (project:dataset) |
| `dedup_ttl_seconds` | No | Dedup TTL in seconds (default: 3600) |
| `velocity_window_seconds` | No | Velocity window size (default: 600) |
| `anomaly_stddev_threshold` | No | Anomaly z-score threshold (default: 3.0) |

---

## 📖 Documentation

- [Flex Template Deployment Guide](docs/FLEX_TEMPLATE_DEPLOY.md) - Detailed deployment instructions
- [GCP Resume Checklist](docs/GCP_RESUME_CHECKLIST.md) - Interview talking points

---

## 🛠️ Development

### **Rebuild After Code Changes**

```bash
cd gcp/
./build_flex_template.sh
```

**Important**: After ANY code changes, you MUST rebuild the Flex Template image. Dataflow pulls the image from Artifact Registry, so old code stays until you rebuild.

### **View Job Logs**

```bash
# Get latest job ID
JOB_ID=$(gcloud dataflow jobs list --region=${REGION} --limit=1 --format="value(JOB_ID)")

# View logs
gcloud logging read "resource.type=dataflow_step AND resource.labels.job_id=${JOB_ID}" \
  --limit=50 --format=json
```

### **Cancel Streaming Job**

```bash
gcloud dataflow jobs list --region=${REGION} --filter='STATE=Running'
gcloud dataflow jobs cancel JOB_ID --region=${REGION}
```

---

## 🏆 Production Features

- ✅ **Unified Flex Template** - Single Docker image for batch + streaming
- ✅ **Stateful Deduplication** - TTL-based invoice_id dedup (bounded memory)
- ✅ **Event-time Processing** - Custom timestamp assignment from event data
- ✅ **Multi-layer Architecture** - Bronze/silver/gold medallion pattern
- ✅ **Fraud Detection** - Velocity, anomaly, and fallback rules
- ✅ **Production-grade Logging** - Structured logs with context
- ✅ **IAM Security** - Service account isolation
- ✅ **Cost Optimized** - Batch jobs exit when done (no idle workers)
- ✅ **Autoscaling** - Streaming jobs scale based on throughput

---

## 📝 License

MIT License - See LICENSE file for details

---

## 🤝 Contributing

This is a portfolio project. For questions or issues, please open a GitHub issue.

---

**Built for production deployment on Google Cloud Platform** 🚀
