"""
airflow/dags/fraud_pipeline_dag.py
==================================
Cloud Composer (Airflow 2.x) DAG that runs the fraud detection pipeline
container as a Kubernetes Pod inside the Composer cluster.

Why KubernetesPodOperator?
    * The pipeline runs in YOUR container -> reproducible across environments
    * Composer's worker pods stay lean; heavy lifting happens in a sidecar pod
    * The pod terminates as soon as the pipeline exits -> cost-safe
    * Works identically in dev/staging/prod (just swap the image tag)

Trigger model:
    schedule=None -> NEVER runs automatically. Manual trigger only via Airflow UI
    or CLI: `airflow dags trigger fraud_detection_pipeline`.
    Change to schedule="0 6 * * *" later if you want a daily 6 AM run.

Required Airflow Variables (set in Composer UI -> Admin -> Variables):
    gcp_project_id          : your GCP project ID
    fraud_image_uri         : us-central1-docker.pkg.dev/PROJECT/fraud-pipeline-repo/fraud-detection-pipeline:latest
    bq_dataset              : fraud_detection
    gcs_bucket              : <project>-fraud-pipeline
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# ─── Config ───────────────────────────────────────────────────────────────
DAG_ID = "fraud_detection_pipeline"
PROJECT_ID = Variable.get("gcp_project_id")
IMAGE_URI  = Variable.get("fraud_image_uri")
BQ_DATASET = Variable.get("bq_dataset", default_var="fraud_detection")
GCS_BUCKET = Variable.get("gcs_bucket", default_var=f"{PROJECT_ID}-fraud-pipeline")

# Composer's default namespace for user workloads
NAMESPACE = "composer-user-workloads"

# Modest pod resources — pipeline is I/O bound, not compute bound
POD_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m",  "memory": "1Gi"},
    limits  ={"cpu": "1000m", "memory": "2Gi"},
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


# ─── DAG ──────────────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    description="Manual fraud detection batch run; container does the actual work",
    schedule=None,                  # MANUAL trigger only -> no cost while idle
    catchup=False,
    start_date=datetime(2026, 1, 1),
    default_args=DEFAULT_ARGS,
    tags=["fraud", "batch", "manual"],
    max_active_runs=1,              # never two runs at once
) as dag:

    # ── Step 1: Generate mock data INSIDE the pipeline container ──────
    # Writes JSONL to /app/data inside the pod. Pod is ephemeral, so we
    # don't try to persist — the next task reads & uploads.
    generate_data = KubernetesPodOperator(
        task_id="generate_mock_data",
        name="fraud-generate-data",
        namespace=NAMESPACE,
        image=IMAGE_URI,
        cmds=["python", "-m"],
        arguments=[
            "scripts.generate_mock_data",
            "--num-wms",      "{{ dag_run.conf.get('num_wms', 1000) }}",
            "--num-invoices", "{{ dag_run.conf.get('num_invoices', 1000) }}",
        ],
        container_resources=POD_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,        # clean up pods, no zombie cost
    )

    # ── Step 2: Run the pipeline (writes to BigQuery) ──────────────────
    # We mount a shared emptyDir volume between tasks via persistent
    # GCS or we re-generate inside this same pod. Simplest: regenerate.
    run_pipeline = KubernetesPodOperator(
        task_id="run_fraud_pipeline",
        name="fraud-pipeline-run",
        namespace=NAMESPACE,
        image=IMAGE_URI,
        cmds=["/bin/bash", "-c"],
        arguments=[
            # Regen data + run pipeline in the same pod so /app/data persists
            "python -m scripts.generate_mock_data "
            "  --num-wms      {{ dag_run.conf.get('num_wms', 1000) }} "
            "  --num-invoices {{ dag_run.conf.get('num_invoices', 1000) }} "
            "&& python -m pipeline.gcp_batch_main "
            "  --runner=DirectRunner "
            "  --project=" + PROJECT_ID + " "
            "  --wms_input=/app/data/wms_receiving.jsonl "
            "  --erp_input=/app/data/erp_invoices.jsonl "
            "  --bq_dataset=" + PROJECT_ID + ":" + BQ_DATASET + " "
            "  --temp_location=gs://" + GCS_BUCKET + "/temp"
        ],
        env_vars={
            "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
        },
        container_resources=POD_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # ── Step 3: Print a summary query (visible in Airflow logs) ────────
    summarize = KubernetesPodOperator(
        task_id="summarize_alerts",
        name="fraud-summarize",
        namespace=NAMESPACE,
        image="google/cloud-sdk:slim",
        cmds=["bash", "-c"],
        arguments=[
            "bq query --use_legacy_sql=false --project_id=" + PROJECT_ID + " "
            "'SELECT rule_name, severity, COUNT(*) AS alerts "
            " FROM `" + PROJECT_ID + "." + BQ_DATASET + ".gold_fraud_alerts` "
            " WHERE DATE(detected_at) = CURRENT_DATE() "
            " GROUP BY 1,2 ORDER BY 1,2'"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    generate_data >> run_pipeline >> summarize
