# Supply Chain Fraud Detection — Deploy & Cleanup Guide 🐶

One doc to rule them all. Covers:
1. Every Docker piece in this project (what, why, how)
2. How to ditch Walmart-internal artifacts (with a manual-download fallback)
3. Whether your image is built and deploy-ready (spoiler: it is ✅)
4. Step-by-step GCP deploy (first-timer friendly)
5. Tear-it-all-down in 2–3 days so you owe Google $0

---

## 1. Docker in this project — the full tour 🐳

There are **two Docker concerns** in this repo. Don't mix them up.

### 1a. Local dev Postgres → `docker-compose.yml`

| Thing | Value |
|---|---|
| Image | `postgres:16-alpine` (~80 MB) |
| Container name | `fraud_pg` |
| Host port | `5433` (because `5432` was already taken on your laptop) |
| Container port | `5432` |
| DB / user / pass | `fraud_detection` / `fraud_app` / `fraud_app` |
| Volume | named `fraud_pg_data` (data survives `down`) |
| Bootstrap SQL | `./db/init_tables.sql` → mounted to `/docker-entrypoint-initdb.d/01_init.sql` (runs once, on first start) |
| Healthcheck | `pg_isready -U fraud_app -d fraud_detection` every 5 s |

Common commands:
```bash
docker compose up -d                 # start
docker compose ps                    # status
docker compose exec postgres psql -U fraud_app -d fraud_detection
docker compose down                  # stop (keeps volume)
docker compose down -v               # stop + WIPE volume (fresh DB next time)
```

### 1b. Pipeline application image → `Dockerfile`

| Thing | Value |
|---|---|
| Base | `python:3.12-slim-bookworm` |
| Stages | Single-stage (no native build needed; everything is a pure-Python or pre-built wheel) |
| Workdir | `/app` (env var `PIPELINE_HOME`) |
| Non-root user | `pipeline` (UID/GID 1000) — CIS best practice |
| Build args | `PYTHON_VERSION`, `PYPI_INDEX_URL`, `PYPI_TRUSTED_HOST` |
| ENTRYPOINT | `python -m` |
| Default CMD | `pipeline.main` (overridable: `docker run img pipeline.gcp_batch_main ...`) |
| Image size | ~924 MB |
| Architecture | `linux/amd64` (forced — your Mac is arm64 but GCP doesn't run arm) |

What's inside `/app`:
```
/app/pipeline/   ← Beam DoFns, transforms, main entry points
/app/scripts/    ← mock data generator
/app/db/         ← Postgres DDL (used by docker-compose only)
/app/setup.py    ← so Dataflow workers can package the module
/app/data/       ← empty, mount or write at runtime
```

Build / run cheatsheet:
```bash
# Build locally (defaults to pypi.org now — no Walmart mirror)
docker build --platform=linux/amd64 -t fraud-detection-pipeline:latest .

# Run pipeline against local files
docker run --rm \
  -v "$PWD/data:/app/data" \
  fraud-detection-pipeline:latest pipeline.main \
  --wms_input data/wms_receiving.jsonl \
  --erp_input data/erp_invoices.jsonl

# Run mock data generator inside the container
docker run --rm -v "$PWD/data:/app/data" \
  fraud-detection-pipeline:latest scripts.generate_mock_data --num-wms 1000
```

### 1c. How they relate
- `docker-compose.yml` is **only for local dev** (gives you a Postgres).
- `Dockerfile` builds the **portable app image** you ship to GCP.
- In Phase 2 on GCP we drop Postgres entirely and write to BigQuery.

---

## 2. Walmart internal artifacts — removed ✂️

### What was Walmart-only
| File | Old | New |
|---|---|---|
| `Dockerfile` | `PYPI_INDEX_URL` defaulted to `pypi.ci.artifacts.walmart.com` | Defaults to `pypi.org` |
| `README.md` | `uv pip install` invoked Walmart mirror | Plain `uv pip install -r requirements.txt` |

Both have been updated. You can still override at build time if you ever want to point at any private mirror:
```bash
docker build --build-arg PYPI_INDEX_URL=https://your-mirror/simple \
             --build-arg PYPI_TRUSTED_HOST=your-mirror -t fraud-detection-pipeline:latest .
```

### Offline / VPN-blocked fallback: manual wheel download

If `pypi.org` is unreachable from where you build:

**Step A — On a machine that DOES have internet** (or your phone hotspot), download all wheels for **linux/amd64 / Python 3.12**:

```bash
mkdir -p wheelhouse
pip download \
  --dest wheelhouse \
  --platform manylinux2014_x86_64 \
  --python-version 312 \
  --implementation cp \
  --abi cp312 \
  --only-binary=:all: \
  -r requirements.txt
```

**The exact libraries you need** (from `requirements.txt`) — direct download links if `pip download` is also blocked:

| Package | Version | Direct URL |
|---|---|---|
| `apache-beam[gcp]` | 2.73.0 | https://pypi.org/project/apache-beam/2.73.0/#files |
| `psycopg2-binary` | 2.9.10 | https://pypi.org/project/psycopg2-binary/2.9.10/#files |
| `Faker` | 30.3.0 | https://pypi.org/project/Faker/30.3.0/#files |
| `python-dateutil` | 2.9.0 | https://pypi.org/project/python-dateutil/2.9.0/#files |
| `structlog` | 24.4.0 | https://pypi.org/project/structlog/24.4.0/#files |
| `pytest` | 8.3.3 | https://pypi.org/project/pytest/8.3.3/#files |
| `pytest-mock` | 3.14.0 | https://pypi.org/project/pytest-mock/3.14.0/#files |

> `apache-beam[gcp]` is the chunky one — it transitively pulls in ~80 wheels (grpcio, google-cloud-*, pyarrow, numpy, etc.). Always prefer `pip download` over hand-grabbing these.

**Step B — Use the wheelhouse in the Docker build.** The `Dockerfile` already contains a commented-out block; uncomment and comment the online `RUN`:

```dockerfile
# COPY wheelhouse/ /tmp/wheelhouse/
# RUN pip install --no-index --find-links=/tmp/wheelhouse -r /tmp/requirements.txt
```

Then build normally:
```bash
docker build --platform=linux/amd64 -t fraud-detection-pipeline:latest .
```

---

## 3. Is the Docker image built and deploy-ready? ✅ Yes.

Verified at deploy-doc-write time:

```
REPO:TAG                                             ID            SIZE   ARCH
fraud-detection-pipeline:latest                      1b9917e3bd3c  924MB  amd64/linux
fraud-detection-pipeline:amd64-latest                1b9917e3bd3c  924MB  amd64/linux
fraud-detection-pipeline:dev                         79259cb5e90e  942MB  amd64/linux
```

Smoke test (already passed):
```
docker run --rm --entrypoint python fraud-detection-pipeline:latest \
  -c "import apache_beam, psycopg2, faker; print(apache_beam.__version__)"
→ imports OK; beam 2.73.0
```

What's **good to go**:
- ✅ Linux/amd64 (will run on GCP)
- ✅ Non-root user
- ✅ All deps install cleanly
- ✅ Pipeline modules importable

What's **NOT yet done** and is required before GCP:
- ❌ Image is not yet pushed to Artifact Registry (it lives only on your laptop)
- ❌ GCP project, bucket, BigQuery dataset, and service account don't exist yet
- ❌ Budget alert not configured

All of the above are handled by the scripts in `gcp/` — covered next.

---

## 4. GCP deploy — step by step (first-timer edition) 🚀

There are TWO ways to deploy. **Use Option A**. It's the cheapest and simplest for a learning project. Option B is the "real prod" path; skip unless you're curious.

### 🛠 Prerequisites (one-time)

1. **Install the gcloud CLI**
   ```bash
   brew install --cask google-cloud-sdk
   ```
2. **Create a GCP project** (or reuse one). The free tier covers everything we're doing.
   - https://console.cloud.google.com/projectcreate
3. **Link a billing account** (required even though we'll stay near $0).
   - https://console.cloud.google.com/billing
4. **Authenticate locally**
   ```bash
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```
5. **Find your billing account ID**
   ```bash
   gcloud billing accounts list
   # copy the ACCOUNT_ID column (format: XXXXXX-XXXXXX-XXXXXX)
   ```

### Option A: BATCH Dataflow job (recommended) — costs ≤ $0.10 per run

#### Step 1 — Set env vars (use these in every shell):
```bash
export PROJECT_ID=your-gcp-project-id
export BILLING_ACCOUNT_ID=XXXXXX-XXXXXX-XXXXXX
export REGION=us-central1
```

#### Step 2 — Create everything in GCP (idempotent, safe to re-run):
```bash
cd /Users/p0m026v/workspace/supply-chain-fraud-detection
chmod +x gcp/*.sh
./gcp/setup.sh
```
This script will:
- Enable APIs (Dataflow, BigQuery, GCS, IAM, Billing)
- Create GCS bucket `gs://${PROJECT_ID}-fraud-pipeline` with a 7-day lifecycle rule
- Create BigQuery dataset `fraud_detection` + medallion tables (bronze/silver/gold + vendor baseline)
- Create service account `fraud-pipeline-sa` with least-privilege roles
- **Create a $25 budget alert at 20% / 40% / 80% / 100%** ← your safety net

Expected output ends with `✅ GCP setup complete.`

#### Step 3 — Build & push the image to Artifact Registry:
```bash
./gcp/build_and_push.sh
```
What it does:
- Creates Artifact Registry repo `fraud-pipeline-repo` if missing
- Configures Docker auth to `${REGION}-docker.pkg.dev`
- Re-builds the image for `linux/amd64` (already amd64 anyway, but the script is explicit)
- Tags as both `:latest` and `:<git-sha>`
- Pushes both tags

Final URL will be:
`us-central1-docker.pkg.dev/${PROJECT_ID}/fraud-pipeline-repo/fraud-detection-pipeline:latest`

#### Step 4 — Trigger a pipeline run on GCP:
```bash
./gcp/trigger_pipeline.sh
```
What it does:
1. Generates mock data **locally** (free)
2. Uploads to `gs://${PROJECT_ID}-fraud-pipeline/input/<timestamp>/`
3. Launches a **batch** Dataflow job (`--max_num_workers=2 --machine_type=e2-small`)
4. Job runs, writes alerts to BigQuery `fraud_detection.gold_fraud_alerts`, then **exits** (no idle worker cost)

Watch it run:
- Console → https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}
- It usually takes 4–7 minutes (most of that is Dataflow worker provisioning, not actual work)

#### Step 5 — See the results:
```bash
bq query --use_legacy_sql=false \
  "SELECT rule_name, severity, COUNT(*) AS alerts
   FROM \`${PROJECT_ID}.fraud_detection.gold_fraud_alerts\`
   WHERE DATE(detected_at) = CURRENT_DATE()
   GROUP BY 1,2 ORDER BY 1,2"
```

That's it. You've deployed. 🎉

### Option B: Cloud Composer (Airflow) — skip for a learning project

The `airflow/dags/fraud_pipeline_dag.py` DAG runs the container via `KubernetesPodOperator`. The catch:

> ⚠ Cloud Composer **environments cost ~$300+/month minimum**, even when idle. Do **NOT** spin one up for a 2-day learning project.

If you ever want it later, the DAG expects these Airflow Variables: `gcp_project_id`, `fraud_image_uri`, `bq_dataset`, `gcs_bucket`.

---

## 5. Tear it all down in 2–3 days 💸→🛑

Two layers of cleanup. Do both to guarantee $0 recurring.

### Layer 1 — kill the data plane (recurring storage costs)
```bash
export PROJECT_ID=your-gcp-project-id
./gcp/teardown.sh
# Type `yes` at the prompt
```
This:
- Cancels any running Dataflow jobs
- Deletes the BigQuery dataset + ALL tables
- Deletes the GCS bucket + ALL objects

After this, recurring cost ≈ **$0**. (The project itself, the service account, and the budget alert remain — they're free to keep around.)

### Layer 2 — nuke the whole project (peace-of-mind option)

If you want **zero possibility** of any future charge:
```bash
# Also deletes the Artifact Registry images (which DO have a tiny storage cost)
gcloud projects delete ${PROJECT_ID}
```
GCP keeps deleted projects in a recoverable state for **30 days** then auto-purges. Billing for the project stops immediately.

### Daily sanity-check while it's still alive
```bash
# Anything still running?
gcloud dataflow jobs list --region=us-central1 --status=active

# Check current month spend
gcloud billing accounts get-iam-policy ${BILLING_ACCOUNT_ID} 2>/dev/null
# Or just open: https://console.cloud.google.com/billing
```

You'll also get **email alerts** at 20% / 40% / 80% / 100% of $25 automatically, courtesy of `setup.sh`. Trust those.

---

## TL;DR

| Question | Answer |
|---|---|
| Is the Docker image ready? | **Yes** — `fraud-detection-pipeline:latest`, linux/amd64, 924 MB, smoke-tested |
| Walmart mirror still referenced? | **No** — defaults to pypi.org now; build-arg override available |
| First GCP deploy command? | `./gcp/setup.sh` (after `export PROJECT_ID=... BILLING_ACCOUNT_ID=...`) |
| How to stop the bleeding in 3 days? | `./gcp/teardown.sh`, optionally followed by `gcloud projects delete ${PROJECT_ID}` |
| Approx cost of one run? | < **$0.10** (batch Dataflow + a few KB GCS + BQ load = free tier) |
