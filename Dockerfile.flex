# syntax=docker/dockerfile:1.7
# =============================================================================
# Supply Chain Fraud Detection — Dataflow Flex Template Image
# =============================================================================
# This is a SEPARATE image from the regular Dockerfile. It's purpose-built to
# be invoked by Dataflow's template launcher, NOT to be run directly.
#
# Base image MUST be one of Google's `*-template-launcher-base` images —
# they ship the `/opt/google/dataflow/python_template_launcher` binary that
# Dataflow's worker plane invokes when you call `flex-template run`.
#
# We pin python311 because Beam 2.73 supports 3.9–3.12 and python311 is the
# most battle-tested launcher base (python312 exists but newer).
# =============================================================================

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG PYPI_INDEX_URL=https://pypi.org/simple
ARG PYPI_TRUSTED_HOST=pypi.org

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# The Flex Template launcher reads THESE env vars to know what to run.
#   FLEX_TEMPLATE_PYTHON_PY_FILE          → the pipeline entry script
#   FLEX_TEMPLATE_PYTHON_SETUP_FILE       → so worker VMs can pip-install our pkg
#   FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE→ pinned deps for the worker harness
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/pipeline/gcp_batch_main.py" \
    FLEX_TEMPLATE_PYTHON_SETUP_FILE="/template/setup.py" \
    FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements-flex.txt"

WORKDIR /template

# Pre-install deps INTO the launcher image so the template launches faster
# (otherwise the launcher pip-installs at job start every time).
# Uses requirements-flex.txt — a slim subset (no psycopg2, no Faker, no pytest)
# that matches what gcp_batch_main.py actually imports.
COPY requirements-flex.txt /template/requirements-flex.txt
RUN pip install --upgrade \
        --index-url "${PYPI_INDEX_URL}" --trusted-host "${PYPI_TRUSTED_HOST}" \
        pip setuptools wheel && \
    pip install \
        --index-url "${PYPI_INDEX_URL}" --trusted-host "${PYPI_TRUSTED_HOST}" \
        -r /template/requirements-flex.txt

# Copy the pipeline source LAST so source edits don't bust the pip cache.
COPY pipeline/   /template/pipeline/
COPY scripts/    /template/scripts/
COPY setup.py    /template/setup.py

# OFFLINE FALLBACK: see Dockerfile (same wheelhouse pattern works here).
# COPY wheelhouse/ /tmp/wheelhouse/
# RUN pip install --no-index --find-links=/tmp/wheelhouse -r /template/requirements-flex.txt

# NOTE: do NOT set ENTRYPOINT / CMD here. The base image's launcher binary IS
# the entrypoint. Setting our own breaks template launching.
