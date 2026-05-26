# syntax=docker/dockerfile:1.7
# =============================================================================
# Supply Chain Fraud Detection — Production Container
# =============================================================================
# Single-stage build (no system packages needed!) — psycopg2-binary ships its
# own libpq, and all other deps are pure-Python wheels.
#
# Image runs as non-root user `pipeline` (UID 1000) per CIS best practice.
# Default ENTRYPOINT executes pipeline.main; override via `docker run ... CMD`.
# =============================================================================

ARG PYTHON_VERSION=3.12-slim-bookworm
FROM python:${PYTHON_VERSION}

# Walmart internal PyPI mirror is reachable from the corporate network when
# pypi.org is firewalled. Override at build time with:
#   docker build --build-arg PYPI_INDEX_URL=https://pypi.org/simple ...
ARG PYPI_INDEX_URL=https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple
ARG PYPI_TRUSTED_HOST=pypi.ci.artifacts.walmart.com

LABEL org.opencontainers.image.title="supply-chain-fraud-detection" \
      org.opencontainers.image.description="Apache Beam fraud detection pipeline" \
      org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIPELINE_HOME=/app

# Non-root user (UID 1000 matches typical Linux conventions)
RUN groupadd --gid 1000 pipeline && \
    useradd  --uid 1000 --gid pipeline --create-home --shell /bin/bash pipeline

WORKDIR ${PIPELINE_HOME}

# Install Python deps first (Docker layer caching: pip layer reuses unless
# requirements.txt changes — source-only edits skip the slow pip step).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade --index-url "${PYPI_INDEX_URL}" --trusted-host "${PYPI_TRUSTED_HOST}" pip setuptools wheel && \
    pip install --index-url "${PYPI_INDEX_URL}" --trusted-host "${PYPI_TRUSTED_HOST}" -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Application source — copied with proper ownership
COPY --chown=pipeline:pipeline pipeline/  ./pipeline/
COPY --chown=pipeline:pipeline scripts/   ./scripts/
COPY --chown=pipeline:pipeline setup.py   ./
COPY --chown=pipeline:pipeline db/        ./db/

# Data dir is mounted at runtime; create as writable placeholder
RUN mkdir -p ${PIPELINE_HOME}/data && chown pipeline:pipeline ${PIPELINE_HOME}/data

USER pipeline

# Default: run the main pipeline. Override to:
#   docker run IMAGE scripts.generate_mock_data --num-wms 1000
#   docker run IMAGE pipeline.gcp_batch_main --runner=DataflowRunner ...
ENTRYPOINT ["python", "-m"]
CMD ["pipeline.main"]
