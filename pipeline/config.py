"""
pipeline.config
===============
Single source of truth for runtime configuration.
Everything is overridable by environment variable so the same artifact
ships to local Docker, CI, and Dataflow without code changes (12-factor).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final


# ---------------------------------------------------------------------------
# Constants — keep MAGIC NUMBERS out of business logic (Zen: explicit > implicit)
# ---------------------------------------------------------------------------
DEFAULT_VELOCITY_WINDOW_SECONDS: Final[int] = 10 * 60       # 10-minute sliding
DEFAULT_VELOCITY_PERIOD_SECONDS: Final[int] = 60            # advance every 60s
DEFAULT_ALLOWED_LATENESS_SECONDS: Final[int] = 60 * 60      # 1 hour late data
DEFAULT_DEDUP_TTL_SECONDS: Final[int] = 24 * 60 * 60        # 24h dedup memory
DEFAULT_ANOMALY_STDDEV_THRESHOLD: Final[float] = 3.0        # 3-sigma rule


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for the local PostgreSQL sink."""
    host: str     = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: int     = field(default_factory=lambda: int(os.getenv("PG_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("PG_DB",   "fraud_detection"))
    user: str     = field(default_factory=lambda: os.getenv("PG_USER", "fraud_app"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "fraud_app"))

    @property
    def dsn(self) -> str:
        """libpq-style DSN, used by psycopg2.connect()."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password} "
            f"connect_timeout=5 application_name=fraud_pipeline"
        )


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level runtime knobs for the Beam pipeline."""
    wms_input_path: str  = field(default_factory=lambda: os.getenv(
        "WMS_INPUT_PATH", "data/wms_receiving.jsonl"))
    erp_input_path: str  = field(default_factory=lambda: os.getenv(
        "ERP_INPUT_PATH", "data/erp_invoices.jsonl"))

    velocity_window_seconds:  int = DEFAULT_VELOCITY_WINDOW_SECONDS
    velocity_period_seconds:  int = DEFAULT_VELOCITY_PERIOD_SECONDS
    allowed_lateness_seconds: int = DEFAULT_ALLOWED_LATENESS_SECONDS
    dedup_ttl_seconds:        int = DEFAULT_DEDUP_TTL_SECONDS
    anomaly_stddev_threshold: float = DEFAULT_ANOMALY_STDDEV_THRESHOLD

    # Throughput knobs — DirectRunner respects these via --direct_num_workers
    direct_num_workers: int = field(default_factory=lambda: int(
        os.getenv("DIRECT_NUM_WORKERS", "4")))

    postgres: PostgresConfig = field(default_factory=PostgresConfig)
