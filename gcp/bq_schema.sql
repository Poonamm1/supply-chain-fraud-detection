-- gcp/bq_schema.sql
-- BigQuery DDL — mirrors db/init_tables.sql but with BigQuery types.
-- Run via: bq query --use_legacy_sql=false < gcp/bq_schema.sql
--
-- Cost design notes:
--   * All tables are PARTITIONED by date  -> query cost = scanned bytes
--     within a partition, not the whole table. Default partition filter
--     is set so accidental SELECT * still hits at most one day.
--   * Tables are CLUSTERED by the natural grouping key for cheaper scans.
--   * No streaming inserts (Beam batch loads via load jobs -> FREE).

CREATE SCHEMA IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}`
  OPTIONS (location = '${BQ_LOCATION}', description = 'Fraud detection medallion');

-- ── BRONZE ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events` (
    event_uuid       STRING NOT NULL,
    source_system    STRING NOT NULL,   -- 'WMS' | 'ERP'
    event_type       STRING NOT NULL,
    event_timestamp  TIMESTAMP NOT NULL,
    payload          JSON,
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY source_system, event_type
OPTIONS (require_partition_filter = TRUE);

-- ── SILVER ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices` (
    invoice_id         STRING NOT NULL,
    po_no              STRING,
    vendor_id          STRING NOT NULL,
    upc_no             STRING,
    invoice_amount     NUMERIC(15, 2) NOT NULL,
    invoice_timestamp  TIMESTAMP NOT NULL,
    bank_account_hash  STRING,         -- SHA-256, never raw PII
    email_id           STRING,
    ingested_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(invoice_timestamp)
CLUSTER BY vendor_id
OPTIONS (require_partition_filter = TRUE);

-- ── GOLD ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts` (
    invoice_id    STRING NOT NULL,
    vendor_id     STRING NOT NULL,
    rule_name     STRING NOT NULL,    -- VELOCITY | ANOMALY | FALLBACK
    severity      STRING NOT NULL,    -- CRITICAL | HIGH | MEDIUM | LOW
    reason        STRING,
    evidence      JSON,               -- Structured fraud evidence
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    fraud_score   INT64,              -- 0-100 risk score
    alert_source  STRING,             -- velocity_check | anomaly_check | fallback_path
    detected_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(detected_at)
CLUSTER BY rule_name, severity
OPTIONS (require_partition_filter = TRUE);

-- ══════════════════════════════════════════════════════════════════════════
-- FEATURE ENGINEERING (ML Platform) - PRODUCTION ARCHITECTURE
-- ══════════════════════════════════════════════════════════════════════════
--
-- ARCHITECTURE PRINCIPLE:
--   Feature engineering is INDEPENDENT from Gold fraud detection layer.
--   
--   Silver (invoices)
--     ├── Gold (fraud alerts) - independent
--     └── Feature Engineering - independent
--         ├── Behavioral Features (from silver ONLY)
--         └── Risk Features (from gold ONLY)
--
-- WHY SEPARATE TABLES?
--   1. Stability: Behavioral features don't change when fraud rules evolve
--   2. Reusability: Same behavioral features used by multiple ML models
--   3. Clear Separation: Features (X) vs Labels (y) for supervised learning
--   4. Production Pattern: Google, Netflix, Uber use separate feature/label stores
--
-- TABLES:
--   1. vendor_daily_behavioral_features (PRIMARY) - behavioral signals only
--   2. vendor_daily_risk_features (OPTIONAL) - fraud labels/risk signals
--   3. vendor_daily_features (VIEW) - joins behavioral + risk for ML training
-- ══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────
-- TABLE 1: Behavioral Features (INDEPENDENT from fraud detection)
-- ─────────────────────────────────────────────────────────────────────────
-- Source: silver_deduplicated_invoices ONLY
-- Purpose: Stable, reusable behavioral features for all ML models
-- Pattern: Daily aggregation per vendor
-- 
-- DESIGN DECISIONS:
--   - Partition by feature_date: Cost optimization (query only needed dates)
--   - Cluster by vendor_id: Most queries filter by vendor
--   - Rolling windows (7d, 30d): Computed via scheduled query (see compute_rolling_features.sql)
--   - NO alert features: Behavioral features are intrinsic, not derived from rules
--
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features` (
    vendor_id                STRING NOT NULL,
    feature_date             DATE NOT NULL,
    
    -- VOLUME FEATURES (capture spending patterns)
    invoice_count            INT64 NOT NULL,           -- Daily activity level
    total_invoice_amount     NUMERIC(15, 2) NOT NULL,  -- Daily spending volume
    avg_invoice_amount       NUMERIC(15, 2) NOT NULL,  -- Typical invoice size
    min_invoice_amount       NUMERIC(15, 2) NOT NULL,  -- Distribution lower bound
    max_invoice_amount       NUMERIC(15, 2) NOT NULL,  -- Distribution upper bound
    stddev_invoice_amount    NUMERIC(15, 2) NOT NULL,  -- Invoice volatility/consistency
    
    -- TEMPORAL FEATURES (capture timing patterns)
    avg_invoices_per_hour    FLOAT64 NOT NULL,         -- Submission rate (bot detection)
    latest_invoice_timestamp TIMESTAMP,                -- Recency (for time decay weighting)
    
    -- ROLLING WINDOW FEATURES (computed via BigQuery scheduled query)
    -- These capture temporal context: "Is this unusual for this vendor THIS WEEK?"
    -- ML models with rolling features have 20-40% higher AUC than single-day features.
    invoice_count_7d         INT64,                    -- 7-day rolling sum
    invoice_count_30d        INT64,                    -- 30-day rolling sum
    avg_invoice_amount_7d    NUMERIC(15, 2),           -- 7-day rolling average
    avg_invoice_amount_30d   NUMERIC(15, 2),           -- 30-day rolling average
    
    -- METADATA
    computed_at              TIMESTAMP NOT NULL
)
PARTITION BY feature_date
CLUSTER BY vendor_id
OPTIONS (
    require_partition_filter = TRUE,
    description = 'ML Feature Store: Pure behavioral features (independent from fraud rules)'
);

-- ─────────────────────────────────────────────────────────────────────────
-- TABLE 2: Risk Features (OPTIONAL - labels for supervised learning)
-- ─────────────────────────────────────────────────────────────────────────
-- Source: gold_fraud_alerts ONLY
-- Purpose: Ground truth labels derived from fraud detection rules
-- Pattern: Daily aggregation per vendor (only vendors with alerts)
--
-- DESIGN DECISIONS:
--   - Separate from behavioral features: Rule logic evolves, features don't
--   - OPTIONAL: Can skip for unsupervised learning or pure feature engineering
--   - Sparse table: Only vendors with alerts (most vendors are clean)
--   - JOIN with behavioral at query time (not at storage time)
--
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features` (
    vendor_id                STRING NOT NULL,
    feature_date             DATE NOT NULL,
    
    -- ALERT COUNT FEATURES (fraud signals)
    anomaly_alert_count      INT64 NOT NULL,           -- ANOMALY rule triggers
    velocity_alert_count     INT64 NOT NULL,           -- VELOCITY rule triggers
    duplicate_alert_count    INT64 NOT NULL,           -- DUPLICATE rule triggers
    total_alert_count        INT64 NOT NULL,           -- Total alerts (any type)
    
    -- DERIVED RISK SCORE (computed via BigQuery scheduled query)
    high_risk_alert_ratio    FLOAT64,                  -- alerts / invoice_count (0.0-1.0)
    
    -- ROLLING RISK WINDOWS (computed via BigQuery scheduled query)
    -- Capture vendor risk trajectory: "Has this vendor been risky for weeks?"
    anomaly_count_30d        INT64,                    -- 30-day anomaly history
    velocity_count_30d       INT64,                    -- 30-day velocity history
    total_alert_count_30d    INT64,                    -- 30-day total alert history
    
    -- METADATA
    computed_at              TIMESTAMP NOT NULL
)
PARTITION BY feature_date
CLUSTER BY vendor_id, total_alert_count DESC
OPTIONS (
    require_partition_filter = TRUE,
    description = 'ML Label Store: Risk signals derived from fraud detection rules'
);

-- ─────────────────────────────────────────────────────────────────────────
-- VIEW: vendor_daily_features (JOIN behavioral + risk for ML training)
-- ─────────────────────────────────────────────────────────────────────────
-- Purpose: Single table for Vertex AI export, training, dashboards
-- Pattern: LEFT JOIN (all vendors, even those with no alerts)
--
-- WHY A VIEW?
--   - Flexibility: Can change join logic without recomputing features
--   - Storage: No duplicate data (behavioral + risk stored once)
--   - Clean Separation: Source tables remain independent
--   - Production Pattern: Large companies use views for ML training tables
--
-- USAGE:
--   SELECT * FROM vendor_daily_features WHERE feature_date >= '2026-01-01'
--   (View handles join, coalescing NULLs to 0 for clean vendors)
--
CREATE OR REPLACE VIEW `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_features` AS
SELECT 
    -- Primary key
    b.vendor_id,
    b.feature_date,
    
    -- BEHAVIORAL FEATURES (always present)
    b.invoice_count,
    b.total_invoice_amount,
    b.avg_invoice_amount,
    b.min_invoice_amount,
    b.max_invoice_amount,
    b.stddev_invoice_amount,
    b.avg_invoices_per_hour,
    b.latest_invoice_timestamp,
    
    -- ROLLING BEHAVIORAL FEATURES
    b.invoice_count_7d,
    b.invoice_count_30d,
    b.avg_invoice_amount_7d,
    b.avg_invoice_amount_30d,
    
    -- RISK FEATURES (NULL for clean vendors → COALESCE to 0)
    COALESCE(r.anomaly_alert_count, 0) AS anomaly_alert_count,
    COALESCE(r.velocity_alert_count, 0) AS velocity_alert_count,
    COALESCE(r.duplicate_alert_count, 0) AS duplicate_alert_count,
    COALESCE(r.total_alert_count, 0) AS total_alert_count,
    COALESCE(r.high_risk_alert_ratio, 0.0) AS high_risk_alert_ratio,
    
    -- ROLLING RISK FEATURES
    COALESCE(r.anomaly_count_30d, 0) AS anomaly_count_30d,
    COALESCE(r.velocity_count_30d, 0) AS velocity_count_30d,
    COALESCE(r.total_alert_count_30d, 0) AS total_alert_count_30d,
    
    -- METADATA
    b.computed_at AS behavioral_computed_at,
    r.computed_at AS risk_computed_at
    
FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features` b
LEFT JOIN `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features` r
    ON b.vendor_id = r.vendor_id
    AND b.feature_date = r.feature_date

-- Filter to reduce view size (optional - remove if you want all history)
-- WHERE b.feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
;

-- ── VENDOR BASELINE (small dim table, no partitioning needed) ───────────
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.vendor_90day_baseline` (
    vendor_id              STRING NOT NULL,
    avg_invoice_amount     NUMERIC(15, 2),
    stddev_invoice_amount  NUMERIC(15, 2),
    p99_invoice_amount     NUMERIC(15, 2),
    avg_daily_invoice_cnt  INT64,
    refreshed_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Seed with same 5 vendors as Phase 1 so anomaly logic still trips.
MERGE `${BQ_PROJECT}.${BQ_DATASET}.vendor_90day_baseline` T
USING (
    SELECT * FROM UNNEST([
        STRUCT('V-1001' AS vendor_id, NUMERIC '1200.00' AS avg_invoice_amount,
               NUMERIC '180.00' AS stddev_invoice_amount,
               NUMERIC '1800.00' AS p99_invoice_amount,
               12 AS avg_daily_invoice_cnt),
        STRUCT('V-1002', NUMERIC '4500.00', NUMERIC '600.00',
               NUMERIC '7000.00', 8),
        STRUCT('V-1003', NUMERIC '850.00',  NUMERIC '120.00',
               NUMERIC '1400.00', 22),
        STRUCT('V-1004', NUMERIC '15000.00', NUMERIC '2200.00',
               NUMERIC '22000.00', 3),
        STRUCT('V-1005', NUMERIC '250.00',  NUMERIC '45.00',
               NUMERIC '400.00', 35)
    ])
) S
ON T.vendor_id = S.vendor_id
WHEN NOT MATCHED THEN
    INSERT (vendor_id, avg_invoice_amount, stddev_invoice_amount,
            p99_invoice_amount, avg_daily_invoice_cnt)
    VALUES (S.vendor_id, S.avg_invoice_amount, S.stddev_invoice_amount,
            S.p99_invoice_amount, S.avg_daily_invoice_cnt);
