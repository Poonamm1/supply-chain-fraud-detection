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
--   Behavioral features (from Silver) separate from Risk features (from Gold).
--   
--   Bronze (raw events)
--     ↓
--   Silver (deduplicated invoices)
--     ├────────► Gold Fraud Alerts (business alerts)
--     │
--     └────────► Feature Engineering
--                 ├─ Behavioral Features (from Silver - ALL invoices)
--                 └─ Risk Features (from Gold - fraud labels)
--
-- WHY SEPARATE BEHAVIORAL + RISK?
--   1. STABILITY: Behavioral features don't change when fraud rules evolve
--   2. COMPLETENESS: Behavioral = ALL invoices (10,000/day), Risk = alerts only (50/day)
--   3. SEPARATION: Features (X) vs Labels (y) for supervised learning
--   4. REUSABILITY: Behavioral features used by multiple ML models
--   5. PRODUCTION: Google, Netflix, Uber use this pattern
--
-- EXAMPLE:
--   Silver: 10,000 invoices/day
--     ├─→ Behavioral: 10,000 rows (ALL vendors)
--     └─→ Risk: 50 rows (only vendors with alerts)
--   Training: LEFT JOIN behavioral + risk = 10,000 rows (50 fraud, 9,950 clean)
--
-- ══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────
-- TABLE 1: BEHAVIORAL FEATURES (from Silver invoices ONLY)
-- ─────────────────────────────────────────────────────────────────────────
-- Source: silver_deduplicated_invoices (ALL invoices, not just fraud)
-- Purpose: Stable behavioral features for ML training
-- Written by: Dataflow (BuildVendorDailyBehavioralFeatures transform)
-- Granularity: vendor-day
--
-- WHY FROM SILVER?
--   - ALL invoices (100% data coverage)
--   - Independent from fraud rules (stable feature schema)
--   - Includes clean vendors (needed for negative training examples)
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
    stddev_invoice_amount    NUMERIC(15, 2),           -- Invoice volatility (NULL if count=1)
    
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
    description = 'Behavioral features from Silver invoices (ALL vendors). Written by Dataflow BuildVendorDailyBehavioralFeatures transform.'
);

-- ─────────────────────────────────────────────────────────────────────────
-- TABLE 2: RISK FEATURES (from Gold fraud alerts ONLY)
-- ─────────────────────────────────────────────────────────────────────────
-- Source: gold_fraud_alerts (fraud alerts only)
-- Purpose: Fraud labels for supervised learning
-- Written by: Dataflow (BuildVendorDailyRiskFeatures transform)
-- Granularity: vendor-day
--
-- WHY FROM GOLD?
--   - Fraud labels derived from detection rules
--   - Can evolve independently (new fraud rules don't break behavioral features)
--   - Sparse table (only vendors with alerts)
--
-- IMPORTANT:
--   This table is SPARSE (only vendors with alerts on a given day).
--   For ML training, use LEFT JOIN with behavioral features so clean vendors
--   appear with 0 alert counts.
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
CLUSTER BY vendor_id
OPTIONS (
    require_partition_filter = TRUE,
    description = 'Risk features from Gold fraud alerts (SPARSE - only vendors with alerts). Written by Dataflow BuildVendorDailyRiskFeatures transform.'
);

-- ─────────────────────────────────────────────────────────────────────────
-- VIEW: VENDOR DAILY FEATURES (JOIN behavioral + risk for ML training)
-- ─────────────────────────────────────────────────────────────────────────
-- Purpose: Single table for Vertex AI export, ML training, dashboards
-- Pattern: LEFT JOIN (all vendors, even clean ones with no alerts)
--
-- WHY A VIEW?
--   - Flexibility: Can change join logic without recomputing features
--   - Storage: No duplicate data (behavioral + risk stored once)
--   - Clean Separation: Source tables remain independent
--   - Production Pattern: Large companies use views for ML training datasets
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

-- ══════════════════════════════════════════════════════════════════════════
-- PHASE 2: TRUST SCORE ENGINE & VENDOR RISK RANKING
-- ══════════════════════════════════════════════════════════════════════════
--
-- PURPOSE:
--   Bridge between Feature Engineering (Phase 1) and Future ML Models (Phase 3+).
--   Trust Score combines behavioral + risk features into single risk assessment.
--   Vendor Ranking identifies top risky vendors for investigation.
--
-- DESIGN PRINCIPLES:
--   1. Rule-Based (v1.0): Weighted penalty system (anomaly=15, velocity=10, dup=5)
--   2. Explainable: Every score has breakdown (which alerts triggered, penalty amounts)
--   3. Historical: Track score trends over time (detect vendor degradation)
--   4. ML-Ready: Schema designed to accept ML-based scores without redesign
--
-- EVOLUTION PATH:
--   Phase 2: Rule-based trust score (100 - penalties)
--   Phase 3: ML-assisted (logistic regression coefficients → weights)
--   Phase 4: Fully ML (neural network, probability scores)
--
-- ══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────
-- TABLE: VENDOR TRUST SCORES (Historical Trust Score Records)
-- ─────────────────────────────────────────────────────────────────────────
-- Source: vendor_daily_behavioral_features + vendor_daily_risk_features
-- Written by: BigQuery Scheduled Query (compute_trust_scores.sql)
-- Frequency: Daily at 2:30 AM (after rolling features computed)
-- Granularity: vendor-day
--
-- PURPOSE:
--   - Historical record of trust scores (permanent audit trail)
--   - Trend analysis (score degradation over time)
--   - ML training labels (future fraud probability models)
--   - Reporting/dashboards (vendor performance reviews)
--
-- TRUST SCORE FORMULA (v1.0 - Rule-Based):
--   penalty = (anomaly_count * 15) + (velocity_count * 10) + (duplicate_count * 5)
--   trust_score = MAX(0, 100 - penalty)
--
-- RISK LEVELS:
--   EXCELLENT: trust_score >= 90  (P90+, minimal fraud risk)
--   GOOD:      trust_score >= 70  (P70-P90, low fraud risk)
--   MODERATE:  trust_score >= 50  (P50-P70, medium fraud risk)
--   HIGH:      trust_score < 50   (P0-P50, high fraud risk)
--
-- EXPLAINABILITY:
--   score_explanation (JSON) contains:
--     - Base score (100)
--     - Penalty breakdown (anomaly: 15*count, velocity: 10*count, etc.)
--     - Final score (100 - tenalty)
--     - Risk level reasoning
--
-- ML EVOLUTION:
--   Future: Add ml_fraud_probability column (0.0-1.0)
--   Schema unchanged (trust_score remains for backward compatibility)
--
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.vendor_trust_scores` (
    vendor_id             STRING NOT NULL,
    score_date            DATE NOT NULL,
    
    -- TRUST SCORE (0-100, higher = more trustworthy)
    trust_score           INT64 NOT NULL,              -- Final trust score (0-100)
    risk_level            STRING NOT NULL,             -- EXCELLENT | GOOD | MODERATE | HIGH
    
    -- PENALTY BREAKDOWN (explainability)
    behavioral_penalty    INT64 NOT NULL,              -- Penalty from behavioral features (future)
    risk_penalty          INT64 NOT NULL,              -- Penalty from fraud alerts
    total_penalty         INT64 NOT NULL,              -- behavioral_penalty + risk_penalty
    
    -- ALERT COUNTS (from risk features)
    anomaly_alert_count   INT64 NOT NULL,              -- ANOMALY rule triggers
    velocity_alert_count  INT64 NOT NULL,              -- VELOCITY rule triggers
    duplicate_alert_count INT64 NOT NULL,              -- DUPLICATE rule triggers
    total_alert_count     INT64 NOT NULL,              -- Sum of all alerts
    
    -- EXPLAINABILITY (JSON breakdown)
    score_explanation     JSON,                        -- Detailed score calculation
    
    -- FUTURE ML (Phase 3+)
    ml_fraud_probability  FLOAT64,                     -- ML-based fraud probability (0.0-1.0)
    ml_model_version      STRING,                      -- Model version used
    
    -- METADATA
    computed_at           TIMESTAMP NOT NULL
)
PARTITION BY score_date
CLUSTER BY vendor_id, trust_score
OPTIONS (
    require_partition_filter = TRUE,
    description = 'Historical trust scores (daily vendor risk assessment). Partitioned by date, clustered by vendor_id for trend queries.'
);

-- ─────────────────────────────────────────────────────────────────────────
-- TABLE: VENDOR RISK RANKINGS (Daily Vendor Risk Leaderboard)
-- ─────────────────────────────────────────────────────────────────────────
-- Source: vendor_trust_scores
-- Written by: BigQuery Scheduled Query (compute_vendor_rankings.sql)
-- Frequency: Daily at 2:45 AM (after trust scores computed)
-- Granularity: vendor-day
--
-- PURPOSE:
--   - Identify top risky vendors (lowest trust scores)
--   - Priority queue for fraud investigations
--   - Executive dashboards ("Top 10 Riskiest Vendors")
--   - Alert routing (send highest risk to senior analysts)
--
-- RANKING LOGIC:
--   - Rank 1 = LOWEST trust score (highest risk)
--   - Rank 2 = 2nd lowest trust score
--   - ...
--   - Rank N = HIGHEST trust score (lowest risk)
--
-- USE CASES:
--   - "Show me top 10 riskiest vendors today"
--   - "Which vendors moved up in risk rank this week?"
--   - "Alert me when vendor enters top 20 risk rank"
--
CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.vendor_risk_rankings` (
    ranking_date          DATE NOT NULL,
    vendor_id             STRING NOT NULL,
    trust_score           INT64 NOT NULL,              -- Trust score (0-100)
    risk_rank             INT64 NOT NULL,              -- Rank (1 = highest risk)
    risk_level            STRING NOT NULL,             -- EXCELLENT | GOOD | MODERATE | HIGH
    rank_change           INT64,                       -- Change vs yesterday (+/- positions)
    
    -- METADATA
    computed_at           TIMESTAMP NOT NULL
)
PARTITION BY ranking_date
CLUSTER BY vendor_id, risk_rank
OPTIONS (
    require_partition_filter = TRUE,
    description = 'Daily vendor risk rankings (leaderboard of risky vendors). Rank 1 = highest risk. Partitioned by date for daily snapshots.'
);

-- ─────────────────────────────────────────────────────────────────────────

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
