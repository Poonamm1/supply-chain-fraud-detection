-- =============================================================================
--  Supply Chain Fraud Detection — Local PostgreSQL Schema (Phase 1)
-- =============================================================================
--  Layout mirrors our prod Medallion architecture:
--      bronze_raw_events            -> immutable landing zone (append-only)
--      silver_deduplicated_invoices -> cleansed, business-key-unique
--      vendor_90day_baseline        -> aggregate features for anomaly scoring
--
--  Notes on production parity:
--    * In prod these are BigQuery tables — we keep types/column names identical
--      so the same schema-evolution PRs can be cherry-picked across.
--    * All timestamps are stored in UTC (`TIMESTAMPTZ`). Local TZs are a
--      footgun at scale.
--    * Primary keys are *business keys*, not surrogate IDs. Auditors love this.
-- =============================================================================

-- Idempotent re-runs for local dev
DROP TABLE IF EXISTS bronze_raw_events           CASCADE;
DROP TABLE IF EXISTS silver_deduplicated_invoices CASCADE;
DROP TABLE IF EXISTS vendor_90day_baseline       CASCADE;
DROP TABLE IF EXISTS gold_fraud_alerts           CASCADE;

-- -----------------------------------------------------------------------------
-- 1. BRONZE — raw landing for *every* event, both sources, no filtering.
--    Schema-on-read via the JSONB payload keeps us flexible while upstream
--    teams iterate on their contracts.
-- -----------------------------------------------------------------------------
CREATE TABLE bronze_raw_events (
    event_uuid       UUID            PRIMARY KEY,         -- generated in-pipeline
    source_system    VARCHAR(32)     NOT NULL,            -- 'WMS' | 'ERP'
    event_type       VARCHAR(64)     NOT NULL,            -- 'RECEIVING' | 'INVOICE'
    ingest_timestamp TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    event_timestamp  TIMESTAMPTZ     NOT NULL,            -- the *event-time*
    payload          JSONB           NOT NULL,
    -- Cheap retrieval by source/time for ops dashboards
    CHECK (source_system IN ('WMS', 'ERP'))
);
CREATE INDEX idx_bronze_source_time
    ON bronze_raw_events (source_system, event_timestamp DESC);
CREATE INDEX idx_bronze_payload_gin
    ON bronze_raw_events USING GIN (payload);


-- -----------------------------------------------------------------------------
-- 2. SILVER — deduplicated invoices, one row per business key.
--    Pipeline guarantees invoice_id uniqueness via a stateful DoFn.
--    `ON CONFLICT DO NOTHING` is the belt-and-suspenders second line of
--    defense for at-least-once delivery semantics.
-- -----------------------------------------------------------------------------
CREATE TABLE silver_deduplicated_invoices (
    invoice_id            VARCHAR(64)   PRIMARY KEY,
    po_no                 VARCHAR(64)   NOT NULL,
    vendor_id             VARCHAR(32)   NOT NULL,
    upc_no                VARCHAR(32),
    invoice_amount        NUMERIC(14,2) NOT NULL,
    invoice_timestamp     TIMESTAMPTZ   NOT NULL,
    bank_account_hash     CHAR(64)      NOT NULL,         -- SHA-256, never PII
    email_id              VARCHAR(254),                   -- RFC 5321 max
    processed_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    dedup_window_start    TIMESTAMPTZ,
    CHECK (invoice_amount >= 0)
);
CREATE INDEX idx_silver_vendor_time
    ON silver_deduplicated_invoices (vendor_id, invoice_timestamp DESC);


-- -----------------------------------------------------------------------------
-- 3. VENDOR 90-DAY BASELINE — pre-computed features used as a SIDE INPUT
--    for anomaly scoring. In prod this is refreshed nightly by a Dataflow
--    batch job; here we hand-seed it for deterministic local tests.
-- -----------------------------------------------------------------------------
CREATE TABLE vendor_90day_baseline (
    vendor_id              VARCHAR(32)  PRIMARY KEY,
    avg_invoice_amount     NUMERIC(14,2) NOT NULL,
    stddev_invoice_amount  NUMERIC(14,2) NOT NULL,
    p99_invoice_amount     NUMERIC(14,2) NOT NULL,
    avg_daily_invoice_cnt  INTEGER       NOT NULL,
    last_refreshed_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Seed a few well-known vendors so the pipeline has something to score against.
-- Anything not in this table will trip the "Review Required - Fallback Mode"
-- path on purpose during demo runs.
INSERT INTO vendor_90day_baseline
    (vendor_id, avg_invoice_amount, stddev_invoice_amount,
     p99_invoice_amount, avg_daily_invoice_cnt) VALUES
    ('V-1001',  4500.00,  850.00,  9000.00, 12),
    ('V-1002', 12500.00, 2100.00, 22000.00, 30),
    ('V-1003',   780.00,  140.00,  1900.00,  4),
    ('V-1004', 50000.00, 8000.00, 88000.00,  8),
    ('V-1005',   250.00,   45.00,   600.00, 60);


-- -----------------------------------------------------------------------------
-- 4. GOLD — fraud alerts surfaced to investigators / downstream case mgmt.
--    Separate table so retention / RBAC can differ from the silver layer.
-- -----------------------------------------------------------------------------
CREATE TABLE gold_fraud_alerts (
    alert_id          BIGSERIAL     PRIMARY KEY,
    invoice_id        VARCHAR(64)   NOT NULL,
    vendor_id         VARCHAR(32)   NOT NULL,
    rule_name         VARCHAR(64)   NOT NULL,    -- 'VELOCITY' | 'ANOMALY' | 'FALLBACK'
    severity          VARCHAR(16)   NOT NULL,    -- 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
    reason            TEXT          NOT NULL,
    evidence          JSONB         NOT NULL,
    detected_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    window_start      TIMESTAMPTZ,
    window_end        TIMESTAMPTZ,
    CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL'))
);
CREATE INDEX idx_gold_vendor_rule_time
    ON gold_fraud_alerts (vendor_id, rule_name, detected_at DESC);
