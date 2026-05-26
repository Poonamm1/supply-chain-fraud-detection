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
    evidence      JSON,
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    detected_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(detected_at)
CLUSTER BY rule_name, severity
OPTIONS (require_partition_filter = TRUE);

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
