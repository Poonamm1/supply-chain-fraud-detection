-- ═══════════════════════════════════════════════════════════════════════════
-- BigQuery Validation Queries — Batch & Streaming Mode
-- ═══════════════════════════════════════════════════════════════════════════
-- Use these queries to validate that both batch and streaming pipelines are
-- working correctly and writing data to bronze/silver/gold tables.
--
-- Replace ${BQ_PROJECT} and ${BQ_DATASET} with your actual values.
-- ═══════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- 1️⃣  REAL-TIME LAYER HEALTH CHECK (last 1 hour)
-- ─────────────────────────────────────────────────────────────────────────────
WITH layer_counts AS (
  SELECT 'bronze' AS layer, COUNT(*) AS row_count,
         MIN(event_timestamp) AS earliest,
         MAX(event_timestamp) AS latest
  FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
  WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  
  UNION ALL
  
  SELECT 'silver' AS layer, COUNT(*) AS row_count,
         MIN(invoice_timestamp) AS earliest,
         MAX(invoice_timestamp) AS latest
  FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
  WHERE invoice_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  
  UNION ALL
  
  SELECT 'gold' AS layer, COUNT(*) AS row_count,
         MIN(detected_at) AS earliest,
         MAX(detected_at) AS latest
  FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
  WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
)
SELECT
  layer,
  row_count,
  earliest,
  latest,
  TIMESTAMP_DIFF(latest, earliest, SECOND) AS span_seconds,
  CASE
    WHEN layer = 'bronze' AND row_count > 0 THEN '✅ Data ingesting'
    WHEN layer = 'silver' AND row_count > 0 THEN '✅ Dedup working'
    WHEN layer = 'gold' AND row_count > 0 THEN '✅ Fraud detection active'
    WHEN row_count = 0 THEN '⚠️  No recent data'
    ELSE '❓ Unknown'
  END AS status
FROM layer_counts
ORDER BY
  CASE layer
    WHEN 'bronze' THEN 1
    WHEN 'silver' THEN 2
    WHEN 'gold' THEN 3
  END;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2️⃣  STREAMING LAG CHECK (are events arriving in real-time?)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  'bronze' AS layer,
  COUNT(*) AS events_last_5min,
  MAX(event_timestamp) AS latest_event_time,
  MAX(ingested_at) AS latest_ingest_time,
  TIMESTAMP_DIFF(MAX(ingested_at), MAX(event_timestamp), SECOND) AS processing_lag_seconds
FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)

UNION ALL

SELECT
  'silver' AS layer,
  COUNT(*) AS events_last_5min,
  MAX(invoice_timestamp) AS latest_event_time,
  MAX(ingested_at) AS latest_ingest_time,
  TIMESTAMP_DIFF(MAX(ingested_at), MAX(invoice_timestamp), SECOND) AS processing_lag_seconds
FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)

UNION ALL

SELECT
  'gold' AS layer,
  COUNT(*) AS events_last_5min,
  MIN(detected_at) AS latest_event_time,  -- Use MIN as proxy for event time
  MAX(detected_at) AS latest_ingest_time,
  0 AS processing_lag_seconds
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)

ORDER BY layer;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3️⃣  FRAUD ALERTS — LAST 10 MINUTES (streaming validation)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  detected_at,
  invoice_id,
  vendor_id,
  rule_name,
  severity,
  fraud_score,
  reason,
  JSON_EXTRACT_SCALAR(evidence, '$.amount') AS amount,
  JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS z_score,
  JSON_EXTRACT_SCALAR(evidence, '$.occurrences') AS velocity_count,
  alert_source
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
ORDER BY detected_at DESC, fraud_score DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4️⃣  FRAUD ALERTS BY RULE (last 1 hour)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  COUNT(DISTINCT vendor_id) AS unique_vendors,
  ROUND(AVG(fraud_score), 1) AS avg_fraud_score,
  MIN(detected_at) AS first_alert,
  MAX(detected_at) AS last_alert
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY rule_name, severity
ORDER BY
  CASE rule_name
    WHEN 'VELOCITY' THEN 1
    WHEN 'ANOMALY' THEN 2
    WHEN 'FALLBACK' THEN 3
  END,
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
  END;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5️⃣  SCHEMA VALIDATION — Ensure all fields are properly typed
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM `${BQ_PROJECT}.${BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('bronze_raw_events', 'silver_deduplicated_invoices', 'gold_fraud_alerts')
ORDER BY table_name, ordinal_position;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6️⃣  DUPLICATE DETECTION VALIDATION (should be ZERO in silver)
-- ─────────────────────────────────────────────────────────────────────────────
WITH invoice_counts AS (
  SELECT
    invoice_id,
    COUNT(*) AS occurrence_count,
    MIN(invoice_timestamp) AS first_seen,
    MAX(invoice_timestamp) AS last_seen
  FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
  WHERE DATE(invoice_timestamp) >= CURRENT_DATE() - 1
  GROUP BY invoice_id
)
SELECT
  *,
  TIMESTAMP_DIFF(last_seen, first_seen, SECOND) AS seconds_apart
FROM invoice_counts
WHERE occurrence_count > 1  -- Should be ZERO if dedup is working!
ORDER BY occurrence_count DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7️⃣  PAYLOAD SCHEMA VALIDATION (check if payload is valid JSON)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  event_uuid,
  source_system,
  event_type,
  -- Try to extract a field from payload to verify it's valid JSON
  JSON_EXTRACT_SCALAR(payload, '$.invoice_id') AS invoice_id_from_payload,
  JSON_EXTRACT_SCALAR(payload, '$.vendor_id') AS vendor_id_from_payload,
  payload
FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
WHERE source_system = 'ERP'
  AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
LIMIT 5;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8️⃣  EVIDENCE JSON VALIDATION (check if evidence is valid JSON)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  invoice_id,
  vendor_id,
  rule_name,
  -- Extract various evidence fields based on rule type
  JSON_EXTRACT_SCALAR(evidence, '$.amount') AS amount,
  JSON_EXTRACT_SCALAR(evidence, '$.baseline_avg') AS baseline_avg,
  JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS z_score,
  JSON_EXTRACT_SCALAR(evidence, '$.occurrences') AS occurrences,
  JSON_EXTRACT_SCALAR(evidence, '$.fraud_pattern') AS fraud_pattern,
  evidence
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9️⃣  WINDOW TIMESTAMP VALIDATION (ensure they're proper timestamps)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  invoice_id,
  vendor_id,
  rule_name,
  window_start,
  window_end,
  TIMESTAMP_DIFF(window_end, window_start, SECOND) AS window_duration_seconds,
  detected_at,
  -- Check if window times are reasonable (should be within last 24 hours for streaming)
  CASE
    WHEN window_start IS NULL THEN 'No window (anomaly/fallback)'
    WHEN window_start > CURRENT_TIMESTAMP() THEN '❌ Future timestamp!'
    WHEN window_start < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN '⚠️  Very old'
    ELSE '✅ Valid'
  END AS window_validation
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY detected_at DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- 🔟  FRAUD SCORE DISTRIBUTION (ensure scores are 0-100)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  rule_name,
  MIN(fraud_score) AS min_score,
  MAX(fraud_score) AS max_score,
  ROUND(AVG(fraud_score), 1) AS avg_score,
  APPROX_QUANTILES(fraud_score, 100)[OFFSET(50)] AS median_score,
  APPROX_QUANTILES(fraud_score, 100)[OFFSET(90)] AS p90_score,
  COUNT(*) AS alert_count,
  COUNTIF(fraud_score < 0 OR fraud_score > 100) AS invalid_scores
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY rule_name
ORDER BY avg_score DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1️⃣1️⃣  STREAMING THROUGHPUT (messages per minute)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  TIMESTAMP_TRUNC(ingested_at, MINUTE) AS minute_bucket,
  COUNT(*) AS messages_per_minute,
  COUNT(DISTINCT JSON_EXTRACT_SCALAR(payload, '$.vendor_id')) AS unique_vendors
FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY minute_bucket
ORDER BY minute_bucket DESC
LIMIT 60;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1️⃣2️⃣  END-TO-END LATENCY (bronze → silver → gold)
-- ─────────────────────────────────────────────────────────────────────────────
WITH bronze_times AS (
  SELECT
    JSON_EXTRACT_SCALAR(payload, '$.invoice_id') AS invoice_id,
    event_timestamp,
    ingested_at AS bronze_ingested_at
  FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
  WHERE source_system = 'ERP'
    AND ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
),
silver_times AS (
  SELECT
    invoice_id,
    invoice_timestamp,
    ingested_at AS silver_ingested_at
  FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
  WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
),
gold_times AS (
  SELECT
    invoice_id,
    detected_at AS gold_ingested_at
  FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
  WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
)
SELECT
  b.invoice_id,
  b.bronze_ingested_at,
  s.silver_ingested_at,
  g.gold_ingested_at,
  TIMESTAMP_DIFF(s.silver_ingested_at, b.bronze_ingested_at, SECOND) AS bronze_to_silver_sec,
  TIMESTAMP_DIFF(g.gold_ingested_at, s.silver_ingested_at, SECOND) AS silver_to_gold_sec,
  TIMESTAMP_DIFF(g.gold_ingested_at, b.bronze_ingested_at, SECOND) AS total_latency_sec
FROM bronze_times b
LEFT JOIN silver_times s ON b.invoice_id = s.invoice_id
LEFT JOIN gold_times g ON b.invoice_id = g.invoice_id
WHERE g.gold_ingested_at IS NOT NULL  -- Only show fraudulent invoices
ORDER BY b.bronze_ingested_at DESC
LIMIT 10;
