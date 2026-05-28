-- ═══════════════════════════════════════════════════════════════════════════
-- BigQuery Validation Queries for Supply Chain Fraud Detection Pipeline
-- ═══════════════════════════════════════════════════════════════════════════
-- Run these queries after your Dataflow job completes to validate fraud alerts
-- and inspect the medallion architecture layers.
--
-- Replace ${BQ_PROJECT} and ${BQ_DATASET} with your actual values, e.g.:
--   fraud-detect-260526-1750.fraud_detection
-- ═══════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- 1️⃣  LAYER HEALTH CHECK — Medallion Architecture Validation
-- ─────────────────────────────────────────────────────────────────────────────
WITH layer_counts AS (
  SELECT 'bronze' AS layer, COUNT(*) AS row_count
  FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
  WHERE DATE(event_timestamp) >= CURRENT_DATE() - 7
  
  UNION ALL
  
  SELECT 'silver' AS layer, COUNT(*) AS row_count
  FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
  WHERE DATE(invoice_timestamp) >= CURRENT_DATE() - 7
  
  UNION ALL
  
  SELECT 'gold' AS layer, COUNT(*) AS row_count
  FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
  WHERE DATE(detected_at) >= CURRENT_DATE() - 7
)
SELECT
  layer,
  row_count,
  CASE
    WHEN layer = 'bronze' AND row_count > 0 THEN '✅ Data ingested'
    WHEN layer = 'silver' AND row_count > 0 THEN '✅ Deduplication working'
    WHEN layer = 'gold' AND row_count > 0 THEN '✅ Fraud detection active'
    ELSE '❌ No data - check pipeline'
  END AS status
FROM layer_counts
ORDER BY
  CASE layer
    WHEN 'bronze' THEN 1
    WHEN 'silver' THEN 2
    WHEN 'gold' THEN 3
  END;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2️⃣  FRAUD ALERTS BY RULE — Portfolio Demo View
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  COUNT(DISTINCT vendor_id) AS unique_vendors,
  COUNT(DISTINCT invoice_id) AS unique_invoices,
  ROUND(AVG(fraud_score), 2) AS avg_fraud_score,
  MIN(detected_at) AS first_alert,
  MAX(detected_at) AS last_alert
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7
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
    WHEN 'LOW' THEN 4
  END;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3️⃣  DETAILED FRAUD ALERTS — Interview-Ready Export
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  invoice_id,
  vendor_id,
  rule_name,
  severity,
  fraud_score,
  reason,
  JSON_EXTRACT_SCALAR(evidence, '$.amount') AS invoice_amount,
  JSON_EXTRACT_SCALAR(evidence, '$.baseline_avg') AS baseline_avg,
  JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS z_score,
  JSON_EXTRACT_SCALAR(evidence, '$.occurrences') AS velocity_count,
  JSON_EXTRACT_SCALAR(evidence, '$.fraud_pattern') AS fraud_pattern,
  alert_source,
  window_start,
  window_end,
  detected_at
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7
ORDER BY fraud_score DESC, detected_at DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4️⃣  VELOCITY FRAUD DEEP DIVE
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  vendor_id,
  JSON_EXTRACT_SCALAR(evidence, '$.amount') AS repeated_amount,
  CAST(JSON_EXTRACT_SCALAR(evidence, '$.occurrences') AS INT64) AS occurrence_count,
  CAST(JSON_EXTRACT_SCALAR(evidence, '$.window_duration_seconds') AS INT64) AS window_seconds,
  JSON_EXTRACT_SCALAR(evidence, '$.invoice_ids') AS invoice_list,
  COUNT(DISTINCT invoice_id) AS unique_flagged_invoices,
  MIN(window_start) AS burst_start,
  MAX(window_end) AS burst_end,
  STRING_AGG(DISTINCT severity ORDER BY severity) AS severities
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE rule_name = 'VELOCITY'
  AND DATE(detected_at) >= CURRENT_DATE() - 7
GROUP BY vendor_id, repeated_amount, occurrence_count, window_seconds, invoice_list
ORDER BY occurrence_count DESC, window_seconds ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5️⃣  ANOMALY FRAUD DEEP DIVE — Z-Score Distribution
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  vendor_id,
  invoice_id,
  severity,
  fraud_score,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.amount') AS FLOAT64), 2) AS invoice_amount,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.baseline_avg') AS FLOAT64), 2) AS baseline_avg,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.baseline_stddev') AS FLOAT64), 2) AS baseline_stddev,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS FLOAT64), 2) AS z_score,
  ROUND(CAST(JSON_EXTRACT_SCALAR(evidence, '$.deviation_pct') AS FLOAT64), 2) AS deviation_pct,
  reason,
  detected_at
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE rule_name = 'ANOMALY'
  AND DATE(detected_at) >= CURRENT_DATE() - 7
ORDER BY ABS(CAST(JSON_EXTRACT_SCALAR(evidence, '$.z_score') AS FLOAT64)) DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6️⃣  FALLBACK PATH ANALYSIS — Unknown Vendors
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  vendor_id,
  COUNT(*) AS fallback_count,
  ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(evidence, '$.amount') AS FLOAT64)), 2) AS avg_amount,
  MIN(CAST(JSON_EXTRACT_SCALAR(evidence, '$.amount') AS FLOAT64)) AS min_amount,
  MAX(CAST(JSON_EXTRACT_SCALAR(evidence, '$.amount') AS FLOAT64)) AS max_amount,
  JSON_EXTRACT_SCALAR(evidence, '$.fallback_reason') AS reason,
  MIN(detected_at) AS first_seen,
  MAX(detected_at) AS last_seen
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE rule_name = 'FALLBACK'
  AND DATE(detected_at) >= CURRENT_DATE() - 7
GROUP BY vendor_id, reason
ORDER BY fallback_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7️⃣  DUPLICATE DETECTION — Silver Layer Analysis
-- ─────────────────────────────────────────────────────────────────────────────
-- Find potential duplicates that made it through (shouldn't happen!)
WITH invoice_occurrences AS (
  SELECT
    invoice_id,
    vendor_id,
    invoice_amount,
    COUNT(*) AS occurrence_count,
    MIN(invoice_timestamp) AS first_seen,
    MAX(invoice_timestamp) AS last_seen
  FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
  WHERE DATE(invoice_timestamp) >= CURRENT_DATE() - 7
  GROUP BY invoice_id, vendor_id, invoice_amount
)
SELECT
  *,
  TIMESTAMP_DIFF(last_seen, first_seen, SECOND) AS seconds_between
FROM invoice_occurrences
WHERE occurrence_count > 1  -- Should be ZERO if dedup is working!
ORDER BY occurrence_count DESC, seconds_between ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8️⃣  TOP RISKY VENDORS — Executive Dashboard View
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  vendor_id,
  COUNT(*) AS total_alerts,
  COUNTIF(rule_name = 'VELOCITY') AS velocity_alerts,
  COUNTIF(rule_name = 'ANOMALY') AS anomaly_alerts,
  COUNTIF(rule_name = 'FALLBACK') AS fallback_alerts,
  COUNTIF(severity = 'CRITICAL') AS critical_count,
  COUNTIF(severity = 'HIGH') AS high_count,
  COUNTIF(severity = 'MEDIUM') AS medium_count,
  ROUND(AVG(fraud_score), 1) AS avg_fraud_score,
  MAX(fraud_score) AS max_fraud_score,
  MIN(detected_at) AS first_alert,
  MAX(detected_at) AS last_alert
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7
GROUP BY vendor_id
HAVING total_alerts >= 3  -- Only vendors with multiple alerts
ORDER BY avg_fraud_score DESC, total_alerts DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9️⃣  BRONZE → SILVER → GOLD FUNNEL — Data Quality Check
-- ─────────────────────────────────────────────────────────────────────────────
WITH funnel AS (
  -- Bronze ERP events
  SELECT
    'bronze_erp' AS stage,
    COUNT(*) AS event_count,
    COUNT(DISTINCT JSON_EXTRACT_SCALAR(payload, '$.invoice_id')) AS unique_invoices
  FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
  WHERE source_system = 'ERP'
    AND DATE(event_timestamp) >= CURRENT_DATE() - 7
  
  UNION ALL
  
  -- Silver deduplicated invoices
  SELECT
    'silver_invoices' AS stage,
    COUNT(*) AS event_count,
    COUNT(DISTINCT invoice_id) AS unique_invoices
  FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
  WHERE DATE(invoice_timestamp) >= CURRENT_DATE() - 7
  
  UNION ALL
  
  -- Gold fraud alerts
  SELECT
    'gold_alerts' AS stage,
    COUNT(*) AS event_count,
    COUNT(DISTINCT invoice_id) AS unique_invoices
  FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
  WHERE DATE(detected_at) >= CURRENT_DATE() - 7
)
SELECT
  stage,
  event_count,
  unique_invoices,
  ROUND(100.0 * unique_invoices / NULLIF(event_count, 0), 2) AS dedup_effectiveness_pct,
  LAG(event_count) OVER (ORDER BY 
    CASE stage
      WHEN 'bronze_erp' THEN 1
      WHEN 'silver_invoices' THEN 2
      WHEN 'gold_alerts' THEN 3
    END
  ) AS prev_stage_count,
  ROUND(100.0 * event_count / NULLIF(
    LAG(event_count) OVER (ORDER BY 
      CASE stage
        WHEN 'bronze_erp' THEN 1
        WHEN 'silver_invoices' THEN 2
        WHEN 'gold_alerts' THEN 3
      END
    ), 0), 2) AS conversion_pct
FROM funnel
ORDER BY
  CASE stage
    WHEN 'bronze_erp' THEN 1
    WHEN 'silver_invoices' THEN 2
    WHEN 'gold_alerts' THEN 3
  END;


-- ─────────────────────────────────────────────────────────────────────────────
-- 🔟  TIME-SERIES FRAUD PATTERN — Hourly Breakdown
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  TIMESTAMP_TRUNC(detected_at, HOUR) AS hour_bucket,
  rule_name,
  COUNT(*) AS alert_count,
  COUNT(DISTINCT vendor_id) AS vendor_count,
  ROUND(AVG(fraud_score), 1) AS avg_score,
  STRING_AGG(DISTINCT severity ORDER BY severity LIMIT 3) AS severities
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7
GROUP BY hour_bucket, rule_name
ORDER BY hour_bucket DESC, alert_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 💎  PORTFOLIO SHOWCASE QUERY — Single Comprehensive View
-- ─────────────────────────────────────────────────────────────────────────────
-- This is your "money shot" query for demos and interviews!
SELECT
  'SUMMARY' AS category,
  'Total Fraud Alerts' AS metric,
  CAST(COUNT(*) AS STRING) AS value
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7

UNION ALL

SELECT
  'SUMMARY',
  'Velocity Alerts',
  CAST(COUNTIF(rule_name = 'VELOCITY') AS STRING)
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7

UNION ALL

SELECT
  'SUMMARY',
  'Anomaly Alerts',
  CAST(COUNTIF(rule_name = 'ANOMALY') AS STRING)
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7

UNION ALL

SELECT
  'SUMMARY',
  'Critical Severity',
  CAST(COUNTIF(severity = 'CRITICAL') AS STRING)
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7

UNION ALL

SELECT
  'SUMMARY',
  'Avg Fraud Score',
  CAST(ROUND(AVG(fraud_score), 1) AS STRING)
FROM `${BQ_PROJECT}.${BQ_DATASET}.gold_fraud_alerts`
WHERE DATE(detected_at) >= CURRENT_DATE() - 7

UNION ALL

SELECT
  'SUMMARY',
  'Total Invoices Processed',
  CAST(COUNT(*) AS STRING)
FROM `${BQ_PROJECT}.${BQ_DATASET}.silver_deduplicated_invoices`
WHERE DATE(invoice_timestamp) >= CURRENT_DATE() - 7

UNION ALL

SELECT
  'SUMMARY',
  'Bronze Events Ingested',
  CAST(COUNT(*) AS STRING)
FROM `${BQ_PROJECT}.${BQ_DATASET}.bronze_raw_events`
WHERE DATE(event_timestamp) >= CURRENT_DATE() - 7

ORDER BY category, metric;
