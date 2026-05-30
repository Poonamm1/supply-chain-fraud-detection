-- gcp/compute_rolling_features.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- ROLLING WINDOW FEATURES - BigQuery Scheduled Query
-- ═══════════════════════════════════════════════════════════════════════════
-- Purpose: Compute 7-day and 30-day rolling window features
-- Schedule: Daily at 2 AM (after Dataflow jobs complete)
-- Pattern: UPDATE existing rows with rolling aggregates
--
-- WHY BIGQUERY INSTEAD OF BEAM?
--   - Beam state for rolling windows is complex and memory-intensive
--   - BigQuery WINDOW functions are optimized and fast
--   - Scheduled queries are free (vs running Dataflow workers)
--   - Easier to maintain and debug
--
-- DEPLOYMENT:
--   bq query --use_legacy_sql=false --schedule='every day 02:00' \
--     --display_name='Compute Rolling Features' \
--     --target_dataset=fraud_detection \
--     < compute_rolling_features.sql
--
-- ═══════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────
-- STEP 1: Update Behavioral Features with Rolling Windows
-- ───────────────────────────────────────────────────────────────────────────
-- This query computes 7-day and 30-day rolling aggregates for behavioral features.
--
-- WINDOW FUNCTION EXPLANATION:
--   SUM(invoice_count) OVER (
--     PARTITION BY vendor_id
--     ORDER BY feature_date
--     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
--   )
--
--   Translation: For each vendor, sum invoice_count across:
--     - Current row (today)
--     - 6 preceding rows (last 6 days)
--     Total = 7 days of data
--
-- WHY ROWS vs RANGE?
--   ROWS BETWEEN: Counts physical rows (works even with gaps)
--   RANGE BETWEEN: Counts logical range (fails if missing days)
--   PRODUCTION: Use ROWS for robustness

MERGE `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features` T
USING (
    -- Compute rolling windows for all vendor-days
    SELECT 
        vendor_id,
        feature_date,
        
        -- 7-day rolling windows (current + 6 preceding days)
        SUM(invoice_count) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS invoice_count_7d,
        
        AVG(avg_invoice_amount) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_invoice_amount_7d,
        
        -- 30-day rolling windows (current + 29 preceding days)
        SUM(invoice_count) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS invoice_count_30d,
        
        AVG(avg_invoice_amount) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_invoice_amount_30d
        
    FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features`
    
    -- Only compute for recent data (optimization)
    -- Adjust window as needed (e.g., last 90 days)
    WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    
) S
ON T.vendor_id = S.vendor_id 
   AND T.feature_date = S.feature_date
WHEN MATCHED THEN
    UPDATE SET
        T.invoice_count_7d = S.invoice_count_7d,
        T.avg_invoice_amount_7d = S.avg_invoice_amount_7d,
        T.invoice_count_30d = S.invoice_count_30d,
        T.avg_invoice_amount_30d = S.avg_invoice_amount_30d;


-- ───────────────────────────────────────────────────────────────────────────
-- STEP 2: Update Risk Features with Rolling Windows
-- ───────────────────────────────────────────────────────────────────────────
-- Compute 30-day rolling alert counts for risk trajectory analysis.

MERGE `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features` T
USING (
    SELECT 
        vendor_id,
        feature_date,
        
        -- 30-day rolling alert counts
        SUM(anomaly_alert_count) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS anomaly_count_30d,
        
        SUM(velocity_alert_count) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS velocity_count_30d,
        
        SUM(total_alert_count) OVER (
            PARTITION BY vendor_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS total_alert_count_30d
        
    FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features`
    WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    
) S
ON T.vendor_id = S.vendor_id 
   AND T.feature_date = S.feature_date
WHEN MATCHED THEN
    UPDATE SET
        T.anomaly_count_30d = S.anomaly_count_30d,
        T.velocity_count_30d = S.velocity_count_30d,
        T.total_alert_count_30d = S.total_alert_count_30d;


-- ───────────────────────────────────────────────────────────────────────────
-- STEP 3: Compute high_risk_alert_ratio (requires join with behavioral features)
-- ───────────────────────────────────────────────────────────────────────────
-- This is the ONLY place where we join behavioral + risk features.
-- We do it here (in BigQuery) not in Beam, to keep Beam transforms independent.

MERGE `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features` T
USING (
    SELECT 
        r.vendor_id,
        r.feature_date,
        
        -- Compute high_risk_alert_ratio = total_alert_count / invoice_count
        -- SAFE_DIVIDE prevents division by zero (returns NULL)
        SAFE_DIVIDE(r.total_alert_count, b.invoice_count) AS high_risk_alert_ratio
        
    FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features` r
    INNER JOIN `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features` b
        ON r.vendor_id = b.vendor_id
        AND r.feature_date = b.feature_date
    
    WHERE r.feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    
) S
ON T.vendor_id = S.vendor_id 
   AND T.feature_date = S.feature_date
WHEN MATCHED THEN
    UPDATE SET
        T.high_risk_alert_ratio = S.high_risk_alert_ratio;


-- ═══════════════════════════════════════════════════════════════════════════
-- VALIDATION QUERIES (run after scheduled query completes)
-- ═══════════════════════════════════════════════════════════════════════════

-- Check rolling window coverage
SELECT 
    'behavioral_features' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(invoice_count_7d) AS rows_with_7d,
    COUNT(invoice_count_30d) AS rows_with_30d,
    ROUND(COUNT(invoice_count_7d) / COUNT(*) * 100, 2) AS pct_7d_coverage,
    ROUND(COUNT(invoice_count_30d) / COUNT(*) * 100, 2) AS pct_30d_coverage
FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features`
WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

UNION ALL

SELECT 
    'risk_features' AS table_name,
    COUNT(*) AS total_rows,
    NULL AS rows_with_7d,
    COUNT(anomaly_count_30d) AS rows_with_30d,
    NULL AS pct_7d_coverage,
    ROUND(COUNT(anomaly_count_30d) / COUNT(*) * 100, 2) AS pct_30d_coverage
FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features`
WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);


-- Check high_risk_alert_ratio computation
SELECT 
    vendor_id,
    feature_date,
    total_alert_count,
    high_risk_alert_ratio,
    CASE 
        WHEN high_risk_alert_ratio IS NULL THEN 'MISSING'
        WHEN high_risk_alert_ratio > 1.0 THEN 'INVALID (>1.0)'
        WHEN high_risk_alert_ratio < 0.0 THEN 'INVALID (<0.0)'
        ELSE 'VALID'
    END AS validation_status
FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_risk_features`
WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY validation_status DESC, high_risk_alert_ratio DESC
LIMIT 20;


-- Sample rolling window results
SELECT 
    vendor_id,
    feature_date,
    invoice_count AS today_count,
    invoice_count_7d AS rolling_7d,
    invoice_count_30d AS rolling_30d,
    ROUND(invoice_count / NULLIF(invoice_count_7d / 7.0, 0), 2) AS vs_7d_avg_ratio
FROM `${BQ_PROJECT}.${BQ_DATASET}.vendor_daily_behavioral_features`
WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND invoice_count_7d IS NOT NULL
ORDER BY vs_7d_avg_ratio DESC
LIMIT 10;


-- ═══════════════════════════════════════════════════════════════════════════
-- NOTES
-- ═══════════════════════════════════════════════════════════════════════════
--
-- COST OPTIMIZATION:
--   - WHERE clause limits to last 90 days (adjust as needed)
--   - Partitioning by feature_date reduces scan bytes
--   - Scheduled queries are free (no per-run charges)
--
-- PERFORMANCE:
--   - WINDOW functions are highly optimized in BigQuery
--   - Runs in ~1-2 minutes for millions of rows
--   - Much faster than Beam state + timers approach
--
-- IDEMPOTENCY:
--   - MERGE statement is idempotent (re-running produces same result)
--   - Safe to run multiple times per day
--
-- MONITORING:
--   - Check BigQuery scheduled query logs for failures
--   - Set up alerts if completion time > 5 minutes
--   - Validate rolling window coverage > 95%
--
-- FUTURE ENHANCEMENTS:
--   - Add more rolling windows (14d, 60d, 90d)
--   - Compute rolling stddev, percentiles
--   - Add year-over-year comparisons
--   - Seasonal adjustments (day of week, end of month)
-- ═══════════════════════════════════════════════════════════════════════════
