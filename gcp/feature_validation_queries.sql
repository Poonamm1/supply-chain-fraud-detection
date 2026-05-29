-- gcp/feature_validation_queries.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- ML Feature Engineering Validation Queries
-- ═══════════════════════════════════════════════════════════════════════════
-- Purpose: Validate vendor_daily_features table after pipeline runs.
-- Usage:
--   bq query --use_legacy_sql=false < gcp/feature_validation_queries.sql
--   OR run each query individually for detailed inspection.
--
-- Expected Results:
--   After running batch or streaming pipeline with test data, you should see:
--   - Multiple vendor-day combinations
--   - Invoice counts matching silver layer
--   - Alert counts matching gold layer
--   - Feature values in reasonable ranges
-- ═══════════════════════════════════════════════════════════════════════════

-- Replace with your project:dataset
-- Example: fraud-detect-260526-1750:fraud_detection
SET PROJECT_DATASET = 'fraud-detect-260526-1750:fraud_detection';

-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 1: Feature Table Overview
-- Shows: How many vendor-days have features computed
-- Expected: > 0 rows after pipeline run
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    COUNT(DISTINCT vendor_id) AS unique_vendors,
    COUNT(DISTINCT feature_date) AS unique_dates,
    COUNT(*) AS total_vendor_days,
    MIN(feature_date) AS earliest_feature_date,
    MAX(feature_date) AS latest_feature_date
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 2: Feature Distribution - Invoice Volume
-- Shows: Distribution of invoice counts per vendor-day
-- ML Insight: Understand normal vendor activity levels vs outliers
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    vendor_id,
    feature_date,
    invoice_count,
    total_invoice_amount,
    avg_invoice_amount,
    stddev_invoice_amount,
    -- Flag high-volume days (potential bot flooding)
    CASE 
        WHEN invoice_count > 50 THEN 'HIGH_VOLUME'
        WHEN invoice_count > 20 THEN 'MEDIUM_VOLUME'
        ELSE 'NORMAL_VOLUME'
    END AS volume_category
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
ORDER BY invoice_count DESC
LIMIT 20;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 3: Feature Distribution - Alert Patterns
-- Shows: Vendors with alerts and their risk profiles
-- ML Insight: These are the "labeled" examples (fraud = True)
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    vendor_id,
    feature_date,
    total_alert_count,
    anomaly_alert_count,
    velocity_alert_count,
    high_risk_alert_ratio,
    invoice_count,
    -- Risk categorization (future ML label)
    CASE 
        WHEN high_risk_alert_ratio > 0.5 THEN 'CRITICAL'
        WHEN high_risk_alert_ratio > 0.2 THEN 'HIGH'
        WHEN high_risk_alert_ratio > 0 THEN 'MEDIUM'
        ELSE 'CLEAN'
    END AS risk_level
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
WHERE total_alert_count > 0
ORDER BY high_risk_alert_ratio DESC, total_alert_count DESC
LIMIT 20;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 4: Feature Distribution - Invoice Amount Statistics
-- Shows: Invoice amount patterns (distribution shape features)
-- ML Insight: High stddev = erratic behavior, low = routine vendor
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    vendor_id,
    feature_date,
    invoice_count,
    avg_invoice_amount,
    stddev_invoice_amount,
    min_invoice_amount,
    max_invoice_amount,
    -- Coefficient of variation (volatility measure)
    ROUND(SAFE_DIVIDE(stddev_invoice_amount, avg_invoice_amount), 4) AS cv,
    -- Flag suspicious patterns
    CASE 
        WHEN max_invoice_amount > (avg_invoice_amount * 5) THEN 'OUTLIER_HIGH'
        WHEN min_invoice_amount < (avg_invoice_amount * 0.1) THEN 'OUTLIER_LOW'
        WHEN stddev_invoice_amount > (avg_invoice_amount * 0.8) THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END AS amount_pattern
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
WHERE invoice_count >= 5  -- Only vendors with enough data
ORDER BY stddev_invoice_amount DESC
LIMIT 20;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 5: Feature Distribution - Temporal Patterns
-- Shows: Invoice submission rates (bot detection features)
-- ML Insight: Very high rates = automated bot, very low = manual entry
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    vendor_id,
    feature_date,
    invoice_count,
    avg_invoices_per_hour,
    latest_invoice_timestamp,
    -- Classify submission patterns
    CASE 
        WHEN avg_invoices_per_hour > 10 THEN 'AUTOMATED_BOT'
        WHEN avg_invoices_per_hour > 5 THEN 'SEMI_AUTOMATED'
        WHEN avg_invoices_per_hour > 0 THEN 'MANUAL_ENTRY'
        ELSE 'SINGLE_SUBMISSION'
    END AS submission_pattern
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
WHERE invoice_count > 1
ORDER BY avg_invoices_per_hour DESC
LIMIT 20;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 6: Data Quality Check - Feature Completeness
-- Shows: Missing or NULL features (data quality validation)
-- Expected: All features should be populated for valid vendor-days
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN invoice_count IS NULL THEN 1 END) AS null_invoice_count,
    COUNT(CASE WHEN avg_invoice_amount IS NULL THEN 1 END) AS null_avg_amount,
    COUNT(CASE WHEN stddev_invoice_amount IS NULL THEN 1 END) AS null_stddev,
    COUNT(CASE WHEN high_risk_alert_ratio IS NULL THEN 1 END) AS null_risk_ratio,
    -- Percentage of complete rows
    ROUND(
        SAFE_DIVIDE(
            COUNT(CASE 
                WHEN invoice_count IS NOT NULL 
                AND avg_invoice_amount IS NOT NULL 
                AND high_risk_alert_ratio IS NOT NULL 
                THEN 1 
            END),
            COUNT(*)
        ) * 100, 
        2
    ) AS pct_complete
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 7: Cross-Layer Validation - Invoice Count Match
-- Shows: Verify feature counts match silver layer (referential integrity)
-- Expected: Counts should match exactly (within same date partition)
-- ───────────────────────────────────────────────────────────────────────────
WITH silver_counts AS (
    SELECT 
        vendor_id,
        DATE(invoice_timestamp) AS invoice_date,
        COUNT(*) AS silver_invoice_count,
        SUM(invoice_amount) AS silver_total_amount
    FROM `fraud-detect-260526-1750.fraud_detection.silver_deduplicated_invoices`
    WHERE DATE(invoice_timestamp) = CURRENT_DATE()  -- Today only
    GROUP BY vendor_id, DATE(invoice_timestamp)
),
feature_counts AS (
    SELECT 
        vendor_id,
        feature_date,
        invoice_count AS feature_invoice_count,
        total_invoice_amount AS feature_total_amount
    FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
    WHERE feature_date = CURRENT_DATE()
)
SELECT 
    COALESCE(s.vendor_id, f.vendor_id) AS vendor_id,
    COALESCE(s.invoice_date, f.feature_date) AS date,
    s.silver_invoice_count,
    f.feature_invoice_count,
    s.silver_total_amount,
    f.feature_total_amount,
    -- Flag mismatches (data quality issue)
    CASE 
        WHEN s.silver_invoice_count != f.feature_invoice_count THEN 'MISMATCH'
        WHEN s.silver_invoice_count IS NULL THEN 'MISSING_SILVER'
        WHEN f.feature_invoice_count IS NULL THEN 'MISSING_FEATURE'
        ELSE 'MATCH'
    END AS validation_status
FROM silver_counts s
FULL OUTER JOIN feature_counts f
    ON s.vendor_id = f.vendor_id 
    AND s.invoice_date = f.feature_date
ORDER BY validation_status DESC, vendor_id;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 8: Cross-Layer Validation - Alert Count Match
-- Shows: Verify alert counts match gold layer
-- Expected: Alert counts should match exactly (within same date partition)
-- ───────────────────────────────────────────────────────────────────────────
WITH gold_counts AS (
    SELECT 
        vendor_id,
        DATE(detected_at) AS alert_date,
        COUNT(*) AS gold_total_alerts,
        SUM(CASE WHEN rule_name = 'ANOMALY' THEN 1 ELSE 0 END) AS gold_anomaly_count,
        SUM(CASE WHEN rule_name = 'VELOCITY' THEN 1 ELSE 0 END) AS gold_velocity_count
    FROM `fraud-detect-260526-1750.fraud_detection.gold_fraud_alerts`
    WHERE DATE(detected_at) = CURRENT_DATE()
    GROUP BY vendor_id, DATE(detected_at)
),
feature_counts AS (
    SELECT 
        vendor_id,
        feature_date,
        total_alert_count AS feature_total_alerts,
        anomaly_alert_count AS feature_anomaly_count,
        velocity_alert_count AS feature_velocity_count
    FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
    WHERE feature_date = CURRENT_DATE()
)
SELECT 
    COALESCE(g.vendor_id, f.vendor_id) AS vendor_id,
    COALESCE(g.alert_date, f.feature_date) AS date,
    g.gold_total_alerts,
    f.feature_total_alerts,
    g.gold_anomaly_count,
    f.feature_anomaly_count,
    g.gold_velocity_count,
    f.feature_velocity_count,
    -- Flag mismatches
    CASE 
        WHEN g.gold_total_alerts != f.feature_total_alerts THEN 'MISMATCH'
        WHEN g.gold_total_alerts IS NULL AND f.feature_total_alerts > 0 THEN 'ORPHAN_FEATURE'
        WHEN f.feature_total_alerts IS NULL AND g.gold_total_alerts > 0 THEN 'MISSING_FEATURE'
        ELSE 'MATCH'
    END AS validation_status
FROM gold_counts g
FULL OUTER JOIN feature_counts f
    ON g.vendor_id = f.vendor_id 
    AND g.alert_date = f.feature_date
ORDER BY validation_status DESC, vendor_id;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 9: Time-Series View - Vendor Behavior Over Time
-- Shows: How vendor features change day-to-day (time-series readiness)
-- ML Insight: LSTM/Transformer models will use this time-series data
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    vendor_id,
    feature_date,
    invoice_count,
    total_invoice_amount,
    total_alert_count,
    high_risk_alert_ratio,
    -- Daily change in invoice count
    invoice_count - LAG(invoice_count) OVER (
        PARTITION BY vendor_id 
        ORDER BY feature_date
    ) AS daily_invoice_change,
    -- Daily change in alert ratio (behavior shift detection)
    ROUND(
        high_risk_alert_ratio - LAG(high_risk_alert_ratio) OVER (
            PARTITION BY vendor_id 
            ORDER BY feature_date
        ),
        4
    ) AS daily_risk_change
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
WHERE vendor_id IN (
    -- Top 5 most active vendors
    SELECT vendor_id 
    FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`
    GROUP BY vendor_id 
    ORDER BY SUM(invoice_count) DESC 
    LIMIT 5
)
ORDER BY vendor_id, feature_date DESC;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 10: ML Readiness Check - Feature Statistics Summary
-- Shows: Overall feature statistics for ML model training assessment
-- Purpose: Understand feature distributions before feeding to Vertex AI
-- ───────────────────────────────────────────────────────────────────────────
SELECT 
    'invoice_count' AS feature_name,
    AVG(invoice_count) AS mean_value,
    STDDEV(invoice_count) AS stddev_value,
    MIN(invoice_count) AS min_value,
    MAX(invoice_count) AS max_value,
    APPROX_QUANTILES(invoice_count, 100)[OFFSET(50)] AS median_value
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`

UNION ALL

SELECT 
    'avg_invoice_amount' AS feature_name,
    AVG(avg_invoice_amount) AS mean_value,
    STDDEV(avg_invoice_amount) AS stddev_value,
    MIN(avg_invoice_amount) AS min_value,
    MAX(avg_invoice_amount) AS max_value,
    APPROX_QUANTILES(avg_invoice_amount, 100)[OFFSET(50)] AS median_value
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`

UNION ALL

SELECT 
    'stddev_invoice_amount' AS feature_name,
    AVG(stddev_invoice_amount) AS mean_value,
    STDDEV(stddev_invoice_amount) AS stddev_value,
    MIN(stddev_invoice_amount) AS min_value,
    MAX(stddev_invoice_amount) AS max_value,
    APPROX_QUANTILES(stddev_invoice_amount, 100)[OFFSET(50)] AS median_value
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`

UNION ALL

SELECT 
    'high_risk_alert_ratio' AS feature_name,
    AVG(high_risk_alert_ratio) AS mean_value,
    STDDEV(high_risk_alert_ratio) AS stddev_value,
    MIN(high_risk_alert_ratio) AS min_value,
    MAX(high_risk_alert_ratio) AS max_value,
    APPROX_QUANTILES(high_risk_alert_ratio, 100)[OFFSET(50)] AS median_value
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`

UNION ALL

SELECT 
    'avg_invoices_per_hour' AS feature_name,
    AVG(avg_invoices_per_hour) AS mean_value,
    STDDEV(avg_invoices_per_hour) AS stddev_value,
    MIN(avg_invoices_per_hour) AS min_value,
    MAX(avg_invoices_per_hour) AS max_value,
    APPROX_QUANTILES(avg_invoices_per_hour, 100)[OFFSET(50)] AS median_value
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_features`

ORDER BY feature_name;


-- ═══════════════════════════════════════════════════════════════════════════
-- SUMMARY INTERPRETATION GUIDE
-- ═══════════════════════════════════════════════════════════════════════════
--
-- QUERY 1: Should show > 0 vendor-days. If 0, pipeline didn't run or failed.
--
-- QUERY 2: Volume distribution shows normal vendor activity baseline.
--          HIGH_VOLUME vendors are candidates for bot detection.
--
-- QUERY 3: Vendors with alerts are your labeled fraud examples.
--          CRITICAL risk_level = high fraud probability.
--
-- QUERY 4: High stddev vendors have erratic behavior (higher risk).
--          OUTLIER flags indicate suspicious invoice patterns.
--
-- QUERY 5: AUTOMATED_BOT pattern = possible flooding attack.
--          Human vendors show MANUAL_ENTRY or SEMI_AUTOMATED.
--
-- QUERY 6: pct_complete should be ~100%. NULL values indicate data issues.
--
-- QUERY 7: MATCH status confirms feature layer correctly aggregates silver.
--          MISMATCH = bug in feature engineering logic.
--
-- QUERY 8: MATCH status confirms alert counts are accurate.
--          MISMATCH = bug in alert counting logic.
--
-- QUERY 9: Time-series view for LSTM model training readiness.
--          Large daily_risk_change = sudden behavior shift (fraud signal).
--
-- QUERY 10: Feature statistics for ML normalization/scaling decisions.
--           Wide range (max >> min) may need log transformation.
--
-- NEXT STEPS FOR ML:
--   1. Export vendor_daily_features to CSV for Vertex AI AutoML Tables
--   2. Use high_risk_alert_ratio > 0 as fraud label (binary classification)
--   3. Train model to predict fraud probability from volume/temporal features
--   4. Deploy model to Vertex AI Endpoint for real-time scoring
--   5. Integrate predictions back into pipeline as new alert rule
-- ═══════════════════════════════════════════════════════════════════════════
