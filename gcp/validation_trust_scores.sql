-- ═══════════════════════════════════════════════════════════════════════════
-- PHASE 2 VALIDATION QUERIES - Trust Score Engine & Vendor Risk Ranking
-- ═══════════════════════════════════════════════════════════════════════════
-- Purpose: Validate Phase 2 implementation is working correctly
-- Run after: Trust scores and rankings computed for first time
-- Expected: All queries return reasonable results (no errors, counts > 0)
-- ═══════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────
-- 1. ROW COUNT VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify tables are populated
-- Expected: All counts > 0

SELECT 'Row Counts' AS validation_category;

SELECT 
  'behavioral_features' AS table_name,
  COUNT(*) AS row_count,
  MIN(feature_date) AS earliest_date,
  MAX(feature_date) AS latest_date
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_behavioral_features`

UNION ALL

SELECT 
  'risk_features',
  COUNT(*),
  MIN(feature_date),
  MAX(feature_date)
FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_risk_features`

UNION ALL

SELECT 
  'trust_scores',
  COUNT(*),
  MIN(score_date),
  MAX(score_date)
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`

UNION ALL

SELECT 
  'vendor_rankings',
  COUNT(*),
  MIN(ranking_date),
  MAX(ranking_date)
FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`

ORDER BY table_name;

-- ───────────────────────────────────────────────────────────────────────────
-- 2. DATA QUALITY VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify data integrity (no NULLs where shouldn't be, valid ranges)
-- Expected: All counts = 0 (no violations)

SELECT 'Data Quality' AS validation_category;

-- Trust scores should be 0-100
SELECT 
  'trust_score_range_violation' AS violation_type,
  COUNT(*) AS violation_count
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE trust_score < 0 OR trust_score > 100

UNION ALL

-- Risk levels should be valid values
SELECT 
  'invalid_risk_level',
  COUNT(*)
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE risk_level NOT IN ('EXCELLENT', 'GOOD', 'MODERATE', 'HIGH')

UNION ALL

-- Alert counts should be non-negative
SELECT 
  'negative_alert_count',
  COUNT(*)
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE anomaly_alert_count < 0 
   OR velocity_alert_count < 0 
   OR duplicate_alert_count < 0

UNION ALL

-- Total penalty should match sum of individual penalties
SELECT 
  'penalty_mismatch',
  COUNT(*)
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE total_penalty != (behavioral_penalty + risk_penalty)

UNION ALL

-- Rankings should start at 1 and be contiguous
SELECT 
  'ranking_gap',
  COUNT(*)
FROM (
  SELECT ranking_date, risk_rank,
         LAG(risk_rank) OVER (PARTITION BY ranking_date ORDER BY risk_rank) AS prev_rank
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`
)
WHERE prev_rank IS NOT NULL AND (risk_rank - prev_rank) != 1

ORDER BY violation_type;

-- ───────────────────────────────────────────────────────────────────────────
-- 3. SCORE RANGE VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify trust scores distribute across expected range
-- Expected: Scores span from low to high, not all concentrated

SELECT 'Score Distribution' AS validation_category;

SELECT
  CASE
    WHEN trust_score >= 90 THEN '90-100 (EXCELLENT)'
    WHEN trust_score >= 70 THEN '70-89 (GOOD)'
    WHEN trust_score >= 50 THEN '50-69 (MODERATE)'
    ELSE '0-49 (HIGH)'
  END AS score_range,
  COUNT(*) AS vendor_count,
  ROUND(AVG(trust_score), 1) AS avg_score,
  MIN(trust_score) AS min_score,
  MAX(trust_score) AS max_score
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE score_date = (SELECT MAX(score_date) FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`)
GROUP BY score_range
ORDER BY min_score DESC;

-- ───────────────────────────────────────────────────────────────────────────
-- 4. RANKING VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify ranking logic works correctly
-- Expected: Rank 1 has lowest trust score, ranks increase monotonically

SELECT 'Ranking Logic' AS validation_category;

WITH latest_rankings AS (
  SELECT *
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`
  WHERE ranking_date = (SELECT MAX(ranking_date) FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`)
)

SELECT
  CASE
    WHEN risk_rank <= 10 THEN 'Top 10 (Highest Risk)'
    WHEN risk_rank <= 50 THEN 'Top 50'
    WHEN risk_rank <= 100 THEN 'Top 100'
    ELSE 'Beyond Top 100'
  END AS rank_tier,
  COUNT(*) AS vendor_count,
  MIN(trust_score) AS min_trust_score,
  MAX(trust_score) AS max_trust_score,
  ROUND(AVG(trust_score), 1) AS avg_trust_score
FROM latest_rankings
GROUP BY rank_tier
ORDER BY MIN(risk_rank);

-- ───────────────────────────────────────────────────────────────────────────
-- 5. HISTORICAL TREND VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify historical tracking works (scores change over time)
-- Expected: Some vendors show score changes

SELECT 'Historical Trends' AS validation_category;

WITH vendor_trends AS (
  SELECT
    vendor_id,
    MIN(score_date) AS first_date,
    MAX(score_date) AS last_date,
    COUNT(DISTINCT score_date) AS days_tracked,
    MIN(trust_score) AS min_score,
    MAX(trust_score) AS max_score,
    (MAX(trust_score) - MIN(trust_score)) AS score_range
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
  GROUP BY vendor_id
)

SELECT
  CASE
    WHEN score_range = 0 THEN 'No change (stable score)'
    WHEN score_range <= 10 THEN 'Minor change (≤10 points)'
    WHEN score_range <= 30 THEN 'Moderate change (11-30 points)'
    ELSE 'Major change (>30 points)'
  END AS trend_category,
  COUNT(*) AS vendor_count,
  ROUND(AVG(days_tracked), 1) AS avg_days_tracked,
  ROUND(AVG(score_range), 1) AS avg_score_range
FROM vendor_trends
GROUP BY trend_category
ORDER BY MIN(score_range);

-- ───────────────────────────────────────────────────────────────────────────
-- 6. EXPLAINABILITY VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify score_explanation JSON is populated and parseable
-- Expected: All explanations exist and contain expected fields

SELECT 'Explainability' AS validation_category;

SELECT
  COUNT(*) AS total_scores,
  COUNTIF(score_explanation IS NOT NULL) AS explanations_exist,
  COUNTIF(JSON_EXTRACT_SCALAR(score_explanation, '$.base_score') = '100') AS has_base_score,
  COUNTIF(JSON_EXTRACT_SCALAR(score_explanation, '$.final_score') IS NOT NULL) AS has_final_score,
  COUNTIF(JSON_EXTRACT_SCALAR(score_explanation, '$.risk_level') IS NOT NULL) AS has_risk_level,
  COUNTIF(JSON_EXTRACT_SCALAR(score_explanation, '$.reasoning') IS NOT NULL) AS has_reasoning
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE score_date = (SELECT MAX(score_date) FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`);

-- ───────────────────────────────────────────────────────────────────────────
-- 7. TOP RISKY VENDORS (Sample Output)
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Show example of Phase 2 output
-- Expected: List of vendors ranked by risk

SELECT 'Top 10 Riskiest Vendors' AS validation_category;

SELECT
  risk_rank,
  vendor_id,
  trust_score,
  risk_level,
  CASE
    WHEN rank_change IS NULL THEN 'NEW'
    WHEN rank_change < 0 THEN CONCAT('↑', ABS(rank_change))  -- Moved up in risk
    WHEN rank_change > 0 THEN CONCAT('↓', rank_change)       -- Moved down in risk
    ELSE '='
  END AS rank_movement
FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`
WHERE ranking_date = (SELECT MAX(ranking_date) FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`)
ORDER BY risk_rank ASC
LIMIT 10;

-- ───────────────────────────────────────────────────────────────────────────
-- 8. SCORE FORMULA VERIFICATION (Spot Check)
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Manually verify trust score calculation for sample vendors
-- Expected: trust_score = 100 - (anomaly*15 + velocity*10 + duplicate*5)

SELECT 'Score Formula Verification' AS validation_category;

SELECT
  vendor_id,
  trust_score,
  anomaly_alert_count,
  velocity_alert_count,
  duplicate_alert_count,
  
  -- Manual calculation
  (100 - (anomaly_alert_count * 15 + velocity_alert_count * 10 + duplicate_alert_count * 5)) AS calculated_score,
  
  -- Verification
  CASE
    WHEN trust_score = (100 - (anomaly_alert_count * 15 + velocity_alert_count * 10 + duplicate_alert_count * 5))
    THEN '✓ PASS'
    ELSE '✗ FAIL'
  END AS verification
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE score_date = (SELECT MAX(score_date) FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`)
ORDER BY RAND()  -- Random sample
LIMIT 20;

-- ───────────────────────────────────────────────────────────────────────────
-- 9. ML COMPATIBILITY VALIDATION
-- ───────────────────────────────────────────────────────────────────────────
-- Purpose: Verify schema is ready for future ML integration
-- Expected: ml_fraud_probability and ml_model_version columns exist (NULL for now)

SELECT 'ML Compatibility' AS validation_category;

SELECT
  COUNT(*) AS total_records,
  COUNTIF(ml_fraud_probability IS NULL) AS ml_prob_null_count,
  COUNTIF(ml_model_version IS NULL) AS ml_version_null_count,
  'Ready for Phase 3 ML integration' AS status
FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
WHERE score_date = (SELECT MAX(score_date) FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`);

-- ═══════════════════════════════════════════════════════════════════════════
-- VALIDATION SUMMARY
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '=== VALIDATION SUMMARY ===' AS summary;

SELECT
  'Phase 2 Implementation' AS component,
  CASE
    WHEN (SELECT COUNT(*) FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`) > 0
      AND (SELECT COUNT(*) FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`) > 0
      AND (SELECT COUNT(*) FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores` WHERE trust_score < 0 OR trust_score > 100) = 0
    THEN '✓ PASS - Trust Score Engine operational'
    ELSE '✗ FAIL - Review validation errors above'
  END AS status;
