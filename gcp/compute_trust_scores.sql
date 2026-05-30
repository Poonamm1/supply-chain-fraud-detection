-- ═══════════════════════════════════════════════════════════════════════════
-- TRUST SCORE ENGINE - BigQuery Scheduled Query
-- ═══════════════════════════════════════════════════════════════════════════
-- Purpose: Compute daily trust scores for all vendors
-- Schedule: Daily at 2:30 AM (after rolling features computed at 2:00 AM)
-- Source: vendor_daily_behavioral_features + vendor_daily_risk_features
-- Output: vendor_trust_scores (historical table)
--
-- TRUST SCORE FORMULA (v1.0 - Rule-Based):
--   penalty = (anomaly_count * ANOMALY_WEIGHT) +
--             (velocity_count * VELOCITY_WEIGHT) +
--             (duplicate_count * DUPLICATE_WEIGHT)
--   trust_score = MAX(0, 100 - penalty)
--
-- WEIGHTS (v1.0):
--   ANOMALY_WEIGHT = 15   (highest severity - statistical deviation)
--   VELOCITY_WEIGHT = 10  (medium severity - rapid activity)
--   DUPLICATE_WEIGHT = 5  (lowest severity - operational error)
--
-- RISK LEVELS:
--   EXCELLENT: >= 90  (Top 10%, minimal fraud risk)
--   GOOD:      >= 70  (70th-90th percentile, low fraud risk)
--   MODERATE:  >= 50  (50th-70th percentile, medium fraud risk)
--   HIGH:      < 50   (Bottom 50%, high fraud risk)
--
-- EXPLAINABILITY:
--   score_explanation JSON contains full breakdown:
--     - base_score: 100
--     - penalties: {anomaly: X, velocity: Y, duplicate: Z}
--     - total_penalty: X+Y+Z
--     - final_score: 100 - (X+Y+Z)
--     - risk_level: "HIGH" (with reasoning)
--
-- ML EVOLUTION:
--   Phase 3+: Add ml_fraud_probability column (logistic regression, XGBoost)
--   Schema unchanged (trust_score remains for backward compatibility)
-- ═══════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────
-- CONFIGURATION
-- ───────────────────────────────────────────────────────────────────────────

-- Trust score weights (v1.0 - tune based on business feedback)
DECLARE ANOMALY_WEIGHT INT64 DEFAULT 15;
DECLARE VELOCITY_WEIGHT INT64 DEFAULT 10;
DECLARE DUPLICATE_WEIGHT INT64 DEFAULT 5;

-- Risk level thresholds (v1.0 - tune based on percentile analysis)
DECLARE EXCELLENT_THRESHOLD INT64 DEFAULT 90;
DECLARE GOOD_THRESHOLD INT64 DEFAULT 70;
DECLARE MODERATE_THRESHOLD INT64 DEFAULT 50;

-- Target date (yesterday - process previous day's data)
DECLARE TARGET_DATE DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- ───────────────────────────────────────────────────────────────────────────
-- COMPUTE TRUST SCORES
-- ───────────────────────────────────────────────────────────────────────────

INSERT INTO `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
(
  vendor_id,
  score_date,
  trust_score,
  risk_level,
  behavioral_penalty,
  risk_penalty,
  total_penalty,
  anomaly_alert_count,
  velocity_alert_count,
  duplicate_alert_count,
  total_alert_count,
  score_explanation,
  ml_fraud_probability,
  ml_model_version,
  computed_at
)

WITH 

-- ─── Step 1: Get all vendors with behavioral features (ALL vendors) ────
behavioral AS (
  SELECT
    vendor_id,
    feature_date,
    invoice_count,
    total_invoice_amount,
    avg_invoice_amount,
    stddev_invoice_amount,
    invoice_count_7d,
    invoice_count_30d
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_behavioral_features`
  WHERE feature_date = TARGET_DATE
),

-- ─── Step 2: Get vendors with risk features (SPARSE - only vendors with alerts) ────
risk AS (
  SELECT
    vendor_id,
    feature_date,
    anomaly_alert_count,
    velocity_alert_count,
    duplicate_alert_count,
    total_alert_count
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_daily_risk_features`
  WHERE feature_date = TARGET_DATE
),

-- ─── Step 3: Join behavioral + risk (LEFT JOIN to include clean vendors) ────
features AS (
  SELECT
    b.vendor_id,
    b.feature_date,
    
    -- Behavioral features
    b.invoice_count,
    b.total_invoice_amount,
    b.avg_invoice_amount,
    b.stddev_invoice_amount,
    b.invoice_count_7d,
    b.invoice_count_30d,
    
    -- Risk features (NULL for clean vendors → COALESCE to 0)
    COALESCE(r.anomaly_alert_count, 0) AS anomaly_alert_count,
    COALESCE(r.velocity_alert_count, 0) AS velocity_alert_count,
    COALESCE(r.duplicate_alert_count, 0) AS duplicate_alert_count,
    COALESCE(r.total_alert_count, 0) AS total_alert_count
    
  FROM behavioral b
  LEFT JOIN risk r
    ON b.vendor_id = r.vendor_id
    AND b.feature_date = r.feature_date
),

-- ─── Step 4: Compute trust scores with penalty breakdown ────
trust_scores AS (
  SELECT
    vendor_id,
    feature_date AS score_date,
    
    -- Alert counts
    anomaly_alert_count,
    velocity_alert_count,
    duplicate_alert_count,
    total_alert_count,
    
    -- Penalty breakdown
    0 AS behavioral_penalty,  -- Future: Add penalties from behavioral features
    (anomaly_alert_count * ANOMALY_WEIGHT +
     velocity_alert_count * VELOCITY_WEIGHT +
     duplicate_alert_count * DUPLICATE_WEIGHT) AS risk_penalty,
    
    (anomaly_alert_count * ANOMALY_WEIGHT +
     velocity_alert_count * VELOCITY_WEIGHT +
     duplicate_alert_count * DUPLICATE_WEIGHT) AS total_penalty,
    
    -- Trust score (0-100, higher = more trustworthy)
    GREATEST(0, 100 - (
      anomaly_alert_count * ANOMALY_WEIGHT +
      velocity_alert_count * VELOCITY_WEIGHT +
      duplicate_alert_count * DUPLICATE_WEIGHT
    )) AS trust_score,
    
    -- Behavioral features (for explanation)
    invoice_count,
    total_invoice_amount,
    avg_invoice_amount
    
  FROM features
),

-- ─── Step 5: Assign risk levels based on trust score ────
risk_levels AS (
  SELECT
    *,
    CASE
      WHEN trust_score >= EXCELLENT_THRESHOLD THEN 'EXCELLENT'
      WHEN trust_score >= GOOD_THRESHOLD THEN 'GOOD'
      WHEN trust_score >= MODERATE_THRESHOLD THEN 'MODERATE'
      ELSE 'HIGH'
    END AS risk_level
  FROM trust_scores
),

-- ─── Step 6: Build explainability JSON ────
final_scores AS (
  SELECT
    vendor_id,
    score_date,
    trust_score,
    risk_level,
    behavioral_penalty,
    risk_penalty,
    total_penalty,
    anomaly_alert_count,
    velocity_alert_count,
    duplicate_alert_count,
    total_alert_count,
    
    -- Explainability JSON
    TO_JSON(STRUCT(
      100 AS base_score,
      STRUCT(
        anomaly_alert_count AS count,
        ANOMALY_WEIGHT AS weight,
        (anomaly_alert_count * ANOMALY_WEIGHT) AS penalty
      ) AS anomaly_penalty,
      STRUCT(
        velocity_alert_count AS count,
        VELOCITY_WEIGHT AS weight,
        (velocity_alert_count * VELOCITY_WEIGHT) AS penalty
      ) AS velocity_penalty,
      STRUCT(
        duplicate_alert_count AS count,
        DUPLICATE_WEIGHT AS weight,
        (duplicate_alert_count * DUPLICATE_WEIGHT) AS penalty
      ) AS duplicate_penalty,
      total_penalty AS total_penalty,
      trust_score AS final_score,
      risk_level AS risk_level,
      CASE
        WHEN risk_level = 'EXCELLENT' THEN 'Minimal fraud risk - vendor in top 10%'
        WHEN risk_level = 'GOOD' THEN 'Low fraud risk - vendor performing well'
        WHEN risk_level = 'MODERATE' THEN 'Medium fraud risk - enhanced monitoring recommended'
        ELSE 'High fraud risk - immediate investigation required'
      END AS reasoning,
      STRUCT(
        invoice_count AS daily_invoice_count,
        ROUND(total_invoice_amount, 2) AS daily_total_amount,
        ROUND(avg_invoice_amount, 2) AS avg_invoice_amount
      ) AS behavioral_context
    )) AS score_explanation,
    
    -- Future ML (Phase 3+)
    CAST(NULL AS FLOAT64) AS ml_fraud_probability,
    CAST(NULL AS STRING) AS ml_model_version,
    
    CURRENT_TIMESTAMP() AS computed_at
    
  FROM risk_levels
)

-- ───────────────────────────────────────────────────────────────────────────
-- OUTPUT
-- ───────────────────────────────────────────────────────────────────────────

SELECT
  vendor_id,
  score_date,
  trust_score,
  risk_level,
  behavioral_penalty,
  risk_penalty,
  total_penalty,
  anomaly_alert_count,
  velocity_alert_count,
  duplicate_alert_count,
  total_alert_count,
  score_explanation,
  ml_fraud_probability,
  ml_model_version,
  computed_at
FROM final_scores
ORDER BY trust_score ASC, vendor_id  -- Lowest trust score first (highest risk)
;

-- ═══════════════════════════════════════════════════════════════════════════
-- VALIDATION QUERIES (Run after scheduled query completes)
-- ═══════════════════════════════════════════════════════════════════════════

-- 1. Count vendors processed
-- SELECT COUNT(*), MIN(trust_score), MAX(trust_score), AVG(trust_score)
-- FROM vendor_trust_scores
-- WHERE score_date = CURRENT_DATE() - 1;

-- 2. Risk level distribution
-- SELECT risk_level, COUNT(*) AS vendor_count
-- FROM vendor_trust_scores
-- WHERE score_date = CURRENT_DATE() - 1
-- GROUP BY risk_level
-- ORDER BY vendor_count DESC;

-- 3. Top 10 riskiest vendors (lowest trust scores)
-- SELECT vendor_id, trust_score, risk_level, total_alert_count
-- FROM vendor_trust_scores
-- WHERE score_date = CURRENT_DATE() - 1
-- ORDER BY trust_score ASC
-- LIMIT 10;

-- 4. Vendors with score degradation (trust score dropped >20 points in 7 days)
-- WITH current AS (
--   SELECT vendor_id, trust_score
--   FROM vendor_trust_scores
--   WHERE score_date = CURRENT_DATE() - 1
-- ),
-- previous AS (
--   SELECT vendor_id, trust_score AS prev_trust_score
--   FROM vendor_trust_scores
--   WHERE score_date = CURRENT_DATE() - 8
-- )
-- SELECT c.vendor_id, c.trust_score, p.prev_trust_score, (c.trust_score - p.prev_trust_score) AS score_change
-- FROM current c
-- JOIN previous p USING (vendor_id)
-- WHERE (c.trust_score - p.prev_trust_score) < -20
-- ORDER BY score_change ASC;

-- 5. View explainability for specific vendor
-- SELECT vendor_id, trust_score, risk_level, score_explanation
-- FROM vendor_trust_scores
-- WHERE score_date = CURRENT_DATE() - 1
--   AND vendor_id = 'V-1001';
