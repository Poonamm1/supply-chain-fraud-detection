-- ═══════════════════════════════════════════════════════════════════════════
-- VENDOR RISK RANKINGS - BigQuery Scheduled Query
-- ═══════════════════════════════════════════════════════════════════════════
-- Purpose: Compute daily vendor risk rankings (leaderboard of risky vendors)
-- Schedule: Daily at 2:45 AM (after trust scores computed at 2:30 AM)
-- Source: vendor_trust_scores
-- Output: vendor_risk_rankings (historical table)
--
-- RANKING LOGIC:
--   - Rank 1 = LOWEST trust score (highest risk)
--   - Rank 2 = 2nd lowest trust score
--   - ...
--   - Rank N = HIGHEST trust score (lowest risk)
--
-- RANK CHANGE LOGIC:
--   - Compare today's rank vs yesterday's rank
--   - Positive change = moved UP in risk (worse)
--   - Negative change = moved DOWN in risk (better)
--   - NULL = new vendor (no historical rank)
--
-- USE CASES:
--   - \"Show me top 10 riskiest vendors today\"
--   - \"Which vendors moved up in risk rank this week?\"
--   - \"Alert me when vendor enters top 20 risk rank\"
--   - Executive dashboards (\"Top Risky Vendors\" widget)
--
-- ═══════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────
-- CONFIGURATION
-- ───────────────────────────────────────────────────────────────────────────

-- Target date (yesterday - process previous day's data)
DECLARE TARGET_DATE DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- Previous date (for rank change calculation)
DECLARE PREVIOUS_DATE DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAYS);

-- ───────────────────────────────────────────────────────────────────────────
-- COMPUTE VENDOR RISK RANKINGS
-- ───────────────────────────────────────────────────────────────────────────

INSERT INTO `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`
(
  ranking_date,
  vendor_id,
  trust_score,
  risk_rank,
  risk_level,
  rank_change,
  computed_at
)

WITH

-- ─── Step 1: Get today's trust scores ────
current_scores AS (
  SELECT
    vendor_id,
    score_date,
    trust_score,
    risk_level
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_trust_scores`
  WHERE score_date = TARGET_DATE
),

-- ─── Step 2: Assign risk rankings (lowest trust score = rank 1) ────
current_rankings AS (
  SELECT
    vendor_id,
    score_date AS ranking_date,
    trust_score,
    risk_level,
    
    -- Rank by trust score (ASC) - lowest score = highest risk = rank 1
    -- Tie-breaking: If scores equal, rank alphabetically by vendor_id
    ROW_NUMBER() OVER (
      ORDER BY trust_score ASC, vendor_id ASC
    ) AS risk_rank
    
  FROM current_scores
),

-- ─── Step 3: Get previous day's rankings (for rank change calculation) ────
previous_rankings AS (
  SELECT
    vendor_id,
    risk_rank AS prev_risk_rank
  FROM `fraud-detect-260526-1750.fraud_detection.vendor_risk_rankings`
  WHERE ranking_date = PREVIOUS_DATE
),

-- ─── Step 4: Calculate rank change (current rank - previous rank) ────
final_rankings AS (
  SELECT
    c.ranking_date,
    c.vendor_id,
    c.trust_score,
    c.risk_rank,
    c.risk_level,
    
    -- Rank change calculation:
    --   Positive change = moved UP in risk (worse)
    --   Example: Was rank 10, now rank 5 → change = -5 (moved up 5 positions in risk)
    --   Example: Was rank 5, now rank 10 → change = +5 (moved down 5 positions in risk)
    --   NULL = new vendor (no historical rank)
    CASE
      WHEN p.prev_risk_rank IS NULL THEN NULL  -- New vendor
      ELSE (c.risk_rank - p.prev_risk_rank)    -- Positive = moved down (less risky), Negative = moved up (more risky)
    END AS rank_change
    
  FROM current_rankings c
  LEFT JOIN previous_rankings p
    ON c.vendor_id = p.vendor_id
)

-- ───────────────────────────────────────────────────────────────────────────
-- OUTPUT
-- ───────────────────────────────────────────────────────────────────────────

SELECT
  ranking_date,
  vendor_id,
  trust_score,
  risk_rank,
  risk_level,
  rank_change,
  CURRENT_TIMESTAMP() AS computed_at
FROM final_rankings
ORDER BY risk_rank ASC  -- Rank 1 (highest risk) first
;

-- ═══════════════════════════════════════════════════════════════════════════
-- VALIDATION QUERIES (Run after scheduled query completes)
-- ═══════════════════════════════════════════════════════════════════════════

-- 1. Top 10 riskiest vendors (rank 1-10)
-- SELECT risk_rank, vendor_id, trust_score, risk_level
-- FROM vendor_risk_rankings
-- WHERE ranking_date = CURRENT_DATE() - 1
-- ORDER BY risk_rank ASC
-- LIMIT 10;

-- 2. Vendors that moved UP in risk (negative rank_change)
-- SELECT vendor_id, trust_score, risk_rank, rank_change,
--        CONCAT('Moved up ', ABS(rank_change), ' positions in risk') AS alert
-- FROM vendor_risk_rankings
-- WHERE ranking_date = CURRENT_DATE() - 1
--   AND rank_change < 0  -- Negative change = moved up in risk
-- ORDER BY rank_change ASC  -- Most dramatic increases first
-- LIMIT 20;

-- 3. Vendors that moved DOWN in risk (positive rank_change)
-- SELECT vendor_id, trust_score, risk_rank, rank_change,
--        CONCAT('Moved down ', rank_change, ' positions in risk') AS improvement
-- FROM vendor_risk_rankings
-- WHERE ranking_date = CURRENT_DATE() - 1
--   AND rank_change > 0  -- Positive change = moved down in risk
-- ORDER BY rank_change DESC  -- Most dramatic improvements first
-- LIMIT 20;

-- 4. New vendors entering rankings
-- SELECT vendor_id, trust_score, risk_rank, risk_level
-- FROM vendor_risk_rankings
-- WHERE ranking_date = CURRENT_DATE() - 1
--   AND rank_change IS NULL  -- NULL = new vendor
-- ORDER BY risk_rank ASC;

-- 5. Risk level distribution by rank tiers
-- SELECT 
--   CASE
--     WHEN risk_rank <= 10 THEN 'Top 10 (Highest Risk)'
--     WHEN risk_rank <= 50 THEN 'Top 50'
--     WHEN risk_rank <= 100 THEN 'Top 100'
--     ELSE 'Below Top 100'
--   END AS rank_tier,
--   COUNT(*) AS vendor_count,
--   COUNTIF(risk_level = 'HIGH') AS high_risk_count,
--   COUNTIF(risk_level = 'MODERATE') AS moderate_risk_count,
--   COUNTIF(risk_level = 'GOOD') AS good_risk_count,
--   COUNTIF(risk_level = 'EXCELLENT') AS excellent_risk_count
-- FROM vendor_risk_rankings
-- WHERE ranking_date = CURRENT_DATE() - 1
-- GROUP BY rank_tier
-- ORDER BY 
--   CASE rank_tier
--     WHEN 'Top 10 (Highest Risk)' THEN 1
--     WHEN 'Top 50' THEN 2
--     WHEN 'Top 100' THEN 3
--     ELSE 4
--   END;

-- 6. Vendors in top 20 risk rank for 7+ consecutive days
-- WITH daily_ranks AS (
--   SELECT vendor_id, ranking_date, risk_rank
--   FROM vendor_risk_rankings
--   WHERE ranking_date >= CURRENT_DATE() - 8
--     AND ranking_date < CURRENT_DATE()
-- ),
-- consistent_risk AS (
--   SELECT 
--     vendor_id,
--     COUNT(*) AS days_in_top_20
--   FROM daily_ranks
--   WHERE risk_rank <= 20
--   GROUP BY vendor_id
--   HAVING COUNT(*) >= 7
-- )
-- SELECT 
--   c.vendor_id,
--   c.days_in_top_20,
--   r.risk_rank AS current_rank,
--   r.trust_score AS current_trust_score
-- FROM consistent_risk c
-- JOIN vendor_risk_rankings r
--   ON c.vendor_id = r.vendor_id
--   AND r.ranking_date = CURRENT_DATE() - 1
-- ORDER BY c.days_in_top_20 DESC, r.risk_rank ASC;
