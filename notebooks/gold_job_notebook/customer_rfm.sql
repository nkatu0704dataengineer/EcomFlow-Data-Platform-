
-- ================================================================================
-- TABLE: ecomflow.ecom_gold.customer_rfm
-- PURPOSE: CRM-Oriented Customer Segmentation using RFM Analysis
-- GRAIN: 1 row = 1 customer (ACC_ID)
-- ANALYSIS DATE: 2023-12-29
-- ================================================================================
-- BUSINESS OBJECTIVE:
-- - Identify most valuable customers (Champions)
-- - Detect at-risk customers for retention campaigns
-- - Segment customers for targeted marketing
-- - Prioritize VIP treatment and resources
-- ================================================================================



-- Step 1: Calculate base RFM metrics from customer_360
WITH rfm_base AS (
  SELECT 
    c.ACC_ID,
    CONCAT(c.FIRST_NAME, ' ', c.LAST_NAME) AS customer_name,
    
    -- RECENCY: Days since last order (as of 2023-12-29)
    -- Lower days = more recent = better customer engagement
    DATEDIFF(DATE('2023-12-29'), DATE(c.last_order)) AS recency_days,
    
    -- FREQUENCY: Total number of orders
    -- More orders = more engaged customer
    c.total_orders AS frequency_orders,
    
    -- MONETARY: Total customer lifetime spending
    -- Higher spending = more valuable customer
    c.total_spent AS monetary_value,
    
    -- Bonus metrics for enriched analysis
    c.avg_order_value,
    c.customer_rank,
    DATE(c.last_order) AS last_order_date,
    
    -- Join review scores from customer_reviews table
    COALESCE(r.AVG_REVIEW_SCORE, 0) AS avg_review_score
    
  FROM ecomflow.ecom_gold.customer_360 AS c
  LEFT JOIN ecomflow.ecom_gold.customer_reviews AS r 
    ON c.ACC_ID = r.ACC_ID
  WHERE c.total_orders > 0  -- Only customers who have placed orders
),

-- Step 2: Calculate RFM scores using NTILE(5) for quintile ranking
-- Score range: 1 (worst) to 5 (best)
rfm_scored AS (
  SELECT 
    *,
    
    -- R_SCORE: Recency score (5 = most recent, 1 = least recent)
    -- We use DESC ordering so recent customers get higher scores
    NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,
    
    -- F_SCORE: Frequency score (5 = most orders, 1 = fewest orders)
    NTILE(5) OVER (ORDER BY frequency_orders ASC) AS f_score,
    
    -- M_SCORE: Monetary score (5 = highest spending, 1 = lowest spending)
    NTILE(5) OVER (ORDER BY monetary_value ASC) AS m_score
    
  FROM rfm_base
),

-- Step 3: Create combined RFM score and classify customer segments
rfm_final AS (
  SELECT 
    *,
    
    -- Concatenate R-F-M scores into single identifier (e.g., '555', '432', '111')
    CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_score,
    
    -- Business-friendly customer segmentation based on RFM patterns
    CASE 
      -- CHAMPIONS: Best customers (High R, High F, High M)
      WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
      
      -- LOYAL CUSTOMERS: Regular buyers with high spending (High F, High M)
      WHEN f_score >= 4 AND m_score >= 4 THEN 'Loyal Customers'
      
      -- POTENTIAL LOYALISTS: Recent customers with moderate activity
      WHEN r_score >= 4 AND f_score >= 3 THEN 'Potential Loyalists'
      
      -- BIG SPENDERS: High monetary value but lower frequency
      WHEN m_score >= 4 AND f_score <= 3 THEN 'Big Spenders'
      
      -- NEW CUSTOMERS: Very recent but few orders
      WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
      
      -- PROMISING: Recently active, growing potential
      WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 2 THEN 'Promising'
      
      -- NEED ATTENTION: Declining activity, requires engagement
      WHEN r_score >= 3 AND f_score >= 3 THEN 'Need Attention'
      
      -- AT RISK: Previously valuable, now inactive (Low R, High F/M)
      WHEN r_score <= 2 AND (f_score >= 3 OR m_score >= 3) THEN 'At Risk'
      
      -- CANNOT LOSE THEM: High value but becoming inactive (urgent intervention)
      WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cannot Lose Them'
      
      -- HIBERNATING: Long time since purchase, moderate history
      WHEN r_score <= 2 AND f_score >= 2 AND m_score >= 2 THEN 'Hibernating'
      
      -- LOST: Inactive with low engagement (Low R, Low F, Low M)
      WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
      
      ELSE 'Others'
    END AS customer_segment
    
  FROM rfm_scored
)

-- Step 4: Final output with all metrics
SELECT 
  acc_id,
  customer_name,
  
  -- Core RFM Metrics
  recency_days,
  frequency_orders,
  ROUND(monetary_value, 2) AS monetary_value,
  
  -- RFM Scores (1-5 scale)
  r_score,
  f_score,
  m_score,
  rfm_score,
  
  -- Customer Segment Classification
  customer_segment,
  
  -- Bonus Metrics
  ROUND(avg_order_value, 2) AS avg_order_value,
  ROUND(avg_review_score, 2) AS avg_review_score,
  customer_rank,
  last_order_date,
  
  -- Analysis metadata
  DATE('2023-12-29') AS analysis_date
  
FROM rfm_final
ORDER BY 
  r_score DESC,
  f_score DESC,
  m_score DESC