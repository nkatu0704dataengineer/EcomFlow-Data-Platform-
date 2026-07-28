
create or replace table ecomflow.ecom_gold.customer_reviews as 
WITH review_stats AS (
    SELECT 
        O.ACC_ID,
        COUNT(*) AS TOTAL_REVIEWS,
        AVG(R.review_score) AS AVG_REVIEW_SCORE,
        SUM(CASE WHEN R.review_score >= 4.0 THEN 1 ELSE 0 END) AS TOTAL_POSITIVE_REVIEWS,
        SUM(CASE WHEN R.review_score <= 2.5 THEN 1 ELSE 0 END) AS TOTAL_NEGATIVE_REVIEWS,
        SUM(CASE WHEN R.has_reply = true THEN 1 ELSE 0 END) AS TOTAL_REPLIES,
        SUM(CASE WHEN R.review_score > 2.5 AND R.review_score < 4.0 THEN 1 ELSE 0 END) AS TOTAL_NEUTRAL_REVIEWS
    FROM ecomflow.ecom_silver.orders AS O 
    JOIN ecomflow.ecom_silver.reviews AS R ON R.order_id = O.order_id
    GROUP BY O.ACC_ID
)
SELECT 
    ACC_ID,
    TOTAL_REVIEWS,
    ROUND(AVG_REVIEW_SCORE, 2) AS AVG_REVIEW_SCORE,
    TOTAL_POSITIVE_REVIEWS,
    TOTAL_NEGATIVE_REVIEWS,
    TOTAL_NEUTRAL_REVIEWS,
   
    ROUND(
        CASE 
            WHEN (TOTAL_POSITIVE_REVIEWS + TOTAL_NEGATIVE_REVIEWS) > 0 
            THEN TOTAL_POSITIVE_REVIEWS / (TOTAL_POSITIVE_REVIEWS + TOTAL_NEGATIVE_REVIEWS)
            ELSE 0
        END, 2
    ) AS POSITIVE_REVIEW_RATE,
    ROUND(
        CASE 
            WHEN TOTAL_REVIEWS > 0 
            THEN TOTAL_REPLIES / TOTAL_REVIEWS
            ELSE 0
        END, 2
    ) AS HAS_REPLY_RATE
FROM review_stats