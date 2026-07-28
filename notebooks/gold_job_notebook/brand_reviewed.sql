

WITH BRAND_REVIEWS AS (
  SELECT 
    P.brand_id,
    COUNT(*) AS TOTAL_REVIEWS,
    AVG(R.review_score) AS AVG_REVIEW_SCORE,
    COUNT(CASE WHEN R.review_score >= 4.0 THEN 1 ELSE NULL END) AS TOTAL_POSITIVE_REVIEWS,
    COUNT(CASE WHEN R.review_score < 4.0 AND R.review_score > 2.5 THEN 1 ELSE NULL END) AS TOTAL_NEUTRAL_REVIEWS,
    COUNT(CASE WHEN R.review_score <= 2.5 THEN 1 ELSE NULL END) AS TOTAL_NEGATIVE_REVIEWS
  FROM ecomflow.ecom_silver.products AS P 
  JOIN ecomflow.ecom_silver.reviews AS R ON P.product_id = R.product_id
  GROUP BY P.brand_id
)
SELECT 
  BR.brand_id, 
  B.brand_name, 
  BR.AVG_REVIEW_SCORE, 
  BR.TOTAL_POSITIVE_REVIEWS, 
  BR.TOTAL_NEUTRAL_REVIEWS, 
  BR.TOTAL_NEGATIVE_REVIEWS,
  ROUND(
    BR.TOTAL_POSITIVE_REVIEWS * 100.0 /
    (BR.TOTAL_POSITIVE_REVIEWS + BR.TOTAL_NEUTRAL_REVIEWS + BR.TOTAL_NEGATIVE_REVIEWS),
    2
  ) AS positive_review_rate,
  ROUND(
    BR.TOTAL_NEGATIVE_REVIEWS * 100.0 /
    (BR.TOTAL_POSITIVE_REVIEWS + BR.TOTAL_NEUTRAL_REVIEWS + BR.TOTAL_NEGATIVE_REVIEWS),
    2
  ) AS negative_review_rate,
  ROUND(
    BR.TOTAL_NEUTRAL_REVIEWS * 100.0 /
    (BR.TOTAL_POSITIVE_REVIEWS + BR.TOTAL_NEUTRAL_REVIEWS + BR.TOTAL_NEGATIVE_REVIEWS),
    2
  ) AS neutral_review_rate
FROM BRAND_REVIEWS AS BR 
JOIN ecomflow.ecom_silver.brands AS B ON BR.brand_id = B.brand_id
order by avg_review_score desc