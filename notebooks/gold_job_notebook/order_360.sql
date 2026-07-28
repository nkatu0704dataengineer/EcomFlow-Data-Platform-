
create or replace table ecomflow.ecom_gold.order_360 as
WITH ORDER_STATS1 AS (
    SELECT 
      O.order_id,
      O.acc_id,
      O.created_at,
      CASE 
        WHEN dayofweek(O.created_at) IN (1, 7) THEN true
        ELSE false
      END as is_weekend_purchase,
      O.order_status,
      O.total_amount,
      O.total_shipfee,
      (O.total_amount + O.total_shipfee) AS GRAND_TOTAL,
      ROUND(O.total_shipfee / O.total_amount, 5) AS shipping_fee_ratio,
      datediff(O.delivered_at, O.created_at) AS delivery_day,
      CASE 
        WHEN datediff(O.delivered_at, O.estimated_delivery_date) > 0 THEN datediff(O.delivered_at, O.estimated_delivery_date)
        ELSE 0
      END AS delay_day
    FROM ecomflow.ecom_silver.orders AS O
),
ORDER_STATS2 AS (
    SELECT 
      O.order_id,
      SUM(O.quantity) AS total_sold,
      COUNT(*) AS total_items,
      ROUND(SUM(O.quantity * P.product_weight_g),2) AS total_weight_sold,
      ROUND(SUM(O.quantity * P.product_volume_cm3),2) AS total_volume_sold,
      ROUND(AVG(O.total_price), 2) AS avg_price_per_item,
      ROUND(AVG(O.shipping_distance_km),2) AS avg_distance_km,
      MAX(O.shipping_distance_km) AS max_distance_km,
      MIN(O.shipping_distance_km) AS min_distance_km,
      ROUND(AVG(O.shipping_fee),2) AS avg_fee
    FROM ecomflow.ecom_silver.order_items AS O 
    JOIN ecomflow.ecom_silver.products AS P ON O.product_id = P.product_id
    GROUP BY O.order_id
),
ORDER_STATS3 AS (
    SELECT 
      R.order_id,
      ROUND(AVG(R.review_score),2) AS avg_review,
      COUNT(*) AS total_reviews,
      MAX(R.has_reply) AS has_reply,
      AVG(datediff(R.replied_at, R.created_at)) AS reply_in,
      SUM(CASE WHEN R.review_sentiment = 'Positive' THEN 1 ELSE 0 END) AS positive_reviews,
      SUM(CASE WHEN R.review_sentiment = 'Negative' THEN 1 ELSE 0 END) AS negative_reviews,
      SUM(CASE WHEN R.review_sentiment = 'Neutral' THEN 1 ELSE 0 END) AS neutral_reviews
    FROM ecomflow.ecom_silver.reviews AS R 
    GROUP BY R.order_id
)
SELECT 
  O.*,
  P.total_sold,
  P.total_items,
  P.total_weight_sold,
  P.total_volume_sold,
  P.avg_price_per_item,
  P.avg_distance_km,
  P.max_distance_km,
  P.min_distance_km,
  P.avg_fee,
  R.avg_review,
  R.total_reviews,
  R.has_reply,
  R.reply_in,
  R.positive_reviews,
  R.negative_reviews,
  R.neutral_reviews,
  CASE 
    WHEN O.total_amount > 100000000 THEN 'HIGH'
    WHEN O.total_amount > 50000000 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS order_value_segment,
  CASE 
    WHEN P.total_volume_sold > 300000 THEN 'HIGH'
    WHEN P.total_volume_sold > 200000 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS product_volume_segment,
  CASE 
    WHEN P.total_weight_sold > 100000 THEN 'HIGH'
    WHEN P.total_weight_sold > 60000 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS product_weight_segment
FROM ORDER_STATS1 AS O 
LEFT JOIN ORDER_STATS2 AS P ON O.order_id = P.order_id 
LEFT JOIN ORDER_STATS3 AS R ON O.order_id = R.order_id