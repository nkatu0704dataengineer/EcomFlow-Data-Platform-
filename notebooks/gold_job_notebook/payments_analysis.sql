
CREATE OR REPLACE TABLE ecomflow.ecom_gold.payments_analysis AS
WITH payment_stats1 AS (
  SELECT o.order_id, COUNT(*) AS total_items
  FROM ecomflow.ecom_silver.orders AS o 
  JOIN ecomflow.ecom_silver.order_items AS i ON o.order_id = i.order_id
  GROUP BY o.order_id
),
payment_stats2 AS (
  SELECT 
     P.payment_method, 
     COUNT(*) AS total_orders,
     SUM(CASE WHEN P.PAYMENT_STATUS='SUCCESS' THEN 1 ELSE 0 END) AS total_paid,
     SUM(CASE WHEN P.PAYMENT_STATUS='PENDING' THEN 1 ELSE 0 END) AS total_pending,
     SUM(CASE WHEN P.PAYMENT_STATUS='FAILED' THEN 1 ELSE 0 END) AS total_failed,
     SUM(CASE WHEN P.PAYMENT_STATUS='REFUNDED' THEN 1 ELSE 0 END) AS total_refunded,
     COUNT(DISTINCT O.acc_id) AS total_customers,
     SUM(P.payment_amount) AS total_revenue,
     ROUND(AVG(P.payment_amount),2) AS avg_order_value,
     MAX(s.total_items) AS max_items,
     MIN(s.total_items) AS min_items
  FROM ecomflow.ecom_silver.payments AS P 
  JOIN ecomflow.ecom_silver.orders AS O ON P.order_id = O.order_id 
  JOIN payment_stats1 AS s ON P.order_id = s.order_id
  GROUP BY P.payment_method
  ORDER BY total_orders DESC
)
SELECT *,
  CASE 
    WHEN payment_method IN ('BANKING', 'MOMO', 'ZALOPAY', 'VNPAY', 'CREDIT_CARD') THEN 'ONLINE'
    ELSE 'OFFLINE'
  END AS payment_channel,
  ROUND(total_revenue/(SELECT SUM(TOTAL_AMOUNT) FROM ecomflow.ecom_silver.orders),2) AS payment_share_pct,
  ROUND(total_customers/(SELECT COUNT(DISTINCT(acc_id)) FROM ecomflow.ecom_silver.orders),2) AS payment_customers_pct,
  ROUND(total_paid * 1.0 / total_orders,2) AS payment_success_rate,
  ROUND(total_pending * 1.0 / total_orders,2) AS payment_pending_rate,
  ROUND(total_failed * 1.0 / total_orders,2) AS payment_failed_rate,
  ROUND(total_refunded * 1.0 / total_orders,2) AS payment_refunded_rate
FROM payment_stats2