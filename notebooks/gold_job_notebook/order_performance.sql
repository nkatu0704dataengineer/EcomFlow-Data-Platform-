
create or replace table ecomflow.ecom_gold.order_performance as
SELECT 
  YEAR(O.created_at) AS YEAR, 
  MONTH(O.created_at) AS MONTH, 
  COUNT(*) AS total_orders,
  SUM(O.total_amount) AS hope_total_revenue,
  AVG(O.total_amount) AS avg_order_value,
  SUM(CASE WHEN O.order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
  SUM(CASE WHEN O.order_status = 'DELIVERED' THEN O.total_amount ELSE 0 END) AS real_total_revenue,
  SUM(CASE WHEN O.ORDER_STATUS='CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
  SUM(CASE WHEN O.ORDER_STATUS='CANCELLED' THEN O.total_amount ELSE 0 END) AS cancelled_hopeful_revenue
FROM ecomflow.ecom_silver.orders AS O
group by year,month
order by year,month