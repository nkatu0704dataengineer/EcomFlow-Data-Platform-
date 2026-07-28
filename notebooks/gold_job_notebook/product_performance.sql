
create or replace table ecomflow.ecom_gold.product_performance as 
SELECT 
  P.PRODUCT_ID, 
  P.product_name,
  P.brand_id,
  C.cate_name,
  P.product_rating,
  P.review_count,
  ROUND(SUM(O.quantity), 2) AS total_sold,
  COUNT(*) AS total_orders,
  ROUND(SUM(O.total_price), 2) AS total_revenue,
  ROUND(AVG(O.total_price/ O.quantity), 2) AS avg_price,
  P.total_stock
FROM ecomflow.ecom_silver.products AS P 
JOIN ecomflow.ecom_silver.categories AS C ON P.category_id = C.cate_id 
JOIN ecomflow.ecom_silver.reviews AS R ON P.product_id = R.product_id 
JOIN ecomflow.ecom_silver.order_items AS O ON P.product_id = O.product_id
GROUP BY P.PRODUCT_ID, P.product_name, P.brand_id, C.cate_name, P.product_rating, P.review_count, P.total_stock
ORDER BY total_sold DESC