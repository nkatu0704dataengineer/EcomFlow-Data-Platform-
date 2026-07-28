

SELECT 
  P.PRODUCT_ID, 
  P.product_name,
  P.brand_id,
  C.cate_name,
  PS.avg_rating AS product_rating,
  PS.review_count,
  ROUND(SUM(O.quantity), 2) AS total_sold,
  COUNT(*) AS total_orders,
  ROUND(SUM(O.total_price), 2) AS total_revenue,
  ROUND(AVG(O.total_price/ O.quantity), 2) AS avg_price
FROM ecomflow.ecom_silver.products AS P 
JOIN ecomflow.ecom_silver.categories AS C ON P.category_id = C.cate_id 
LEFT JOIN ecomflow.ecom_silver.product_scores AS PS ON P.product_id = PS.product_id
JOIN ecomflow.ecom_silver.reviews AS R ON P.product_id = R.product_id 
JOIN ecomflow.ecom_silver.order_items AS O ON P.product_id = O.product_id
GROUP BY P.PRODUCT_ID, P.product_name, P.brand_id, C.cate_name, PS.avg_rating, PS.review_count
ORDER BY total_sold DESC