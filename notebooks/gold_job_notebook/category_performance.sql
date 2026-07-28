

WITH CATE_STATS AS (
SELECT 
  C.cate_id,
  C.cate_name,
  COUNT(DISTINCT P.product_id) AS total_products,
  SUM(O.quantity) AS total_sold,
  COUNT(DISTINCT O.order_id) AS total_orders,
  ROUND(SUM(O.total_price), 2) AS total_revenue,
  ROUND(AVG(R.review_score), 2) AS avg_review_score,
  ROUND(AVG(O.total_price/O.quantity), 2) AS avg_price
FROM ecomflow.ecom_silver.products AS P 
JOIN ecomflow.ecom_silver.categories AS C ON P.category_id = C.cate_id 
JOIN ecomflow.ecom_silver.reviews AS R ON P.product_id = R.product_id 
JOIN ecomflow.ecom_silver.order_items AS O ON P.product_id = O.product_id
GROUP BY C.cate_id, C.cate_name
ORDER BY total_sold DESC
)

SELECT *,
  ROUND(total_revenue * 100 / SUM(total_revenue) OVER(), 2) AS revenue_share,
  ROUND(total_sold * 100 / SUM(total_sold) OVER(), 2) AS sold_share,
  ROUND(total_orders * 100 / SUM(total_orders) OVER(), 2) AS orders_share
FROM CATE_STATS