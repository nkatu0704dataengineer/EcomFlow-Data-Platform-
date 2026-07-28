WITH CATE_STATS AS (
  SELECT 
  P.category_id,
  c.cate_name,
  COUNT(*) AS total_products,
  SUM(CASE WHEN P.product_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_products,
  SUM(CASE WHEN P.product_status = 'INACTIVE' THEN 1 ELSE 0 END) AS inactive_products,
  SUM(CASE WHEN P.product_status = 'DISCONTINUED' THEN 1 ELSE 0 END) AS discontinued_products,
  SUM(CASE WHEN P.product_status = 'OUT_OF_STOCK' THEN 1 ELSE 0 END) AS out_of_stock_products
FROM ecomflow.ecom_silver.products AS P 
JOIN ecomflow.ecom_silver.categories AS C ON P.category_id = C.cate_id
GROUP BY P.category_id, C.cate_name
ORDER BY total_products DESC
)
SELECT *,
  ROUND(active_products * 100.0 / total_products, 2) AS active_rate,
  ROUND(inactive_products * 100.0 / total_products, 2) AS inactive_rate,
  ROUND(discontinued_products * 100.0 / total_products, 2) AS discontinued_rate,
  ROUND(out_of_stock_products * 100.0 / total_products, 2) AS out_of_stock_rate
FROM CATE_STATS
