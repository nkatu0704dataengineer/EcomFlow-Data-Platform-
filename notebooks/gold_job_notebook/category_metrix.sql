SELECT P.category_id, C.cate_name, 
  COUNT(*) AS total_products,
  ROUND(AVG(P.product_weight_g), 2) AS avg_weight_g,
  ROUND(AVG(P.product_length_cm), 2) AS avg_length_cm,
  ROUND(AVG(P.product_height_cm), 2) AS avg_height_cm,
  ROUND(AVG(P.product_width_cm), 2) AS avg_width_cm, 
  ROUND(AVG(P.product_volume_cm3), 2) AS avg_volume_cm3,
  ROUND(AVG(P.product_density_g_cm3), 2) AS avg_density_g_cm3,
  ROUND(MAX(P.product_weight_g), 2) AS max_weight,
  ROUND(MIN(P.product_weight_g), 2) AS min_weight
FROM ecomflow.ecom_silver.products AS P 
JOIN ecomflow.ecom_silver.categories AS C ON P.category_id = C.cate_id
GROUP BY P.category_id, C.cate_name
ORDER BY total_products DESC