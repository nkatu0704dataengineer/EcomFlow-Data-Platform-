

SELECT 
  S.sub_id, 
  S.sub_name, 
  S.sub_district, 
  initcap(S.sub_city) as sub_city,
  COUNT(O.order_ITEM_id) AS total_orders, 
  COUNT(DISTINCT OD.acc_id) AS total_customers, 
  ROUND(SUM(O.sale_price)/100000.0, 3) AS total_revenue,
  ROUND(AVG(O.total_price)/100000.0, 3) AS avg_order_value
FROM ecomflow.ecom_silver.subsidiaries AS S 
JOIN ecomflow.ecom_silver.order_items AS O ON S.sub_id = O.subsidiary_id 
JOIN ecomflow.ecom_silver.orders AS OD ON O.order_id = OD.order_id
GROUP BY S.sub_id, S.sub_name, S.sub_district, S.sub_city