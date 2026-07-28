

with brand_stats as (
 SELECT 
  OI.subsidiary_id,
  COUNT(DISTINCT OI.order_id) AS total_order,
  SUM(OI.order_item_id) AS total_items,
  SUM(OI.quantity) AS total_quantity,
  ROUND(SUM(OI.shipping_fee), 2) AS total_fee,
  ROUND(AVG(OI.shipping_fee), 2) AS avg_fee,
  ROUND(SUM(OI.total_price), 2) AS total_revenue,
  ROUND(AVG(OI.total_price), 2) AS avg_revenue, 
  ROUND(AVG(OI.shipping_distance_km), 2) AS avg_distance,
  ROUND(MAX(OI.shipping_distance_km), 2) AS max_distance,
  ROUND(MIN(OI.shipping_distance_km), 2) AS min_distance,
  ROUND(SUM(OI.item_total_weight_g), 2) AS total_weight
FROM ecomflow.ecom_silver.order_items AS OI
GROUP BY OI.subsidiary_id
)
select *, 
ROUND(total_items * 1.0 / total_order, 2) as avg_items_per_order
from brand_stats
ORDER BY subsidiary_id