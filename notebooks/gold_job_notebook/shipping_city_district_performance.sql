select 
  o.shipping_city, 
  o.shipping_district,
  count(*) as total_orders,
  round(avg(o.total_shipfee),2) as avg_fee,
  round(avg(o.total_amount),2) as avg_revenue,
  round(avg(datediff(o.delivered_at, o.shipped_at)),2) as avg_days,
  max(datediff(o.delivered_at, o.shipped_at)) as max_days,
  min(datediff(o.delivered_at, o.shipped_at)) as min_days,
  round(sum(case when o.delivered_at < o.estimated_delivery_date then 1 else 0 end) * 100.0 / count(*),2) as ontime_rate
from ecomflow.ecom_silver.orders as O
group by o.shipping_city, o.shipping_district
order by o.shipping_city, o.shipping_district