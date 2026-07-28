select district, count(*) as total_customer
  from ecomflow.ecom_silver.customers
  where city='Hà Nội'
  group by district
  order by total_customer desc