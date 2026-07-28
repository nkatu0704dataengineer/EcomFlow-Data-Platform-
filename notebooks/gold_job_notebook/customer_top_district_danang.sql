select district, count(*) as total_customer
from ecomflow.ecom_silver.customers
where city='Đà Nẵng'
group by district
order by total_customer desc