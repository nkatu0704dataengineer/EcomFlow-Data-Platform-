select city, count(*) as total_customer
from ecomflow.ecom_silver.customers
group by city
order by total_customer desc