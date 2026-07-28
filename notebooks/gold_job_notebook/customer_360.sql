
create or replace table ecomflow.ecom_gold.customer_360 as
WITH CUS3601 AS (
    SELECT ACC_ID, COUNT(*) AS TOTAL_ORDERS, SUM(TOTAL_AMOUNT) AS
      TOTAL_SPENT, MIN(CREATED_AT) AS FIRSR_ORDER, MAX(created_at) AS LAST_ORDER
    FROM ecomflow.ecom_silver.orders 
    GROUP BY ACC_ID
    ORDER BY TOTAL_ORDERS DESC
),
CUS3602 AS (
    SELECT O.ACC_ID, COUNT(ORDER_ITEM_ID) AS TOTAL_PRODUCTS_PURCHASED, AVG(E.TOTAL_PRICE) AS AVG_ORDER_VALUE 
    FROM ecomflow.ecom_silver.order_items AS E JOIN ecomflow.ecom_silver.orders AS O ON E.order_id = O.order_id
    GROUP BY O.ACC_ID
)

SELECT CS.ACC_ID, CS.LAST_NAME, CS.FIRST_NAME, CS.GENDER, 
    CS.DISTRICT, CS.CITY, CS.customer_rank, CS.account_age_days, C1.total_orders, C2.total_products_purchased, C1.total_spent,C2.avg_order_value,C1.firsr_order,C1.last_order,CS.is_active
FROM ecomflow.ecom_silver.customers AS CS LEFT JOIN CUS3601 AS C1 ON CS.acc_id = C1.ACC_ID JOIN CUS3602 AS C2 ON C1.ACC_ID = C2.ACC_ID 


