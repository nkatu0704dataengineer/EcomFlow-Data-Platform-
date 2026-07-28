
WITH CAMPAIGN_STATS1 AS (
SELECT 
  P.sale_id, 
  P.sale_name,
  P.start_date,
  P.end_date,
  COUNT(*) AS TOTAL_PRODUCTS,
  datediff(P.end_date, P.start_date) AS duration_days
FROM ecomflow.ecom_silver.program_sales AS P 
JOIN ecomflow.ecom_silver.product_sales AS S ON P.sale_id = S.sale_id
GROUP BY P.sale_id, P.sale_name, P.start_date, P.end_date
ORDER BY P.sale_id
),
CAMPAIGN_STATS2 AS (
SELECT 
  P.sale_id,
  COUNT(*) AS TOTAL_ORDERS,
  SUM(O.quantity) AS TOTAL_QUANTITY_SOLD,
  ROUND(SUM(O.total_price), 2) AS TOTAL_REVENUE,
  ROUND(AVG(O.total_price), 2) AS AVG_REVENUE,
  sum(o.list_price*o.quantity) as TOTAL_ORG_PRICE,
  SUM(O.sale_price*O.quantity) AS TOTAL_SALE_PRICE
FROM ecomflow.ecom_silver.program_sales AS P 
JOIN ecomflow.ecom_silver.order_items AS O ON P.sale_id = O.sale_id
GROUP BY P.sale_id
ORDER BY P.sale_id
),
CAMPAIGN_STATS3 AS (
  WITH customer_first_order AS (
    SELECT acc_id, MIN(created_at) AS first_order_date
    FROM ecomflow.ecom_silver.orders
    GROUP BY acc_id
  ),
  campaign_customers AS (
    SELECT DISTINCT
      O.sale_id,
      D.acc_id,
      F.first_order_date,
      MIN(D.created_at) AS first_order_in_campaign
    FROM ecomflow.ecom_silver.order_items AS O 
    JOIN ecomflow.ecom_silver.orders AS D ON O.order_id = D.order_id
    JOIN customer_first_order AS F ON D.acc_id = F.acc_id
    GROUP BY O.sale_id, D.acc_id, F.first_order_date
  )
  SELECT 
    sale_id,
    COUNT(DISTINCT acc_id) AS TOTAL_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN first_order_date = first_order_in_campaign THEN acc_id END) AS NEW_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN first_order_date < first_order_in_campaign THEN acc_id END) AS RETURNING_CUSTOMERS
  FROM campaign_customers
  GROUP BY sale_id
)
SELECT *, 
  (TOTAL_ORG_PRICE - TOTAL_SALE_PRICE) AS TOTAL_DISCOUNT_AMOUNT
FROM 
CAMPAIGN_STATS1
JOIN CAMPAIGN_STATS2 USING (sale_id)
JOIN CAMPAIGN_STATS3 USING (sale_id)