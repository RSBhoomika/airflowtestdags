INSERT INTO demo_database.fact_sales_order (
sale_id,
order_date,
customer_sk,
product_sk,
quantity_sold,
selling_price,
gross_amount
)
SELECT
rs.sale_id,
rs.sale_date AS order_date,
dcp.customer_sk,
dpc.product_sk,
rs.quantity AS quantity_sold,
rs.unit_price AS selling_price,
rs.quantity * rs.unit_price AS gross_amount
FROM demo_database.raw_sales rs
JOIN (
SELECT customer_sk, customer_name, customer_email
FROM demo_database.dim_customer_profile
WHERE is_current = TRUE
GROUP BY customer_sk, customer_name, customer_email
) dcp ON rs.customer_name = dcp.customer_name
AND rs.customer_email = dcp.customer_email
JOIN (
SELECT MIN(product_sk) AS product_sk, product_name, category_name
FROM demo_database.dim_product_catalog
GROUP BY product_name, category_name
) dpc ON rs.product_name = dpc.product_name
AND rs.category = dpc.category_name
WHERE rs.sale_id NOT IN (
SELECT sale_id FROM demo_database.fact_sales_order
);