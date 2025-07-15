CREATE OR REPLACE VIEW demo_database.vw_sales_summary AS
SELECT
    c.customer_name,
    c.customer_email,
    ch.channel_name,
    p.product_name,
    p.category_name,
    COUNT(f.order_sk) AS num_orders,
    SUM(f.quantity_sold) AS total_quantity,
    SUM(f.gross_amount) AS total_revenue,
    ROUND(AVG(f.selling_price), 2) AS avg_selling_price,
    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date
FROM demo_database.fact_sales_order f
JOIN demo_database.dim_customer_profile c ON f.customer_sk = c.customer_sk AND c.is_current = TRUE
JOIN demo_database.dim_sales_channel ch ON c.channel_id = ch.channel_id
JOIN demo_database.dim_product_catalog p ON f.product_sk = p.product_sk
GROUP BY
    c.customer_name,
    c.customer_email,
    ch.channel_name,
    p.product_name,
    p.category_name;