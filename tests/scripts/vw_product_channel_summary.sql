CREATE OR REPLACE VIEW demo_database.vw_product_channel_summary AS
SELECT
    ch.channel_name,
    p.product_name,
    p.category_name,
    SUM(f.quantity_sold) AS total_units_sold,
    SUM(f.gross_amount) AS total_revenue,
    COUNT(DISTINCT c.customer_sk) AS distinct_customers,
    ROUND(SUM(f.gross_amount) / NULLIF(COUNT(DISTINCT c.customer_sk), 0), 2) AS avg_revenue_per_customer
FROM demo_database.fact_sales_order f
JOIN demo_database.dim_customer_profile c ON f.customer_sk = c.customer_sk AND c.is_current = TRUE
JOIN demo_database.dim_sales_channel ch ON c.channel_id = ch.channel_id
JOIN demo_database.dim_product_catalog p ON f.product_sk = p.product_sk
GROUP BY
    ch.channel_name,
    p.product_name,
    p.category_name;