INSERT INTO demo_database.dim_product_catalog (product_name, category_name, base_price)
SELECT DISTINCT rs.product_name, rs.category, rs.unit_price
FROM demo_database.raw_sales rs
LEFT JOIN demo_database.dim_product_catalog dp
    ON rs.product_name = dp.product_name AND rs.category = dp.category_name
WHERE dp.product_sk IS NULL;