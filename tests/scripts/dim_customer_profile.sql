INSERT INTO demo_database.dim_customer_profile (
    customer_name, customer_email, channel_id,
    effective_date, expiry_date, is_current
)
SELECT DISTINCT
    rs.customer_name,
    rs.customer_email,
    CASE
	    WHEN rs.customer_email LIKE '%loyalty%' THEN 2 -- Loyalty
	    WHEN rs.customer_email LIKE '%store%' THEN 3   -- Retail Store
	    WHEN rs.customer_email LIKE '%partner%' THEN 4 -- Partner Portal
    	ELSE 1                                          -- Default to Online
	END AS channel_id,
    CURRENT_DATE,
    '9999-12-31',
    TRUE
FROM demo_database.raw_sales rs
LEFT JOIN demo_database.dim_customer_profile dcp
    ON rs.customer_email = dcp.customer_email AND dcp.is_current = TRUE
WHERE dcp.customer_sk IS NULL
   OR rs.customer_name <> dcp.customer_name;