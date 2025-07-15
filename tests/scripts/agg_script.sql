CREATE TABLE move_db.pgw_region_agg
PROPERTIES("replication_num" = "2")
AS
SELECT
    region,
    countryofsale,
    COUNT(*) AS cnt
FROM move_db.pgw_parsed
GROUP BY region, countryofsale;