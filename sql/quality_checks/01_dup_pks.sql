-- 01: PKs duplicadas dentro da mesma partição (0 rows = pass).
SELECT transaction_date, ingestion_date, count(*)
FROM fact_daily_volume
GROUP BY 1, 2 HAVING count(*) > 1;
