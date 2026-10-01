-- 01: PKs duplicadas dentro da mesma partição (0 rows = pass).
SELECT transaction_date, ingestion_date, COUNT(*) AS n
FROM `{project}.gold.fact_daily_volume`
GROUP BY 1, 2 HAVING COUNT(*) > 1;
