-- 04: Frescor (sempre 1 row; alerta se days_stale > 7).
SELECT MAX(ingestion_date) AS latest_ingestion_date,
       DATE_DIFF(CURRENT_DATE(), CAST(MAX(ingestion_date) AS DATE), DAY) AS days_stale
FROM `{project}.gold.fact_daily_volume`;
