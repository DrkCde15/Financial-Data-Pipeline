-- 04: Frescor (1 row sempre; alerta se ingestion_date < hoje - 7 dias).
SELECT max(ingestion_date) AS latest_ingestion_date,
       current_date - max(ingestion_date) AS days_stale
FROM fact_daily_volume;
