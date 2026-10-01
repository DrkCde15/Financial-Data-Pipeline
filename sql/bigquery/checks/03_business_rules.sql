-- 03: Regras de negócio (0 rows = pass).
SELECT CAST(transaction_date AS STRING) AS key, 'negative avg_ticket' AS issue
FROM `{project}.gold.fact_daily_volume` WHERE avg_ticket_completed < 0
UNION ALL
SELECT CAST(transaction_date AS STRING), 'negative total'
FROM `{project}.gold.fact_daily_volume` WHERE total_completed < 0;
