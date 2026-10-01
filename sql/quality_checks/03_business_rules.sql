-- 03: Regras de negócio (0 rows = pass).
-- avg_ticket nunca negativo; total_completed >= 0.
SELECT transaction_date AS key, 'negative avg_ticket' AS issue
FROM fact_daily_volume WHERE avg_ticket_completed < 0
UNION ALL
SELECT transaction_date, 'negative total' FROM fact_daily_volume WHERE total_completed < 0;
