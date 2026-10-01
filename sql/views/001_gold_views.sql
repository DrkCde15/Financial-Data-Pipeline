-- Views analíticas sobre a partição mais recente (ingestion_date = max).
-- Consumidores (BI/sql) leem as views, nunca as tabelas cruas.

CREATE OR REPLACE VIEW v_daily_volume AS
SELECT transaction_date, n_total, n_completed, total_completed,
       avg_ticket_completed, n_outliers, outlier_total
FROM fact_daily_volume
WHERE ingestion_date = (SELECT max(ingestion_date) FROM fact_daily_volume)
ORDER BY transaction_date;

CREATE OR REPLACE VIEW v_branch_ranking AS
SELECT branch_id, branch_name, city, state,
       n_transactions, total_volume, avg_ticket, n_accounts
FROM agg_branch
WHERE ingestion_date = (SELECT max(ingestion_date) FROM agg_branch)
ORDER BY total_volume DESC;

CREATE OR REPLACE VIEW v_ticket_by_type AS
SELECT transaction_type, n, total, avg_ticket
FROM agg_transaction_type
WHERE ingestion_date = (SELECT max(ingestion_date) FROM agg_transaction_type)
ORDER BY total DESC;

-- KPIs em uma linha (p/ dashboard / teste de sanidade).
CREATE OR REPLACE VIEW v_kpis AS
SELECT
    (SELECT count(*) FROM fact_daily_volume
      WHERE ingestion_date = (SELECT max(ingestion_date) FROM fact_daily_volume)) AS n_days,
    (SELECT coalesce(sum(n_completed), 0) FROM fact_daily_volume
      WHERE ingestion_date = (SELECT max(ingestion_date) FROM fact_daily_volume)) AS total_transactions,
    (SELECT coalesce(sum(total_completed), 0) FROM fact_daily_volume
      WHERE ingestion_date = (SELECT max(ingestion_date) FROM fact_daily_volume)) AS total_volume,
    (SELECT coalesce(sum(n_outliers), 0) FROM fact_daily_volume
      WHERE ingestion_date = (SELECT max(ingestion_date) FROM fact_daily_volume)) AS total_outliers,
    (SELECT max(ingestion_date) FROM fact_daily_volume) AS ingestion_date;
