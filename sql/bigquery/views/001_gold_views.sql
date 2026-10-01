-- Views analíticas sobre a partição mais recente (ingestion_date = max).
-- `{project}` é substituído pelo loader (default: engdta).

CREATE OR REPLACE VIEW `{project}.gold.v_daily_volume` AS
SELECT transaction_date, n_total, n_completed, total_completed,
       avg_ticket_completed, n_outliers, outlier_total
FROM `{project}.gold.fact_daily_volume`
WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.fact_daily_volume`)
ORDER BY transaction_date;

CREATE OR REPLACE VIEW `{project}.gold.v_branch_ranking` AS
SELECT branch_id, branch_name, city, state,
       n_transactions, total_volume, avg_ticket, n_accounts
FROM `{project}.gold.agg_branch`
WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.agg_branch`)
ORDER BY total_volume DESC;

CREATE OR REPLACE VIEW `{project}.gold.v_ticket_by_type` AS
SELECT transaction_type, n, total, avg_ticket
FROM `{project}.gold.agg_transaction_type`
WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.agg_transaction_type`)
ORDER BY total DESC;

CREATE OR REPLACE VIEW `{project}.gold.v_kpis` AS
SELECT
  (SELECT COUNT(*) FROM `{project}.gold.fact_daily_volume`
    WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.fact_daily_volume`)) AS n_days,
  (SELECT COALESCE(SUM(n_completed), 0) FROM `{project}.gold.fact_daily_volume`
    WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.fact_daily_volume`)) AS total_transactions,
  (SELECT COALESCE(SUM(total_completed), 0) FROM `{project}.gold.fact_daily_volume`
    WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.fact_daily_volume`)) AS total_volume,
  (SELECT COALESCE(SUM(n_outliers), 0) FROM `{project}.gold.fact_daily_volume`
    WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM `{project}.gold.fact_daily_volume`)) AS total_outliers;
