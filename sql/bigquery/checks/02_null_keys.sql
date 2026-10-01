-- 02: Nulos em colunas críticas (0 rows = pass).
SELECT 'fact_daily_volume' AS tbl, CAST(transaction_date AS STRING) AS key, 'null transaction_date' AS issue
FROM `{project}.gold.fact_daily_volume` WHERE transaction_date IS NULL
UNION ALL
SELECT 'agg_branch', branch_id, 'null branch_id' FROM `{project}.gold.agg_branch` WHERE branch_id IS NULL
UNION ALL
SELECT 'agg_transaction_type', transaction_type, 'null transaction_type'
FROM `{project}.gold.agg_transaction_type` WHERE transaction_type IS NULL
UNION ALL
SELECT 'outliers', transaction_id, 'null transaction_id' FROM `{project}.gold.outliers` WHERE transaction_id IS NULL;
