# sql/ — BigQuery sandbox (serving único, v0.6.0+)

- `bigquery/views/001_gold_views.sql` — `v_daily_volume`, `v_branch_ranking`,
  `v_ticket_by_type`, `v_kpis` (sempre sobre `max(ingestion_date)`).
  `{project}` é substituído pelo loader.
- `bigquery/checks/` — `01_dup_pks`, `02_null_keys`, `03_business_rules`
  (0 rows = pass), `04_freshness` (reporta `days_stale`).
