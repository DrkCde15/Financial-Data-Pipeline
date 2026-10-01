# sql/ — Gold no PostgreSQL (etapa Serving, implementada v0.4.0)

- `ddl/001_gold.sql` — 4 tabelas (`fact_daily_volume`, `agg_transaction_type`,
  `agg_branch`, `outliers`): `NUMERIC(18,2)` p/ dinheiro, `TIMESTAMPTZ` p/ eventos,
  PK `(chave, ingestion_date)`, índices p/ dashboard. DDL roda em Postgres **e**
  SQLite (testes) — `DEFAULT CURRENT_TIMESTAMP` nos dois.
- `views/001_gold_views.sql` — `v_daily_volume`, `v_branch_ranking`,
  `v_ticket_by_type`, `v_kpis` (sempre sobre `max(ingestion_date)`).
- `quality_checks/` — `01_dup_pks`, `02_null_keys`, `03_business_rules`
  (0 rows = pass), `04_freshness` (reporta `days_stale`).

Loader: `scripts/load_postgres.py` (`DATABASE_URL`, DELETE+INSERT por
`ingestion_date`, idempotente). Testes com SQLite, produção no Postgres do
`docker-compose.yml`.
