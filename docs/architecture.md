# Arquitetura — Pipeline de Dados Financeiros Simulado

## 1. Objetivo

Construir um pipeline medallion (Bronze → Silver → Gold) sobre dados
bancários **fictícios** de uma cooperativa, executável localmente e
evolutivo para PySpark, Delta Lake, PostgreSQL e BigQuery (sandbox, estágio 5).

## 2. Status atual — Etapas 1–5 locais (implementadas, v0.4.0)

```text
data/raw/*.csv|*.json  (simula API/CSV/JSON)
        │  scripts/generate_synthetic_data.py
        ▼
data/raw/              (branches, products, customers, accounts, transactions)
        │  scripts/run_bronze_ingestion.py
        ▼
data/bronze/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
        │  scripts/run_silver.py
        ▼
data/silver/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet (+ _quarantine/)
        │  scripts/run_gold.py
        ▼
data/gold/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet (4 marts)
        │  scripts/load_postgres.py ($DATABASE_URL, DELETE+INSERT por dia)
        ▼
PostgreSQL (fact_daily_volume, agg_transaction_type, agg_branch, outliers)
  + views (v_daily_volume, v_branch_ranking, v_ticket_by_type, v_kpis)
  + quality_checks (01–04)
        ▲ agendado por
dags/financial_pipeline.py (Airflow 2.6, @daily, backfill por {{ ds }})
```

Regras da Bronze (propositalmente mínimas):

- Cópia fiel do raw (sem limpeza, sem dedup, sem coerção de tipos).
- Adiciona apenas `_ingested_at` (UTC) e `_source_file` (auditoria).
- Valida contrato: colunas esperadas precisam existir (`schemas.py`).
- Partição por `ingestion_date` (prepara particionamento futuro no BigQuery).
- Re-execução idempotente (sobrescreve a partição do dia).
- Formato Parquet (transição natural para load via GCS → BigQuery no estágio 5).

## 3. Status por etapa (v0.4.0)

| Etapa | Status | Escopo / tecnologia |
|-------|--------|---------------------|
| Silver | ✅ implementada (v0.2.0, pandas) | Tipagem, dedup por PK, quarentena, FKs. PySpark/Delta/GE adiados (over-engineering p/ 328 linhas) |
| Gold | ✅ implementada (v0.3.0, pandas) | `fact_daily_volume`, `agg_transaction_type`, `agg_branch`, `outliers` — médias excl. outliers |
| Serving | ✅ implementada (v0.4.0) | Postgres 15 (`sql/ddl`, `views` v_*, `quality_checks` 01–04) + `scripts/load_postgres.py` (DELETE+INSERT por dia, SQLite nos testes) |
| Orquestração | ✅ implementada (v0.4.0) | Airflow 2.6 (`dags/financial_pipeline.py`, 6 tasks, retries=2, `@daily`, backfill `{{ ds }}`) + `docker-compose.yml` (postgres + airflow) |
| Observabilidade | Métricas de run, alertas | Projeto 4 do portfólio |
| Cloud (estágio 5) | GCS (staging) + BigQuery sandbox → datasets bronze/silver/gold | Projeto 5 — adiado de propósito (sandbox expira em 60 dias; com aviso prévio de custo/quotas) |

## 4. Decisões técnicas

- **Sem Spark nesta etapa:** pandas + pyarrow são suficientes para < 1k linhas
  e mantêm o projeto leve e executável em qualquer máquina.
- **Parquet na Bronze:** preserva tipos e prepara o load futuro
  (GCS → BigQuery, estágio 5) sem reescrever a ingestão.
- **Config centralizada (`config.py`):** caminhos via `.env`, nunca hardcoded.
- **Dados 100% sintéticos:** nenhum dado real; 2 outliers intencionais em
  `transactions.json` preservados na Silver com `is_outlier=true` para a Gold.
- **Quarentena, não drop silencioso:** linhas rejeitadas vão para
  `silver/_quarantine/<tabela>/` (parquet all-string, diagnóstico) com
  `_quarantine_reason` (`null_<pk>`, `invalid_*`, `fk_*_missing`).
- **Diretórios omitidos de propósito:** `notebooks/` só será criado na etapa BI.
- **Cloud só no estágio 5 (decisão):** BigQuery sandbox já disponível
  (`engdta.staging`), mas nenhum dataset `bronze/silver/gold` será criado
  antes do estágio 5 — tabelas do sandbox expiram em 60 dias e o free tier
  (1 TiB queries + 10 GiB/mês) deve ser preservado para o benchmark de custo.

## 5. Contratos de dados

Ver `src/financial_pipeline/ingestion/schemas.py`:

- `branches(10)`, `products(6)`, `customers(50)`, `accounts(60)`, `transactions(202)`
- Chaves: `branch_id`, `customer_id`, `account_id`, `transaction_id`, `product_id`
- Relacionamentos: customer → branch; account → customer/branch/product;
  transaction → account.
