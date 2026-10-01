# Arquitetura — Pipeline de Dados Financeiros Simulado

## 1. Objetivo

Construir um pipeline medallion (Bronze → Silver → Gold) sobre dados
bancários **fictícios** de uma cooperativa, com serving no BigQuery sandbox,
evolutivo para PySpark e Delta Lake.

## 2. Status atual (v0.6.0 — pipeline completo, serving único BQ)

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
        │  scripts/load_bigquery.py (load jobs WRITE_TRUNCATE por tabela)
        ▼
BigQuery engdta.{bronze,silver,gold} (14 tabs + views v_* + checks 01–04)
        │  dashboard/app.py (Streamlit: KPIs, série, ranking, outliers)
        ▲ agendado por
dags/financial_pipeline.py (Airflow 2.6, 6 tasks lineares, @daily, {{ ds }})
```

Regras da Bronze (propositalmente mínimas):

- Cópia fiel do raw (sem limpeza, sem dedup, sem coerção de tipos).
- Adiciona apenas `_ingested_at` (UTC) e `_source_file` (auditoria).
- Valida contrato: colunas esperadas precisam existir (`schemas.py`).
- Partição por `ingestion_date` (prepara particionamento futuro no BigQuery).
- Re-execução idempotente (sobrescreve a partição do dia).
- Formato Parquet (transição natural para load via GCS → BigQuery no estágio 5).

## 3. Status por etapa (v0.6.0)

| Etapa | Status | Escopo / tecnologia |
|-------|--------|---------------------|
| Bronze→Silver→Gold | ✅ pandas local | Tipagem, dedup, quarentena, FKs; 4 marts com grão declarado |
| Serving | ✅ BigQuery sandbox (único) | `engdta.{bronze,silver,gold}` via load jobs WRITE_TRUNCATE + `sql/bigquery` (views + checks 01–04). Postgres removido em v0.6.0 (era dual serving; BQ cobre o caso) |
| BI | ✅ Streamlit (v0.7.0, código) | `dashboard/` sobre views BQ (fallback parquet local, `DATA_SOURCE`). Looker Studio segue opcional p/ link público |
| Orquestração | ✅ Airflow 2.6 | DAG linear 6 tasks, retries=2, `@daily`, backfill `{{ ds }}` + compose (postgres só metadados, podman) |
| Observabilidade | Métricas de run, alertas | Projeto 4 do portfólio |
| Fora do sandbox | Particionamento, IAM, benchmark pago | Projeto 5 |

## 4. Decisões técnicas

- **Sem Spark nesta etapa:** pandas + pyarrow são suficientes para < 1k linhas
  e mantêm o projeto leve e executável em qualquer máquina.
- **Parquet nas camadas:** preserva tipos e alimenta o BQ via load jobs
  sem etapa intermediária.
- **Serving único BQ (decisão v0.6.0):** Postgres removido — dual serving
  dobrava loaders e dialetos SQL sem cobrir caso novo. O Postgres do compose
  segue existindo, mas só como metadados do Airflow (infra, não pipeline).
- **Config centralizada (`config.py`):** caminhos via `.env`, nunca hardcoded.
- **Dados 100% sintéticos:** nenhum dado real; 2 outliers intencionais em
  `transactions.json` preservados na Silver com `is_outlier=true` para a Gold.
- **Quarentena, não drop silencioso:** linhas rejeitadas vão para
  `silver/_quarantine/<tabela>/` (parquet all-string, diagnóstico) com
  `_quarantine_reason` (`null_<pk>`, `invalid_*`, `fk_*_missing`).
- **Diretórios omitidos de propósito:** `notebooks/` só será criado na etapa BI.
- **Cloud no sandbox:** `engdta.{bronze,silver,gold}` criados com
  expiração default de 60 dias (regra do sandbox). Carga via load jobs Parquet
  WRITE_TRUNCATE (batch, grátis) em vez de DML (restrito no sandbox).
  `ingestion_date` como coluna STRING de linhagem — particionamento físico
  fica p/ fora do sandbox. Lib `google-cloud-bigquery` (a imagem do Airflow
  já traz; credenciais via ADC montado).

## 5. Contratos de dados

Ver `src/financial_pipeline/ingestion/schemas.py`:

- `branches(10)`, `products(6)`, `customers(50)`, `accounts(60)`, `transactions(202)`
- Chaves: `branch_id`, `customer_id`, `account_id`, `transaction_id`, `product_id`
- Relacionamentos: customer → branch; account → customer/branch/product;
  transaction → account.
