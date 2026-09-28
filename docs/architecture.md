# Arquitetura — Pipeline de Dados Financeiros Simulado

## 1. Objetivo

Construir um pipeline medallion (Bronze → Silver → Gold) sobre dados
bancários **fictícios** de uma cooperativa, executável localmente e
evolutivo para PySpark, Delta Lake, PostgreSQL e BigQuery (sandbox, estágio 5).

## 2. Status atual — Etapas 1–2 (implementadas)

```text
data/raw/*.csv|*.json  (simula API/CSV/JSON)
        │  scripts/generate_synthetic_data.py
        ▼
data/raw/              (branches, products, customers, accounts, transactions)
        │  scripts/run_bronze_ingestion.py
        │  src/financial_pipeline/ingestion/bronze.py
        ▼
data/bronze/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
        │  scripts/run_silver.py
        │  src/financial_pipeline/transformation/silver.py
        ▼
data/silver/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
data/silver/_quarantine/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
```

Regras da Bronze (propositalmente mínimas):

- Cópia fiel do raw (sem limpeza, sem dedup, sem coerção de tipos).
- Adiciona apenas `_ingested_at` (UTC) e `_source_file` (auditoria).
- Valida contrato: colunas esperadas precisam existir (`schemas.py`).
- Partição por `ingestion_date` (prepara particionamento futuro no BigQuery).
- Re-execução idempotente (sobrescreve a partição do dia).
- Formato Parquet (transição natural para load via GCS → BigQuery no estágio 5).

## 3. Próximas etapas (Silver implementada)

| Etapa | Status | Escopo / tecnologia |
|-------|--------|---------------------|
| Silver | ✅ implementada (v0.2.0, pandas) | Tipagem (`birth_date`/`open_date`→date, `timestamp`→UTC, `amount` round 2 + `is_outlier`, `is_active`→bool), dedup por PK, quarentena (PK nula, tipo inválido, status inválido, FK órfã). PySpark/Delta/GE seguem adiados (over-engineering p/ 328 linhas) |
| Gold | NÃO implementada | Agregações (volume/dia, ticket médio, clientes ativos, outliers) |
| Serving | Consultas SQL + relatórios | PostgreSQL, notebooks, Power BI/Looker |
| Orquestração | Agendamento e retries | Airflow |
| Observabilidade | Métricas de run, alertas | Projeto 4 do portfólio |
| Cloud (estágio 5) | GCS (staging) + BigQuery sandbox → datasets bronze/silver/gold | Projeto 5 do portfólio — adiado de propósito (sandbox expira em 60 dias; com aviso prévio de custo/quotas) |

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
- **Diretórios omitidos de propósito:** `notebooks/`, `gold/` só serão criados
  quando a etapa correspondente começar.
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
