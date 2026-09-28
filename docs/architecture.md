# Arquitetura — Pipeline de Dados Financeiros Simulado

## 1. Objetivo

Construir um pipeline medallion (Bronze → Silver → Gold) sobre dados
bancários **fictícios** de uma cooperativa, executável localmente e
evolutivo para PySpark, Delta Lake, PostgreSQL, Databricks e AWS.

## 2. Status atual — Etapa 1 (implementada)

```text
data/raw/*.csv|*.json  (simula API/CSV/JSON)
        │  scripts/generate_synthetic_data.py
        ▼
data/raw/              (branches, products, customers, accounts, transactions)
        │  scripts/run_bronze_ingestion.py
        │  src/financial_pipeline/ingestion/bronze.py
        ▼
data/bronze/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
```

Regras da Bronze (propositalmente mínimas):

- Cópia fiel do raw (sem limpeza, sem dedup, sem coerção de tipos).
- Adiciona apenas `_ingested_at` (UTC) e `_source_file` (auditoria).
- Valida contrato: colunas esperadas precisam existir (`schemas.py`).
- Partição por `ingestion_date` (prepara Delta Lake futuro).
- Re-execução idempotente (sobrescreve a partição do dia).
- Formato Parquet (transição natural para Delta Lake).

## 3. Próximas etapas (NÃO implementadas)

| Etapa | Escopo | Tecnologias futuras |
|-------|--------|---------------------|
| Silver | Limpeza, tipagem, dedup, nulos, normalização | PySpark, Delta Lake, Great Expectations ou Pandera |
| Gold | Agregações (volume/dia, ticket médio, clientes ativos, outliers) | Spark SQL, PostgreSQL |
| Serving | Consultas SQL + relatórios | PostgreSQL, notebooks, Power BI/Looker |
| Orquestração | Agendamento e retries | Airflow |
| Observabilidade | Métricas de run, alertas | Projeto 4 do portfólio |
| Cloud | S3 + Glue + Athena / Databricks | Projeto 5 do portfólio (com aviso prévio de custo) |

## 4. Decisões técnicas

- **Sem Spark nesta etapa:** pandas + pyarrow são suficientes para < 1k linhas
  e mantêm o projeto leve e executável em qualquer máquina.
- **Parquet na Bronze:** preserva tipos e prepara a migração para Delta
  (`delta-spark`) sem reescrever a ingestão.
- **Config centralizada (`config.py`):** caminhos via `.env`, nunca hardcoded.
- **Dados 100% sintéticos:** nenhum dado real; 2 outliers intencionais em
  `transactions.json` para exercícios futuros de detecção.
- **Diretórios omitidos de propósito:** `notebooks/`, `transformation/`,
  `silver/`, `gold/` só serão criados quando a etapa correspondente começar.

## 5. Contratos de dados

Ver `src/financial_pipeline/ingestion/schemas.py`:

- `branches(10)`, `products(6)`, `customers(50)`, `accounts(60)`, `transactions(202)`
- Chaves: `branch_id`, `customer_id`, `account_id`, `transaction_id`, `product_id`
- Relacionamentos: customer → branch; account → customer/branch/product;
  transaction → account.
