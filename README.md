# Pipeline de Dados Financeiros Simulado

## Objetivo

Construir um pipeline de dados bancários de uma cooperativa financeira
**fictícia**, seguindo arquitetura medallion (Bronze → Silver → Gold),
para demonstrar competências de Data Engineering Jr/Pleno: SQL, Python,
PySpark, Delta Lake, PostgreSQL, BigQuery, ETL/ELT e data quality.

**Escopo desta versão (v0.4.0 — pipeline local completo):**
`raw → Bronze → Silver → Gold → PostgreSQL`, agendado por Airflow 2.6,
executável local via `docker compose`, sem Spark e sem cloud.
Fonte 100% simulada (decisão até o estágio 5).

## Problema

Portfólios de dados frequentemente mostram notebooks isolados, sem pipeline
reprodutível, sem contrato de dados, sem testes e sem caminho de evolução
para produção (orquestração, qualidade, observabilidade, cloud).
Este projeto resolve isso começando pela fundação: dados sintéticos
versionados, ingestão idempotente com auditoria, configuração centralizada
e testes automatizados.

## Arquitetura

```text
JSON/CSV simulado → Bronze (Parquet) → Silver (limpeza+quarentena) → Gold (4 marts)
                                                                              ↓
                                                     PostgreSQL (DDL+views+checks)
                                                                              ▲
                                              Airflow 2.6 @daily (6 tasks, {{ ds }})
```

Etapas implementadas:

- `scripts/generate_synthetic_data.py` gera 5 tabelas fictícias em `data/raw/`
- `scripts/run_bronze_ingestion.py` copia para `data/bronze/<tabela>/ingestion_date=.../data.parquet`
- Bronze adiciona apenas `_ingested_at` e `_source_file` (sem limpeza)
- Validação de contrato (colunas esperadas) com falha rápida
- `scripts/run_silver.py` limpa para `data/silver/<tabela>/ingestion_date=.../data.parquet`
- Silver: dedup por PK, tipagem (`birth_date`/`open_date`→date, `timestamp`→UTC,
  `amount` round 2 + `is_outlier`, `is_active`→bool), status normalizado,
  FKs validadas; rejeitos em `data/silver/_quarantine/<tabela>/` com `_quarantine_reason`
- `scripts/run_gold.py` agrega para `data/gold/<tabela>/ingestion_date=.../data.parquet`
  (só calcula — não encosta no banco; roda sem Postgres)
- Gold (grão declarado, completed excl. outliers nas médias):
  `fact_daily_volume(transaction_date)`, `agg_transaction_type(transaction_type)`,
  `agg_branch(branch_id)`, `outliers(transaction_id)` enriquecida p/ investigação
- `scripts/load_postgres.py` carrega Gold (parquet) → PostgreSQL (DELETE+INSERT por
  `ingestion_date`, idempotente) + `sql/ddl`, `sql/views` (v_* sobre
  `max(ingestion_date)`), `sql/quality_checks` (01–03: 0 rows = pass; 04: frescor).
  Separado da Gold de propósito: permite recarregar o banco sem recomputar e
  auditar o dado servido (não o arquivo)
- `dags/financial_pipeline.py` (Airflow 2.6): 6 tasks
  `generate >> bronze >> silver >> gold >> load >> checks`, retries=2, `@daily`

Detalhes: ver `docs/architecture.md` e `sql/README.md`.

## Tecnologias

| Camada | v0.4.0 (atual) | Futuro (planejado) |
|--------|-----------------|-------------------|
| Linguagem | Python 3.10+ (type hints, pathlib, logging) | PySpark (adiado — over-engineering p/ 328 linhas) |
| Ingestão/Transformação | pandas + pyarrow (Parquet) | Delta Lake |
| Config | python-dotenv + `.env` (`DATABASE_URL`, `*_DATA_DIR`) | — |
| Testes | pytest + pytest-cov (26 testes; Postgres via SQLite) | Great Expectations / Pandera |
| Serving | PostgreSQL 15 (`sql/ddl`, `views`, `quality_checks`) + `load_postgres.py` | BI (Power BI/Looker) |
| Orquestração | Airflow 2.6 (`dags/`, `docker-compose.yml`) | — |
| Cloud | — | BigQuery sandbox (estágio 5) |

Nenhum recurso cloud é criado nesta etapa. Nenhum custo GCP/BigQuery existe.

## Estrutura do projeto

```text
financial-data-pipeline/
├── README.md
├── pyproject.toml
├── .env.example
├── .gitignore
├── data/
│   ├── raw/        # versionado: branches/products/customers.csv, accounts/transactions.json
│   ├── bronze/     # gerado localmente (gitignored): parquet por tabela/dia
│   ├── silver/     # gerado localmente (gitignored): clean + _quarantine por tabela/dia
│   └── gold/       # gerado localmente (gitignored): 4 marts por dia de ingestão
├── src/financial_pipeline/
│   ├── config.py               # configuração centralizada via .env (raw/bronze/silver/gold)
│   ├── ingestion/
│   │   ├── bronze.py           # raw → bronze (cópia + auditoria + validação)
│   │   └── schemas.py          # contrato de colunas + mapa tabela→arquivo
│   └── transformation/
│       ├── silver.py           # bronze → silver (tipagem + dedup + quarentena + FKs)
│       └── gold.py             # silver → gold (grão declarado + médias excl. outliers)
├── scripts/
│   ├── generate_synthetic_data.py
│   ├── run_bronze_ingestion.py
│   ├── run_silver.py
│   └── run_gold.py
├── sql/
│   ├── README.md
│   ├── ddl/            # 001_gold.sql (4 tabelas, NUMERIC p/ dinheiro, PK por dia)
│   ├── views/          # v_daily_volume, v_branch_ranking, v_ticket_by_type, v_kpis
│   └── quality_checks/ # 01_dup_pks, 02_null_keys, 03_business_rules, 04_freshness
├── dags/
│   └── financial_pipeline.py  # Airflow 2.6 (6 tasks, retries=2, @daily)
├── docker-compose.yml  # postgres:15 + airflow:2.6.3 (LocalExecutor)
├── tests/          # config + bronze + silver + gold + load_postgres + dag (26 testes)
└── docs/architecture.md
```

`notebooks/` e `gold/` seguem **omitidos de propósito** — serão criados
quando cada etapa começar.

## Como executar

Pré-requisitos: Python 3.10+, pip.

```bash
cd financial-data-pipeline

# 1. (Opcional) ambiente isolado
python -m venv .venv && source .venv/bin/activate

# 2. Dependências mínimas (pandas, pyarrow, dotenv, pytest)
pip install -e ".[dev]"

# 3. Configuração (opcional — defaults já funcionam)
cp .env.example .env

# 4. Gerar dados sintéticos (reprodutível via SYNTHETIC_SEED=42)
python scripts/generate_synthetic_data.py

# 5. Ingestão raw → Bronze
python scripts/run_bronze_ingestion.py
# Subconjuntos: python scripts/run_bronze_ingestion.py --tables branches customers
# Repartição fixa: python scripts/run_bronze_ingestion.py --date 2024-01-01

# 6. Transformação bronze → Silver
python scripts/run_silver.py
# Subconjuntos: python scripts/run_silver.py --tables transactions
# Dia específico: python scripts/run_silver.py --date 2024-01-01
# (default: última partição bronze de cada tabela)

# 7. Agregação silver → Gold
python scripts/run_gold.py
# Subconjuntos: python scripts/run_gold.py --tables fact_daily_volume outliers

# 8. Serving local (sem Docker: SQLite; com Docker: Postgres)
pip install -e ".[postgres]"  # psycopg + SQLAlchemy (só p/ Postgres real)
export DATABASE_URL=sqlite:////tmp/finance.db  # testes locais
python scripts/load_postgres.py                # DDL + carga (46 linhas)
python scripts/load_postgres.py --checks       # 01–03: 0 violações = pass

# Com Docker (requer Docker instalado):
docker compose up -d
export DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/finance
python scripts/load_postgres.py && python scripts/load_postgres.py --checks
# Airflow UI: http://localhost:8080 (admin/admin) → ative financial_data_pipeline

# 9. Testes
pytest
```

Saída esperada: 5 tabelas, 328 linhas Bronze → 328 Silver clean, 0 quarentena
→ Gold: `fact_daily_volume=28`, `agg_transaction_type=6`, `agg_branch=10`,
`outliers=2` (T999991/T999992 enriquecidos p/ investigação)
→ Postgres: 46 linhas (28+6+10+2), checks 01–03 com 0 violações.

## Próximas etapas

1. **BI/observabilidade:** dashboards sobre as views + métricas de run/alertas
   (conversa com o projeto 4 do portfólio).
2. **Orquestração/observabilidade:** Airflow + métricas de run
   (conversa com os projetos 2–4 do portfólio).
3. **Cloud/custo (estágio 5, adiado):** GCS + BigQuery sandbox
   (datasets `bronze`/`silver`/`gold`, tabelas particionadas por
   `ingestion_date`) com benchmark e estimativa separada de performance
   vs. custo (projeto 5). Nada será criado antes do estágio 5 — tabelas do
   sandbox expiram em 60 dias. Nada será criado sem aviso prévio sobre
   quotas/cobrança.

Todos os dados são sintéticos e fictícios. Nenhum dado real de clientes
ou bancos é utilizado.
