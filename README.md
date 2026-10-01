# Pipeline de Dados Financeiros

## Objetivo

Construir um pipeline de dados bancários de uma cooperativa financeira
**fictícia**, seguindo arquitetura medallion (Bronze → Silver → Gold),
para demonstrar competências de Data Engineering Jr/Pleno: SQL, Python,
PySpark, Delta Lake, BigQuery, ETL/ELT e data quality.

**Escopo desta versão (v0.7.0 — pipeline + BI como código):**
`raw → Bronze → Silver → Gold → BigQuery sandbox → Streamlit`,
agendado por Airflow 2.6, executável local via `podman compose`, sem Spark.
Fonte 100% simulada. Serving único: BigQuery (o Postgres do compose é só
o banco de metadados do Airflow — não entra no pipeline).

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
                                                     BigQuery sandbox (14 tabs + views + checks)
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
  (só calcula — não encosta no banco)
- Gold (grão declarado, completed excl. outliers nas médias):
  `fact_daily_volume(transaction_date)`, `agg_transaction_type(transaction_type)`,
  `agg_branch(branch_id)`, `outliers(transaction_id)` enriquecida p/ investigação
- `scripts/load_bigquery.py` carrega as 3 camadas → BigQuery `engdta`
  (load jobs Parquet WRITE_TRUNCATE por tabela, idempotente) + `sql/bigquery`
  (views v_* sobre `max(ingestion_date)`, checks 01–03: 0 rows = pass; 04: frescor)
- `dags/financial_pipeline.py` (Airflow 2.6): 6 tasks
  `generate >> bronze >> silver >> gold >> bq_load >> bq_checks`, retries=2, `@daily`

Detalhes: ver `docs/architecture.md` e `sql/README.md`.

## Tecnologias

| Camada | v0.6.0 (atual) | Futuro (planejado) |
|--------|-----------------|-------------------|
| Linguagem | Python 3.10+ (type hints, pathlib, logging) | PySpark (adiado — over-engineering p/ 328 linhas) |
| Ingestão/Transformação | pandas + pyarrow (Parquet) | Delta Lake |
| Config | python-dotenv + `.env` (`GCP_PROJECT`, `*_DATA_DIR`) | — |
| Testes | pytest + pytest-cov (BQ mockado, sem sandbox no CI) | Great Expectations / Pandera |
| Serving | BigQuery sandbox `engdta` (`sql/bigquery`, `load_bigquery.py` via lib) | — |
| BI | Streamlit (`dashboard/`: KPIs, série, ranking, outliers; BQ ou parquet local) | Looker Studio (link público) |
| Orquestração | Airflow 2.6 (`dags/`, 6 tasks, `docker-compose.yml`, podman) | — |
| Cloud extra | — | Benchmark pago / particionamento (fora do sandbox) |

Sandbox é grátis (free tier: 1 TiB queries + 10 GiB/mês); o projeto ocupa ~88 KiB.

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
│   ├── run_gold.py
│   └── load_bigquery.py  # camadas locais → BQ (load jobs WRITE_TRUNCATE + checks)
├── sql/
│   ├── README.md
│   └── bigquery/
│       ├── views/          # v_daily_volume, v_branch_ranking, v_ticket_by_type, v_kpis
│       └── checks/         # 01_dup_pks, 02_null_keys, 03_business_rules, 04_freshness
├── dags/
│   └── financial_pipeline.py  # Airflow 2.6 (6 tasks lineares, retries=2, @daily)
├── docker-compose.yml  # postgres:15 (só metadados do Airflow) + airflow:2.6.3
├── dashboard/
│   ├── app.py            # Streamlit (KPIs, série, ranking, outliers)
│   ├── data.py           # carga BQ + fallback local (testável, sem streamlit)
│   └── requirements.txt
├── tests/          # config + bronze + silver + gold + bigquery + dag + dashboard
└── docs/architecture.md
```

`notebooks/` segue **omitido de propósito** — será criado na etapa BI.

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

# 8. Serving — BigQuery sandbox (extra `cloud`)
pip install -e ".[cloud]"                       # google-cloud-bigquery
gcloud auth application-default login          # ADC (uma vez; no Airflow vai via mount)
python scripts/load_bigquery.py            # datasets + 14 tabelas + views
python scripts/load_bigquery.py --checks   # 01–03: 0 violações = pass
# Só Gold/serving: python scripts/load_bigquery.py --layer gold

# 9. BI — Streamlit (dashboard como código, sem clique manual)
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py                 # BQ via ADC
DATA_SOURCE=local streamlit run dashboard/app.py  # offline (data/gold/)

# 10. Orquestração (requer podman/docker)
podman compose up -d
# Airflow UI: http://localhost:8080 (admin/admin) → ative financial_data_pipeline

# 11. Testes
pytest
```

Saída esperada: 5 tabelas, 328 linhas Bronze → 328 Silver clean, 0 quarentena
→ Gold: `fact_daily_volume=28`, `agg_transaction_type=6`, `agg_branch=10`,
`outliers=2` (T999991/T999992 enriquecidos p/ investigação)
→ BigQuery `engdta` (US, sandbox): 14 tabelas (702 linhas) + 4 views, checks zerados.
