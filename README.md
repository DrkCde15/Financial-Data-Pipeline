# Pipeline de Dados Financeiros Simulado

## Objetivo

Construir um pipeline de dados bancários de uma cooperativa financeira
**fictícia**, seguindo arquitetura medallion (Bronze → Silver → Gold),
para demonstrar competências de Data Engineering Jr/Pleno: SQL, Python,
PySpark, Delta Lake, PostgreSQL, ETL/ELT e data quality.

**Escopo desta versão (v0.1.0 — Etapa 1):** somente ingestão `raw → Bronze`,
executável 100% local, sem Spark, sem banco e sem cloud.

## Problema

Portfólios de dados frequentemente mostram notebooks isolados, sem pipeline
reprodutível, sem contrato de dados, sem testes e sem caminho de evolução
para produção (orquestração, qualidade, observabilidade, cloud).
Este projeto resolve isso começando pela fundação: dados sintéticos
versionados, ingestão idempotente com auditoria, configuração centralizada
e testes automatizados.

## Arquitetura

```text
JSON/CSV simulado → Ingestão (Python/pandas) → Bronze (Parquet particionado)
                                                        ↓
                                              Silver (futuro: PySpark/Delta)
                                                        ↓
                                              Gold (futuro: agregações)
                                                        ↓
                                              PostgreSQL / SQL / relatórios
```

Etapa 1 implementada:

- `scripts/generate_synthetic_data.py` gera 5 tabelas fictícias em `data/raw/`
- `scripts/run_bronze_ingestion.py` copia para `data/bronze/<tabela>/ingestion_date=.../data.parquet`
- Bronze adiciona apenas `_ingested_at` e `_source_file` (sem limpeza)
- Validação de contrato (colunas esperadas) com falha rápida

Detalhes: ver `docs/architecture.md` e `sql/README.md` (placeholder da Gold).

## Tecnologias

| Camada | Etapa 1 (atual) | Futuro (planejado) |
|--------|-----------------|-------------------|
| Linguagem | Python 3.10+ (type hints, pathlib, logging) | PySpark |
| Ingestão | pandas + pyarrow (Parquet) | Delta Lake |
| Config | python-dotenv + `.env` | — |
| Testes | pytest + pytest-cov | Great Expectations / Pandera |
| Serving | — | PostgreSQL, Spark SQL |
| Orquestração | scripts CLI | Airflow |
| Cloud | — | AWS S3/Glue/Athena, Databricks (com aviso de custo) |

Nenhum recurso cloud é criado nesta etapa. Nenhum custo AWS existe.

## Estrutura do projeto

```text
financial-data-pipeline/
├── README.md
├── pyproject.toml
├── .env.example
├── .gitignore
├── data/
│   ├── raw/        # versionado: branches/products/customers.csv, accounts/transactions.json
│   └── bronze/     # gerado localmente (gitignored): parquet por tabela/dia
├── src/financial_pipeline/
│   ├── config.py               # configuração centralizada via .env
│   └── ingestion/
│       ├── bronze.py           # raw → bronze (cópia + auditoria + validação)
│       └── schemas.py          # contrato de colunas + mapa tabela→arquivo
├── scripts/
│   ├── generate_synthetic_data.py
│   └── run_bronze_ingestion.py
├── sql/README.md   # placeholder documentado (Gold/PostgreSQL futuro)
├── tests/          # test_config + test_bronze_ingestion (10 testes)
└── docs/architecture.md
```

Diretórios como `notebooks/`, `transformation/`, `silver/`, `gold/` foram
**omitidos de propósito** — serão criados quando cada etapa começar, para
evitar placeholders vazios sem função.

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

# 6. Testes
pytest
```

Saída esperada da ingestão: 5 tabelas, 328 linhas
(branches=10, products=6, customers=50, accounts=60, transactions=202).

## Próximas etapas

1. **Silver (limpeza):** tipagem (`amount`→decimal, `timestamp`→timestamp),
   tratamento de nulos, remoção de duplicados, normalização de `status`,
   PySpark + Delta Lake, testes de schema (Pandera/GE).
2. **Gold (agregações):** volume por dia, saldo médio por agência, clientes
   ativos, ticket médio, transações por tipo, detecção de outliers
   (os 2 outliers intencionais em `transactions.json` servem de fixture).
3. **Serving:** carga no PostgreSQL + `sql/` com DDL, views e checks.
4. **Orquestração/observabilidade:** Airflow + métricas de run
   (conversa com os projetos 2–4 do portfólio).
5. **Cloud/custo:** S3 + Athena/Databricks com benchmark e estimativa
   separada de performance vs. custo (projeto 5). Nada será criado sem
   aviso prévio sobre possível cobrança.

Todos os dados são sintéticos e fictícios. Nenhum dado real de clientes
ou bancos é utilizado.
