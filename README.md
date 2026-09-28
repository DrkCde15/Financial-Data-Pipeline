# Pipeline de Dados Financeiros Simulado

## Objetivo

Construir um pipeline de dados bancários de uma cooperativa financeira
**fictícia**, seguindo arquitetura medallion (Bronze → Silver → Gold),
para demonstrar competências de Data Engineering Jr/Pleno: SQL, Python,
PySpark, Delta Lake, PostgreSQL, BigQuery, ETL/ELT e data quality.

**Escopo desta versão (v0.2.0 — Etapas 1–2):** ingestão `raw → Bronze` +
transformação `Bronze → Silver`, executável 100% local, sem Spark, sem banco
e sem cloud. Fonte 100% simulada até fechar Silver/Gold (decisão).

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
                                              Silver (limpeza + quarentena)
                                                        ↓
                                              Gold (futuro: agregações)
                                                        ↓
                                              PostgreSQL / SQL / relatórios
```

Etapas 1–2 implementadas:

- `scripts/generate_synthetic_data.py` gera 5 tabelas fictícias em `data/raw/`
- `scripts/run_bronze_ingestion.py` copia para `data/bronze/<tabela>/ingestion_date=.../data.parquet`
- Bronze adiciona apenas `_ingested_at` e `_source_file` (sem limpeza)
- Validação de contrato (colunas esperadas) com falha rápida
- `scripts/run_silver.py` limpa para `data/silver/<tabela>/ingestion_date=.../data.parquet`
- Silver: dedup por PK, tipagem (`birth_date`/`open_date`→date, `timestamp`→UTC,
  `amount` round 2 + `is_outlier`, `is_active`→bool), status normalizado,
  FKs validadas; rejeitos em `data/silver/_quarantine/<tabela>/` com `_quarantine_reason`

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
│   └── silver/     # gerado localmente (gitignored): clean + _quarantine por tabela/dia
├── src/financial_pipeline/
│   ├── config.py               # configuração centralizada via .env (raw/bronze/silver)
│   ├── ingestion/
│   │   ├── bronze.py           # raw → bronze (cópia + auditoria + validação)
│   │   └── schemas.py          # contrato de colunas + mapa tabela→arquivo
│   └── transformation/
│       └── silver.py           # bronze → silver (tipagem + dedup + quarentena + FKs)
├── scripts/
│   ├── generate_synthetic_data.py
│   ├── run_bronze_ingestion.py
│   └── run_silver.py
├── sql/README.md   # placeholder documentado (Gold/PostgreSQL futuro)
├── tests/          # test_config + test_bronze_ingestion + test_silver (15 testes)
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

# 7. Testes
pytest
```

Saída esperada: 5 tabelas, 328 linhas Bronze → 328 Silver clean, 0 quarentena
(branches=10, products=6, customers=50, accounts=60, transactions=202;
`is_outlier=true` em T999991/T999992 para a Gold).

## Próximas etapas

1. **Gold (agregações):** volume por dia, saldo médio por agência, clientes
   ativos, ticket médio, transações por tipo, detecção de outliers
   (os 2 outliers intencionais em `transactions.json` servem de fixture).
3. **Serving:** carga no PostgreSQL + `sql/` com DDL, views e checks.
4. **Orquestração/observabilidade:** Airflow + métricas de run
   (conversa com os projetos 2–4 do portfólio).
5. **Cloud/custo (estágio 5, adiado):** GCS + BigQuery sandbox
   (datasets `bronze`/`silver`/`gold`, tabelas particionadas por
   `ingestion_date`) com benchmark e estimativa separada de performance
   vs. custo (projeto 5). Nada será criado antes do estágio 5 — tabelas do
   sandbox expiram em 60 dias. Nada será criado sem aviso prévio sobre
   quotas/cobrança.

Todos os dados são sintéticos e fictícios. Nenhum dado real de clientes
ou bancos é utilizado.
