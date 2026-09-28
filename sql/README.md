# sql/ — placeholder documentado

Esta pasta está reservada para a etapa **Gold / PostgreSQL** (futura).

Nada foi criado aqui na etapa 1 (ingestão → Bronze) de propósito:
o pipeline ainda não possui camada Silver/Gold nem banco relacional.

Planejado para etapas futuras:

- `ddl/` — DDL das tabelas Gold (ex.: `fact_transactions_daily`, `dim_customers`)
- `views/` — views analíticas (volume por dia, saldo médio por agência, ticket médio)
- `quality_checks/` — consultas de data quality (nulos, duplicados, outliers)
- `seeds/` — cargas iniciais pequenas, se necessário

Enquanto isso, as análises futuras previstas são:

- volume de transações por dia
- saldo médio por agência
- quantidade de clientes ativos
- ticket médio
- transações por tipo
- valores suspeitos/outliers
