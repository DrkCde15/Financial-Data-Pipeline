-- Gold DDL (PostgreSQL 15+). Idempotent: CREATE IF NOT EXISTS + loader faz
-- DELETE WHERE ingestion_date antes do INSERT (re-run seguro).
-- Tipos: NUMERIC(18,2) p/ dinheiro (nunca float), TIMESTAMPTZ p/ eventos UTC.

CREATE TABLE IF NOT EXISTS fact_daily_volume (
    transaction_date     DATE            NOT NULL,
    n_total              INTEGER         NOT NULL CHECK (n_total >= 0),
    n_completed          INTEGER         NOT NULL CHECK (n_completed >= 0),
    total_completed      NUMERIC(18,2)   NOT NULL,
    avg_ticket_completed NUMERIC(18,2)   NOT NULL,
    n_outliers           INTEGER         NOT NULL DEFAULT 0 CHECK (n_outliers >= 0),
    outlier_total        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    ingestion_date       DATE            NOT NULL,
    _built_at        TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_date, ingestion_date)
);

CREATE TABLE IF NOT EXISTS agg_transaction_type (
    transaction_type TEXT            NOT NULL,
    n                INTEGER         NOT NULL CHECK (n > 0),
    total            NUMERIC(18,2)   NOT NULL,
    avg_ticket       NUMERIC(18,2)   NOT NULL,
    ingestion_date   DATE            NOT NULL,
    _built_at        TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_type, ingestion_date)
);

CREATE TABLE IF NOT EXISTS agg_branch (
    branch_id      TEXT            NOT NULL,
    n_transactions INTEGER         NOT NULL CHECK (n_transactions > 0),
    total_volume   NUMERIC(18,2)   NOT NULL,
    avg_ticket     NUMERIC(18,2)   NOT NULL,
    n_accounts     INTEGER         NOT NULL CHECK (n_accounts > 0),
    branch_name    TEXT,
    city           TEXT,
    state          CHAR(2),
    ingestion_date DATE            NOT NULL,
    _built_at      TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (branch_id, ingestion_date)
);

CREATE TABLE IF NOT EXISTS outliers (
    transaction_id   TEXT            NOT NULL,
    amount           NUMERIC(18,2)   NOT NULL,
    "timestamp"      TIMESTAMPTZ     NOT NULL,
    status           TEXT            NOT NULL,
    transaction_type TEXT            NOT NULL,
    account_id       TEXT,
    customer_id      TEXT,
    branch_id        TEXT,
    branch_name      TEXT,
    full_name        TEXT,
    ingestion_date   DATE            NOT NULL,
    _built_at        TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, ingestion_date)
);

-- Índices p/ consumo (latest partition + ordenação típica de dashboard).
CREATE INDEX IF NOT EXISTS ix_fact_daily_volume_ingestion_date
    ON fact_daily_volume (ingestion_date, transaction_date);
CREATE INDEX IF NOT EXISTS ix_agg_branch_ingestion_date
    ON agg_branch (ingestion_date, total_volume DESC);
CREATE INDEX IF NOT EXISTS ix_outliers_ingestion_date
    ON outliers (ingestion_date, amount DESC);
