"""Tests for Gold -> PostgreSQL loader (stage 4, SQLite stand-in)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from scripts.load_postgres import apply_ddl, load_table, run_checks


def _gold_fixture(tmp_path: Path) -> Path:
    gold = tmp_path / "gold"
    day = "2024-01-01"
    fact = pd.DataFrame([{
        "transaction_date": "2024-06-01", "n_total": 2, "n_completed": 2,
        "total_completed": 300.0, "avg_ticket_completed": 150.0,
        "n_outliers": 0, "outlier_total": 0.0, "_built_at": "2024-01-01T00:00:00+00:00",
    }])
    by_type = pd.DataFrame([{
        "transaction_type": "deposit", "n": 2, "total": 300.0,
        "avg_ticket": 150.0, "_built_at": "2024-01-01T00:00:00+00:00",
    }])
    branch = pd.DataFrame([{
        "branch_id": "B001", "n_transactions": 2, "total_volume": 300.0,
        "avg_ticket": 150.0, "n_accounts": 1, "branch_name": "Ag A",
        "city": "City", "state": "SP", "_built_at": "2024-01-01T00:00:00+00:00",
    }])
    out = pd.DataFrame([{
        "transaction_id": "T999991", "amount": 999999.99,
        "timestamp": "2024-06-15T10:00:00+00:00", "status": "completed",
        "transaction_type": "deposit", "account_id": "A1", "customer_id": "C1",
        "branch_id": "B001", "branch_name": "Ag A", "full_name": "Ana",
        "_built_at": "2024-01-01T00:00:00+00:00",
    }])
    for t, df in {"fact_daily_volume": fact, "agg_transaction_type": by_type,
                  "agg_branch": branch, "outliers": out}.items():
        d = gold / t / f"ingestion_date={day}"
        d.mkdir(parents=True, exist_ok=True)
        df.to_parquet(d / "data.parquet", index=False)
    return gold


def test_ddl_and_load_are_idempotent(tmp_path: Path) -> None:
    """DDL + DELETE+INSERT duas vezes = mesmas linhas (sem duplicar)."""
    from financial_pipeline.config import get_project_root

    gold = _gold_fixture(tmp_path)
    db = tmp_path / "finance.db"
    con = sqlite3.connect(str(db))
    apply_ddl(con, get_project_root())
    s1 = load_table(con, gold, "fact_daily_volume", "2024-01-01")
    s2 = load_table(con, gold, "fact_daily_volume", "2024-01-01")
    assert s1["rows"] == s2["rows"] == 1
    n = con.execute("SELECT count(*) FROM fact_daily_volume").fetchone()[0]
    assert n == 1
    con.close()


def test_checks_pass_on_clean_gold(tmp_path: Path) -> None:
    """01-03 com 0 violações em Gold limpa."""
    from financial_pipeline.config import get_project_root

    gold = _gold_fixture(tmp_path)
    db = tmp_path / "finance.db"
    con = sqlite3.connect(str(db))
    apply_ddl(con, get_project_root())
    for t in ["fact_daily_volume", "agg_transaction_type", "agg_branch", "outliers"]:
        load_table(con, gold, t, "2024-01-01")
    res = run_checks(con, get_project_root())
    assert res["01_dup_pks.sql"]["ok"] is True
    assert res["02_null_keys.sql"]["ok"] is True
    assert res["03_business_rules.sql"]["ok"] is True
    con.close()
