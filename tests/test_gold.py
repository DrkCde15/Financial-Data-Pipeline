"""Tests for silver -> gold aggregations (stage 3)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from financial_pipeline.transformation.gold import (
    GoldError,
    build_agg_branch,
    build_agg_transaction_type,
    build_fact_daily_volume,
    build_gold,
)


def _silver_frames() -> dict[str, pd.DataFrame]:
    branches = pd.DataFrame([
        {"branch_id": "B001", "branch_name": "Ag A", "city": "City A", "state": "SP"},
        {"branch_id": "B002", "branch_name": "Ag B", "city": "City B", "state": "RJ"},
    ])
    accounts = pd.DataFrame([
        {"account_id": "A1", "customer_id": "C1", "branch_id": "B001",
         "product_id": "P001", "open_date": "2023-01-01", "status": "active"},
        {"account_id": "A2", "customer_id": "C2", "branch_id": "B002",
         "product_id": "P001", "open_date": "2023-01-02", "status": "active"},
    ])
    customers = pd.DataFrame([
        {"customer_id": "C1", "full_name": "Ana", "birth_date": "1990-01-01",
         "branch_id": "B001", "is_active": True},
        {"customer_id": "C2", "full_name": "Bob", "birth_date": "1991-02-02",
         "branch_id": "B002", "is_active": True},
    ])
    transactions = pd.DataFrame([
        {"transaction_id": "T1", "account_id": "A1", "transaction_type": "deposit",
         "amount": 100.0, "timestamp": "2024-06-01T10:00:00Z", "status": "completed",
         "is_outlier": False},
        {"transaction_id": "T2", "account_id": "A1", "transaction_type": "deposit",
         "amount": 200.0, "timestamp": "2024-06-01T11:00:00Z", "status": "completed",
         "is_outlier": False},
        {"transaction_id": "T3", "account_id": "A2", "transaction_type": "fee",
         "amount": 999999.99, "timestamp": "2024-06-02T10:00:00Z", "status": "completed",
         "is_outlier": True},
    ])
    return {"branches": branches, "accounts": accounts,
            "customers": customers, "transactions": transactions}


def test_fact_daily_excludes_outliers_from_avg() -> None:
    """Outlier counted separately, avg over clean completed only."""
    frames = _silver_frames()
    fact = build_fact_daily_volume(frames["transactions"])
    assert len(fact) == 2  # 06-01 and 06-02
    day1 = fact[fact["transaction_date"] == pd.Timestamp("2024-06-01", tz="UTC")].iloc[0]
    assert day1["n_total"] == 2
    assert day1["avg_ticket_completed"] == 150.0
    day2 = fact[fact["transaction_date"] == pd.Timestamp("2024-06-02", tz="UTC")].iloc[0]
    assert day2["n_outliers"] == 1
    assert day2["avg_ticket_completed"] == 0.0  # only outlier that day
    assert day2["outlier_total"] == 999999.99


def test_agg_type_and_branch_grains() -> None:
    """Type grain is (transaction_type); branch grain is (branch_id) enriched."""
    frames = _silver_frames()
    by_type = build_agg_transaction_type(frames["transactions"])
    assert set(by_type["transaction_type"]) == {"deposit"}  # fee row is outlier
    assert by_type.iloc[0]["total"] == 300.0
    by_branch = build_agg_branch(frames["transactions"], frames["accounts"], frames["branches"])
    assert len(by_branch) == 1  # only B001 has clean completed volume
    assert by_branch.iloc[0]["branch_id"] == "B001"
    assert by_branch.iloc[0]["total_volume"] == 300.0
    assert by_branch.iloc[0]["n_accounts"] == 1


@pytest.fixture
def silver_dir(tmp_path: Path) -> Path:
    """Write Silver fixtures to disk partitioned by ingestion_date."""
    silver = tmp_path / "silver"
    frames = _silver_frames()
    frames["products"] = pd.DataFrame(
        [{"product_id": "P001", "product_name": "Conta", "product_type": "account"}]
    )
    for t, df in frames.items():
        out = silver / t / "ingestion_date=2024-01-01"
        out.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out / "data.parquet", index=False)
    return silver


def test_build_gold_end_to_end_and_idempotent(silver_dir: Path, tmp_path: Path) -> None:
    """Full build writes 4 tables; re-run overwrites with same counts."""
    gold = tmp_path / "gold"
    first = build_gold(silver_dir, gold, ingestion_date="2024-01-01")
    assert {s["table"] for s in first} == {
        "fact_daily_volume", "agg_transaction_type", "agg_branch", "outliers"
    }
    for s in first:
        assert Path(s["output_path"]).exists()
    out = {s["table"]: s["rows"] for s in first}
    assert out["outliers"] == 1
    second = build_gold(silver_dir, gold, ingestion_date="2024-01-01")
    assert {s["rows"] for s in second} == set(out.values())


def test_build_gold_missing_silver_raises(tmp_path: Path) -> None:
    """Absent silver -> GoldError (fail-fast)."""
    with pytest.raises(GoldError, match="not found"):
        build_gold(tmp_path / "nope", tmp_path / "gold")


def test_build_gold_unknown_table_raises(silver_dir: Path, tmp_path: Path) -> None:
    """Unknown gold table -> GoldError."""
    with pytest.raises(GoldError, match="Unknown gold tables"):
        build_gold(silver_dir, tmp_path / "gold", tables=["dim_nope"])
