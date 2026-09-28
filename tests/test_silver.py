"""Tests for bronze -> silver transformation (stage 2)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from financial_pipeline.transformation.silver import (
    SilverError,
    clean_transactions,
    run_silver,
    transform_table_to_silver,
)


@pytest.fixture
def bronze_fixture(tmp_path: Path) -> Path:
    """Create minimal bronze partitions (branches + customers + accounts + transactions)."""
    bronze = tmp_path / "bronze"
    audit = {"_ingested_at": "2024-01-01T00:00:00+00:00", "_source_file": "x"}

    def _write(table: str, rows: list[dict]) -> None:
        df = pd.DataFrame(rows)
        for k, v in audit.items():
            df[k] = v
        out = bronze / table / "ingestion_date=2024-01-01"
        out.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out / "data.parquet", index=False)

    _write("branches", [
        {"branch_id": "B001", "branch_name": " Ag A ", "city": "City A", "state": "sp"},
        {"branch_id": "B001", "branch_name": "Ag A dup", "city": "City A", "state": "SP"},
    ])
    _write("products", [
        {"product_id": "P001", "product_name": "Conta", "product_type": "Account"},
    ])
    _write("customers", [
        {"customer_id": "C0001", "full_name": "Ana", "birth_date": "1990-01-15",
         "branch_id": "B001", "is_active": True},
        {"customer_id": "C0002", "full_name": "Bob", "birth_date": "not-a-date",
         "branch_id": "B001", "is_active": True},
        {"customer_id": "C0003", "full_name": "Zed", "birth_date": "1980-05-01",
         "branch_id": "B999", "is_active": True},  # orphan branch -> FK quarantine
    ])
    _write("accounts", [
        {"account_id": "A00001", "customer_id": "C0001", "branch_id": "B001",
         "product_id": "P001", "open_date": "2023-01-10", "status": " Active "},
        {"account_id": "A00002", "customer_id": "C999", "branch_id": "B001",
         "product_id": "P001", "open_date": "2023-02-01", "status": "active"},
    ])
    _write("transactions", [
        {"transaction_id": "T000001", "account_id": "A00001", "transaction_type": "DEPOSIT",
         "amount": 100.555, "timestamp": "2024-06-01T10:00:00", "status": "Completed"},
        {"transaction_id": "T000002", "account_id": "A999", "transaction_type": "deposit",
         "amount": 10.0, "timestamp": "2024-06-01T11:00:00", "status": "completed"},
        {"transaction_id": "T000003", "account_id": "A00001", "transaction_type": "deposit",
         "amount": 999999.99, "timestamp": "2024-06-15T10:00:00", "status": "completed"},
    ])
    return bronze


def test_clean_transactions_types_and_outlier_flag() -> None:
    """Amount rounded to 2 decimals, timestamp UTC, outlier flagged not dropped."""
    df = pd.DataFrame([
        {"transaction_id": "T1", "account_id": "A1", "transaction_type": "deposit",
         "amount": "100.555", "timestamp": "2024-06-01T10:00:00", "status": "completed"},
        {"transaction_id": "T2", "account_id": "A1", "transaction_type": "deposit",
         "amount": 999999.99, "timestamp": "2024-06-15T10:00:00", "status": "completed"},
    ])
    clean, quar, _ = clean_transactions(df)
    assert quar.empty
    assert len(clean) == 2  # outlier kept
    assert float(clean.loc[clean["transaction_id"] == "T1", "amount"].iloc[0]) == 100.56
    assert bool(clean.loc[clean["transaction_id"] == "T2", "is_outlier"].iloc[0]) is True
    assert str(clean["timestamp"].dtype).endswith("[UTC]") or "utc" in str(clean["timestamp"].dtype).lower()


def test_transform_dedups_and_normalizes(bronze_fixture: Path, tmp_path: Path) -> None:
    """Branches dedup by PK + state UPPER + stripped names."""
    silver = tmp_path / "silver"
    summary = transform_table_to_silver("branches", bronze_fixture, silver, ingestion_date="2024-01-01")
    assert summary["rows_in"] == 2
    assert summary["rows_clean"] == 1
    df = pd.read_parquet(Path(summary["output_path"]))
    assert df.iloc[0]["state"] == "SP"
    assert df.iloc[0]["branch_name"] == "Ag A"
    assert "_cleaned_at" in df.columns


def test_run_silver_applies_fk_quarantine(bronze_fixture: Path, tmp_path: Path) -> None:
    """Orphan FKs (C0003->B999, A00002->C999, T000002->A999) land in quarantine."""
    silver = tmp_path / "silver"
    summaries = run_silver(bronze_fixture, silver, ingestion_date="2024-01-01")
    by_table = {s["table"]: s for s in summaries}
    # customers: 3 in (1 bad date quarantined, 1 FK orphan) -> 1 clean
    assert by_table["customers"]["rows_clean"] == 1
    assert by_table["customers"]["rows_quarantined"] == 2
    # accounts: A00002 orphan customer -> quarantined
    assert by_table["accounts"]["rows_clean"] == 1
    # transactions: T000002 orphan account -> quarantined, outlier T000003 kept
    assert by_table["transactions"]["rows_clean"] == 2
    assert by_table["transactions"]["rows_quarantined"] == 1
    q_path = Path(by_table["transactions"]["quarantine_path"])
    assert q_path.exists()
    qdf = pd.read_parquet(q_path)
    assert "_quarantine_reason" in qdf.columns


def test_run_silver_is_idempotent(bronze_fixture: Path, tmp_path: Path) -> None:
    """Re-running the same partition overwrites with same counts."""
    silver = tmp_path / "silver"
    first = run_silver(bronze_fixture, silver, tables=["products"], ingestion_date="2024-01-01")
    second = run_silver(bronze_fixture, silver, tables=["products"], ingestion_date="2024-01-01")
    assert first[0]["rows_clean"] == second[0]["rows_clean"] == 1


def test_missing_bronze_raises(tmp_path: Path) -> None:
    """Absent bronze partition -> SilverError (fail-fast)."""
    with pytest.raises(SilverError, match="not found"):
        transform_table_to_silver("branches", tmp_path / "nope", tmp_path / "silver")
