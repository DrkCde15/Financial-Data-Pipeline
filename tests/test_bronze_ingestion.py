"""Tests for raw -> bronze ingestion (stage 1)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from financial_pipeline.ingestion.bronze import (
    IngestionError,
    ingest_table_to_bronze,
    run_bronze_ingestion,
    validate_columns,
)
from financial_pipeline.ingestion.schemas import EXPECTED_COLUMNS


@pytest.fixture
def tiny_raw(tmp_path: Path) -> Path:
    """Create a minimal raw fixture (branches + customers only)."""
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "branches.csv").write_text(
        "branch_id,branch_name,city,state\n"
        "B001,Agencia Ficticia A,City A,SP\n"
        "B002,Agencia Ficticia B,City B,RJ\n",
        encoding="utf-8",
    )
    (raw / "customers.csv").write_text(
        "customer_id,full_name,birth_date,branch_id,is_active\n"
        "C0001,Ana Silva,1990-01-15,B001,True\n"
        "C0002,Bruno Souza,1985-05-20,B002,False\n",
        encoding="utf-8",
    )
    (raw / "products.csv").write_text(
        "product_id,product_name,product_type\nP001,Conta Corrente Simples,account\n",
        encoding="utf-8",
    )
    (raw / "accounts.json").write_text(
        '[{"account_id": "A00001", "customer_id": "C0001", "branch_id": "B001",'
        ' "product_id": "P001", "open_date": "2023-01-10", "status": "active"}]',
        encoding="utf-8",
    )
    (raw / "transactions.json").write_text(
        '[{"transaction_id": "T000001", "account_id": "A00001",'
        ' "transaction_type": "deposit", "amount": 100.0,'
        ' "timestamp": "2024-06-01T10:00:00", "status": "completed"}]',
        encoding="utf-8",
    )
    return raw


def test_validate_columns_ok() -> None:
    """Expected columns present -> no error."""
    df = pd.DataFrame({c: [1] for c in EXPECTED_COLUMNS["branches"]})
    validate_columns("branches", df)  # must not raise


def test_validate_columns_missing() -> None:
    """Missing column -> IngestionError."""
    df = pd.DataFrame({"branch_id": ["B001"]})
    with pytest.raises(IngestionError, match="missing columns"):
        validate_columns("branches", df)


def test_ingest_table_adds_audit_columns(tiny_raw: Path, tmp_path: Path) -> None:
    """Bronze output preserves rows and adds _ingested_at/_source_file."""
    bronze = tmp_path / "bronze"
    summary = ingest_table_to_bronze("branches", tiny_raw, bronze, ingestion_date="2024-01-01")

    assert summary["rows"] == 2
    out = Path(summary["output_path"])
    assert out.exists()

    df = pd.read_parquet(out)
    assert len(df) == 2
    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns
    assert (df["_source_file"] == "branches.csv").all()


def test_ingest_is_idempotent(tiny_raw: Path, tmp_path: Path) -> None:
    """Re-running the same partition overwrites (same row count)."""
    bronze = tmp_path / "bronze"
    first = ingest_table_to_bronze("customers", tiny_raw, bronze, ingestion_date="2024-01-01")
    second = ingest_table_to_bronze("customers", tiny_raw, bronze, ingestion_date="2024-01-01")
    assert first["rows"] == second["rows"] == 2
    df = pd.read_parquet(Path(second["output_path"]))
    assert len(df) == 2


def test_run_bronze_ingestion_all_tables(tiny_raw: Path, tmp_path: Path) -> None:
    """Full run ingests the 5 contracted tables."""
    bronze = tmp_path / "bronze"
    summaries = run_bronze_ingestion(tiny_raw, bronze, ingestion_date="2024-01-01")
    assert {s["table"] for s in summaries} == {
        "branches", "products", "customers", "accounts", "transactions"
    }
    for s in summaries:
        assert Path(s["output_path"]).exists()
        assert s["rows"] >= 1


def test_ingest_missing_file_raises(tmp_path: Path) -> None:
    """Absent raw file -> IngestionError (fail-fast)."""
    raw = tmp_path / "empty_raw"
    raw.mkdir()
    bronze = tmp_path / "bronze"
    with pytest.raises(IngestionError, match="not found"):
        ingest_table_to_bronze("branches", raw, bronze)


def test_ingest_unknown_table_raises(tiny_raw: Path, tmp_path: Path) -> None:
    """Unknown table name -> IngestionError."""
    with pytest.raises(IngestionError, match="Unknown table"):
        ingest_table_to_bronze("gold_facts", tiny_raw, tmp_path / "bronze")
