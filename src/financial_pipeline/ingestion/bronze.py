"""Raw -> Bronze ingestion (stage 1 of the medallion pipeline).

Bronze semantics (intentionally minimal):
  - Exact copy of raw content (no cleaning, no dedup, no type coercion).
  - Adds audit columns: `_ingested_at` (UTC) and `_source_file`.
  - Writes one Parquet dataset per table under:
        bronze/<table>/ingestion_date=YYYY-MM-DD/data.parquet
  - Validates presence of expected columns (fail-fast on contract breach).
  - Overwrites the day partition (idempotent re-runs).

Future stages (NOT implemented here):
  - Silver: cleaning/typing/dedup.
  - Gold: aggregations + PostgreSQL serving.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from financial_pipeline.ingestion.schemas import EXPECTED_COLUMNS, SOURCE_FILES

logger = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    """Raised when a raw source cannot be ingested to Bronze."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_raw_file(raw_path: Path) -> pd.DataFrame:
    """Read a CSV or JSON raw file into a DataFrame."""
    if not raw_path.exists():
        raise IngestionError(f"Raw file not found: {raw_path}")
    try:
        if raw_path.suffix.lower() == ".csv":
            return pd.read_csv(raw_path)
        if raw_path.suffix.lower() == ".json":
            return pd.read_json(raw_path)
    except Exception as exc:
        raise IngestionError(f"Failed to read {raw_path}: {exc}") from exc
    raise IngestionError(f"Unsupported raw format: {raw_path.suffix} ({raw_path.name})")


def validate_columns(table: str, df: pd.DataFrame) -> None:
    """Fail-fast check: all expected columns must be present (extras allowed)."""
    expected = EXPECTED_COLUMNS.get(table)
    if expected is None:
        raise IngestionError(f"Unknown bronze table: {table!r}")
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise IngestionError(f"Table {table!r} missing columns: {missing}")
    if df.empty:
        raise IngestionError(f"Table {table!r} is empty (0 rows in raw file)")


def ingest_table_to_bronze(
    table: str,
    raw_dir: Path,
    bronze_dir: Path,
    ingestion_date: str | None = None,
) -> dict:
    """Ingest a single table from raw to bronze. Returns a summary dict."""
    if table not in SOURCE_FILES:
        raise IngestionError(f"Unknown table: {table!r}")

    source_file = SOURCE_FILES[table]
    raw_path = raw_dir / source_file
    logger.info("Ingesting %s from %s", table, raw_path.name)

    df = read_raw_file(raw_path)
    validate_columns(table, df)

    df = df.copy()
    df["_ingested_at"] = _utc_now_iso()
    df["_source_file"] = source_file

    day = ingestion_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = bronze_dir / table / f"ingestion_date={day}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    try:
        df.to_parquet(out_path, index=False)
    except Exception as exc:
        raise IngestionError(f"Failed to write bronze {out_path}: {exc}") from exc

    summary = {
        "table": table,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "source_file": source_file,
        "output_path": str(out_path),
        "ingestion_date": day,
    }
    logger.info("Bronze OK: %s rows=%d -> %s", table, len(df), out_path)
    return summary


def run_bronze_ingestion(
    raw_dir: Path,
    bronze_dir: Path,
    tables: list[str] | None = None,
    ingestion_date: str | None = None,
) -> list[dict]:
    """Ingest all (or selected) tables. Raises IngestionError on first failure."""
    selected = tables or sorted(SOURCE_FILES)
    unknown = [t for t in selected if t not in SOURCE_FILES]
    if unknown:
        raise IngestionError(f"Unknown tables requested: {unknown}")
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return [
        ingest_table_to_bronze(t, raw_dir, bronze_dir, ingestion_date) for t in selected
    ]
