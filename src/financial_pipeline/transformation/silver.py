"""Bronze -> Silver transformation (stage 2 of the medallion pipeline).

Silver semantics (pandas, local):
  - Reads one Bronze partition:
        bronze/<table>/ingestion_date=YYYY-MM-DD/data.parquet
  - Cleans WITHOUT dropping business outliers:
      * strip strings, state UPPER, status/transaction_type lower
      * drop duplicates by PK (keep first, count them)
      * quarantine PK nulls (never silently drop)
      * cast dates (birth_date/open_date -> date, timestamp -> UTC datetime)
      * amount -> numeric rounded to 2 decimals (+ is_outlier flag, not filter)
      * is_active -> bool
      * FK violations (customer->branch, account->customer/branch/product,
        transaction->account) go to quarantine, not to Silver
  - Writes:
        silver/<table>/ingestion_date=YYYY-MM-DD/data.parquet (clean)
        silver/_quarantine/<table>/ingestion_date=YYYY-MM-DD/data.parquet (rejected)
  - Overwrites the day partition (idempotent re-runs).
  - Preserves Bronze audit cols (_ingested_at, _source_file), adds _cleaned_at.

Outliers are FLAGGED, not removed: transactions keep 999999.99 and -50.00
with is_outlier=true for Gold exercises.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class SilverError(RuntimeError):
    """Raised when a Bronze partition cannot be transformed to Silver."""


PRIMARY_KEYS: dict[str, str] = {
    "branches": "branch_id",
    "products": "product_id",
    "customers": "customer_id",
    "accounts": "account_id",
    "transactions": "transaction_id",
}

ACCOUNT_STATUS = {"active", "inactive"}
TRANSACTION_STATUS = {"completed", "pending", "failed"}
TRANSACTION_TYPES = {
    "deposit",
    "withdrawal",
    "transfer_in",
    "transfer_out",
    "payment",
    "fee",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_quarantine(q_df: pd.DataFrame, qp: Path) -> None:
    """Write quarantine as all-string parquet (diagnostic store).

    Quarantine mixes raw rows (strings) with post-clean rows (datetimes/bools)
    from FK checks; casting to string avoids Arrow type conflicts on write.
    """
    q_write = q_df.copy()
    for c in q_write.columns:
        if q_write[c].dtype != object:
            try:
                q_write[c] = q_write[c].astype("string")
            except Exception:
                q_write[c] = q_write[c].astype(str)
        else:
            # object cols may hold Timestamps/dates from cleaned rows
            if any(isinstance(v, (pd.Timestamp,)) for v in q_write[c].head(20).tolist()):
                q_write[c] = q_write[c].astype("string")
    q_write.to_parquet(qp, index=False)


def _resolve_bronze_path(bronze_dir: Path, table: str, ingestion_date: str | None) -> tuple[Path, str]:
    """Return (parquet_path, day). Uses given day or latest ingestion_date=* partition."""
    if ingestion_date is not None:
        day = ingestion_date
        candidate = bronze_dir / table / f"ingestion_date={day}" / "data.parquet"
        if not candidate.exists():
            raise SilverError(f"Bronze partition not found: {candidate}")
        return candidate, day
    table_dir = bronze_dir / table
    if not table_dir.exists():
        raise SilverError(f"Bronze table not found: {table_dir}")
    partitions = sorted(p for p in table_dir.glob("ingestion_date=*") if (p / "data.parquet").exists())
    if not partitions:
        raise SilverError(f"No Bronze partitions for table {table!r} in {table_dir}")
    latest = partitions[-1]
    return latest / "data.parquet", latest.name.split("=", 1)[1]


def _strip_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def _split_pk_nulls(df: pd.DataFrame, pk: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split PK-null rows into quarantine. Returns (valid, quarantined)."""
    mask = df[pk].isna() | (df[pk].astype("string").str.strip() == "")
    q = df[mask].copy()
    if not q.empty:
        q["_quarantine_reason"] = f"null_{pk}"
    return df[~mask].copy(), q


def _drop_dup_pk(df: pd.DataFrame, pk: str) -> tuple[pd.DataFrame, int]:
    before = len(df)
    df = df.drop_duplicates(subset=[pk], keep="first")
    return df, before - len(df)


def clean_branches(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df = df.copy()
    df = _strip_cols(df, ["branch_id", "branch_name", "city", "state"])
    if "state" in df.columns:
        df["state"] = df["state"].str.upper()
    df, q_null = _split_pk_nulls(df, "branch_id")
    df, dups = _drop_dup_pk(df, "branch_id")
    return df, q_null, {"dups_removed": dups}


def clean_products(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df = df.copy()
    df = _strip_cols(df, ["product_id", "product_name", "product_type"])
    if "product_type" in df.columns:
        df["product_type"] = df["product_type"].str.lower()
    df, q_null = _split_pk_nulls(df, "product_id")
    df, dups = _drop_dup_pk(df, "product_id")
    return df, q_null, {"dups_removed": dups}


def clean_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df = df.copy()
    df = _strip_cols(df, ["customer_id", "full_name", "branch_id"])
    df, q_null = _split_pk_nulls(df, "customer_id")
    df, dups = _drop_dup_pk(df, "customer_id")
    quarantine_parts = [q_null]

    # birth_date -> date (quarantine unparsable). Stored as datetime64 (midnight)
    # for Parquet compat; logical type is DATE (BQ DATE in stage 5).
    if "birth_date" in df.columns:
        parsed = pd.to_datetime(df["birth_date"], errors="coerce")
        bad = parsed.isna() & df["birth_date"].notna()
        if bad.any():
            q = df[bad].copy()
            q["_quarantine_reason"] = "invalid_birth_date"
            quarantine_parts.append(q)
            df = df[~bad].copy()
            parsed = parsed[~bad]
        df["birth_date"] = parsed.dt.normalize()

    # is_active -> bool (quarantine unparsable)
    if "is_active" in df.columns:
        mapped = df["is_active"].map(
            {True: True, False: False, 1: True, 0: False, "True": True, "False": False,
             "true": True, "false": False, "1": True, "0": False}
        )
        bad = mapped.isna() & df["is_active"].notna()
        if bad.any():
            q = df[bad].copy()
            q["_quarantine_reason"] = "invalid_is_active"
            quarantine_parts.append(q)
            df = df[~bad].copy()
            mapped = mapped[~bad]
        df["is_active"] = mapped.astype("boolean")

    q = pd.concat(quarantine_parts, ignore_index=True) if len(quarantine_parts) > 1 or not q_null.empty else q_null
    return df, q, {"dups_removed": dups}


def clean_accounts(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df = df.copy()
    df = _strip_cols(df, ["account_id", "customer_id", "branch_id", "product_id", "status"])
    df, q_null = _split_pk_nulls(df, "account_id")
    df, dups = _drop_dup_pk(df, "account_id")
    quarantine_parts = [q_null] if not q_null.empty else []

    if "status" in df.columns:
        df["status"] = df["status"].str.lower()
        bad = ~df["status"].isin(ACCOUNT_STATUS)
        if bad.any():
            q = df[bad].copy()
            q["_quarantine_reason"] = "invalid_account_status"
            quarantine_parts.append(q)
            df = df[~bad].copy()

    if "open_date" in df.columns:
        parsed = pd.to_datetime(df["open_date"], errors="coerce")
        bad = parsed.isna() & df["open_date"].notna()
        if bad.any():
            q = df[bad].copy()
            q["_quarantine_reason"] = "invalid_open_date"
            quarantine_parts.append(q)
            df = df[~bad].copy()
            parsed = parsed[~bad]
        df["open_date"] = parsed.dt.normalize()

    q = pd.concat(quarantine_parts, ignore_index=True) if quarantine_parts else df.iloc[0:0].copy()
    return df, q, {"dups_removed": dups}


def clean_transactions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df = df.copy()
    df = _strip_cols(df, ["transaction_id", "account_id", "transaction_type", "status"])
    df, q_null = _split_pk_nulls(df, "transaction_id")
    df, dups = _drop_dup_pk(df, "transaction_id")
    quarantine_parts = [q_null] if not q_null.empty else []

    for col, allowed, reason in [
        ("transaction_type", TRANSACTION_TYPES, "invalid_transaction_type"),
        ("status", TRANSACTION_STATUS, "invalid_transaction_status"),
    ]:
        if col in df.columns:
            df[col] = df[col].str.lower()
            bad = ~df[col].isin(allowed)
            if bad.any():
                q = df[bad].copy()
                q["_quarantine_reason"] = reason
                quarantine_parts.append(q)
                df = df[~bad].copy()

    # amount -> numeric, 2 decimals. Null/NaN quarantined; outliers FLAGGED.
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        bad = df["amount"].isna()
        if bad.any():
            q = df[bad].copy()
            q["_quarantine_reason"] = "invalid_amount"
            quarantine_parts.append(q)
            df = df[~bad].copy()
        df["amount"] = df["amount"].round(2)
        # Flag, don't drop: |amount| anomaly or negative (future Gold outlier study)
        df["is_outlier"] = (df["amount"] > 100000) | (df["amount"] < 0)

    # timestamp -> UTC datetime. Unparsable quarantined.
    if "timestamp" in df.columns:
        parsed = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        bad = parsed.isna() & df["timestamp"].notna()
        if bad.any():
            q = df[bad].copy()
            q["_quarantine_reason"] = "invalid_timestamp"
            quarantine_parts.append(q)
            df = df[~bad].copy()
            parsed = parsed[~bad]
        df["timestamp"] = parsed

    q = pd.concat(quarantine_parts, ignore_index=True) if quarantine_parts else df.iloc[0:0].copy()
    return df, q, {"dups_removed": dups}


CLEANERS = {
    "branches": clean_branches,
    "products": clean_products,
    "customers": clean_customers,
    "accounts": clean_accounts,
    "transactions": clean_transactions,
}


def _apply_fk_checks(
    cleaned: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Move FK-violating rows from clean to quarantine extras. Returns {table: fk_quarantine}."""
    out: dict[str, pd.DataFrame] = {t: cleaned[t].iloc[0:0].copy() for t in cleaned}

    def _split_fk(child: str, col: str, parent: str, parent_col: str, reason: str) -> None:
        if child not in cleaned or parent not in cleaned:
            return
        if col not in cleaned[child].columns or parent_col not in cleaned[parent].columns:
            return
        valid = set(cleaned[parent][parent_col].dropna().astype(str))
        mask = ~cleaned[child][col].astype(str).isin(valid)
        if mask.any():
            q = cleaned[child][mask].copy()
            q["_quarantine_reason"] = reason
            out[child] = pd.concat([out[child], q], ignore_index=True)
            cleaned[child] = cleaned[child][~mask].copy()

    _split_fk("customers", "branch_id", "branches", "branch_id", "fk_branch_missing")
    _split_fk("accounts", "customer_id", "customers", "customer_id", "fk_customer_missing")
    _split_fk("accounts", "branch_id", "branches", "branch_id", "fk_branch_missing")
    _split_fk("accounts", "product_id", "products", "product_id", "fk_product_missing")
    _split_fk("transactions", "account_id", "accounts", "account_id", "fk_account_missing")
    return out


def transform_table_to_silver(
    table: str,
    bronze_dir: Path,
    silver_dir: Path,
    ingestion_date: str | None = None,
    cleaned_refs: dict[str, pd.DataFrame] | None = None,
) -> dict:
    """Transform a single Bronze partition to Silver. Returns a summary dict."""
    if table not in CLEANERS:
        raise SilverError(f"Unknown silver table: {table!r}")
    bronze_path, day = _resolve_bronze_path(bronze_dir, table, ingestion_date)
    try:
        df = pd.read_parquet(bronze_path)
    except Exception as exc:
        raise SilverError(f"Failed to read bronze {bronze_path}: {exc}") from exc
    if df.empty:
        raise SilverError(f"Bronze table {table!r} is empty ({bronze_path})")

    rows_in = len(df)
    clean_df, q_df, notes = CLEANERS[table](df)

    # FK pass is applied at run_silver level (needs all tables); single-table
    # call keeps rows and reports fk_pending so CLI/tests stay explicit.
    clean_df = clean_df.copy()
    clean_df["_cleaned_at"] = _utc_now_iso()

    out_dir = silver_dir / table / f"ingestion_date={day}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    try:
        clean_df.to_parquet(out_path, index=False)
    except Exception as exc:
        raise SilverError(f"Failed to write silver {out_path}: {exc}") from exc

    quarantine_path: str | None = None
    rows_q = len(q_df)
    if not q_df.empty:
        q_dir = silver_dir / "_quarantine" / table / f"ingestion_date={day}"
        q_dir.mkdir(parents=True, exist_ok=True)
        qp = q_dir / "data.parquet"
        _write_quarantine(q_df, qp)
        quarantine_path = str(qp)

    summary = {
        "table": table,
        "rows_in": rows_in,
        "rows_clean": int(len(clean_df)),
        "rows_quarantined": int(rows_q),
        "dups_removed": int(notes.get("dups_removed", 0)),
        "output_path": str(out_path),
        "quarantine_path": quarantine_path,
        "ingestion_date": day,
    }
    logger.info(
        "Silver OK: %s in=%d clean=%d quar=%d dups=%d -> %s",
        table, rows_in, len(clean_df), rows_q, summary["dups_removed"], out_path,
    )
    return summary


def run_silver(
    bronze_dir: Path,
    silver_dir: Path,
    tables: list[str] | None = None,
    ingestion_date: str | None = None,
) -> list[dict]:
    """Transform all (or selected) tables, applying FK checks across them."""
    selected = tables or sorted(CLEANERS)
    unknown = [t for t in selected if t not in CLEANERS]
    if unknown:
        raise SilverError(f"Unknown tables requested: {unknown}")
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Pass 1: clean each table independently (reads pinned day per table).
    cleaned: dict[str, pd.DataFrame] = {}
    quarantine: dict[str, pd.DataFrame] = {}
    days: dict[str, str] = {}
    for t in selected:
        bronze_path, day = _resolve_bronze_path(bronze_dir, t, ingestion_date)
        days[t] = day
        df = pd.read_parquet(bronze_path)
        clean_df, q_df, _ = CLEANERS[t](df)
        cleaned[t] = clean_df
        quarantine[t] = q_df

    # Pass 2: FK checks only when the full graph (or at least parent) is present.
    fk_extra = _apply_fk_checks(cleaned)
    for t in selected:
        if not fk_extra[t].empty:
            quarantine[t] = pd.concat([quarantine[t], fk_extra[t]], ignore_index=True)

    # Pass 3: write (idempotent overwrite) + summaries.
    summaries: list[dict] = []
    for t in selected:
        day = days[t]
        clean_df = cleaned[t].copy()
        clean_df["_cleaned_at"] = _utc_now_iso()
        out_dir = silver_dir / t / f"ingestion_date={day}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        try:
            clean_df.to_parquet(out_path, index=False)
        except Exception as exc:
            raise SilverError(f"Failed to write silver {out_path}: {exc}") from exc

        q_df = quarantine[t]
        # Recompute dups as rows_in - clean - quarantined(non-dup)? Keep simple:
        rows_in = len(pd.read_parquet(_resolve_bronze_path(bronze_dir, t, ingestion_date)[0]))
        summaries.append(
            {
                "table": t,
                "rows_in": rows_in,
                "rows_clean": int(len(clean_df)),
                "rows_quarantined": int(len(q_df)),
                "output_path": str(out_path),
                "quarantine_path": None,
                "ingestion_date": day,
            }
        )
        if not q_df.empty:
            q_dir = silver_dir / "_quarantine" / t / f"ingestion_date={day}"
            q_dir.mkdir(parents=True, exist_ok=True)
            qp = q_dir / "data.parquet"
            _write_quarantine(q_df, qp)
            summaries[-1]["quarantine_path"] = str(qp)
        logger.info(
            "Silver OK: %s in=%d clean=%d quar=%d -> %s",
            t, rows_in, len(clean_df), len(q_df), out_path,
        )
    return summaries
