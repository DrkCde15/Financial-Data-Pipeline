"""Silver -> Gold aggregations (stage 3 of the medallion pipeline).

Gold tables (pandas, local, grain declared):
  - fact_daily_volume: grain (transaction_date). One row per UTC date.
      n_total, n_completed, total_completed, avg_ticket_completed,
      n_outliers, outlier_total. Averages exclude outliers (documented).
  - agg_transaction_type: grain (transaction_type). Completed only, excl outliers.
      n, total, avg_ticket.
  - agg_branch: grain (branch_id). Joins transactions->accounts->branches.
      Completed only, excl outliers + branch/city/state labels,
      n_accounts, n_transactions, total_volume, avg_ticket.
  - outliers: grain (transaction_id). Enriched is_outlier=true rows for
      investigation (amount, timestamp, account/customer/branch).

Reads Silver latest (or pinned) partitions, writes:
    gold/<table>/ingestion_date=YYYY-MM-DD/data.parquet
Overwrites the partition (idempotent). Adds _built_at (UTC).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class GoldError(RuntimeError):
    """Raised when Silver cannot be aggregated to Gold."""


GOLD_TABLES = ["fact_daily_volume", "agg_transaction_type", "agg_branch", "outliers"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_silver_path(silver_dir: Path, table: str, ingestion_date: str | None) -> tuple[Path, str]:
    """Return (parquet_path, day). Uses given day or latest ingestion_date=* partition."""
    if ingestion_date is not None:
        day = ingestion_date
        candidate = silver_dir / table / f"ingestion_date={day}" / "data.parquet"
        if not candidate.exists():
            raise GoldError(f"Silver partition not found: {candidate}")
        return candidate, day
    table_dir = silver_dir / table
    if not table_dir.exists():
        raise GoldError(f"Silver table not found: {table_dir}")
    partitions = sorted(p for p in table_dir.glob("ingestion_date=*") if (p / "data.parquet").exists())
    if not partitions:
        raise GoldError(f"No Silver partitions for table {table!r} in {table_dir}")
    latest = partitions[-1]
    return latest / "data.parquet", latest.name.split("=", 1)[1]


def _load_silver(silver_dir: Path, ingestion_date: str | None) -> tuple[dict[str, pd.DataFrame], str]:
    """Load the 5 Silver tables pinned to one day. Returns (frames, day)."""
    frames: dict[str, pd.DataFrame] = {}
    day: str | None = None
    for t in ["branches", "products", "customers", "accounts", "transactions"]:
        path, d = _resolve_silver_path(silver_dir, t, ingestion_date)
        if day is None:
            day = d
        elif ingestion_date is None and d != day:
            logger.warning("Silver partitions diverge: %s=%s vs %s — using %s per table", t, d, day, d)
            day = d if t == "transactions" else day
        try:
            frames[t] = pd.read_parquet(path)
        except Exception as exc:
            raise GoldError(f"Failed to read silver {path}: {exc}") from exc
        if frames[t].empty:
            raise GoldError(f"Silver table {t!r} is empty ({path})")
    assert day is not None
    return frames, day


def build_fact_daily_volume(transactions: pd.DataFrame) -> pd.DataFrame:
    """Grain (transaction_date). Averages exclude outliers."""
    tx = transactions.copy()
    tx["timestamp"] = pd.to_datetime(tx["timestamp"], utc=True, errors="coerce")
    tx = tx.dropna(subset=["timestamp"])
    tx["transaction_date"] = tx["timestamp"].dt.normalize()
    tx["is_outlier"] = tx.get("is_outlier", False).fillna(False).astype(bool)
    rows = []
    for date, g in tx.groupby("transaction_date"):
        completed = g[g["status"] == "completed"]
        clean = completed[~completed["is_outlier"]]
        out = g[g["is_outlier"]]
        rows.append({
            "transaction_date": pd.Timestamp(date).normalize(),
            "n_total": int(len(g)),
            "n_completed": int(len(completed)),
            "total_completed": round(float(clean["amount"].sum()), 2) if len(clean) else 0.0,
            "avg_ticket_completed": round(float(clean["amount"].mean()), 2) if len(clean) else 0.0,
            "n_outliers": int(out["is_outlier"].sum()),
            "outlier_total": round(float(out["amount"].sum()), 2) if len(out) else 0.0,
        })
    df = pd.DataFrame(rows).sort_values("transaction_date").reset_index(drop=True)
    return df


def build_agg_transaction_type(transactions: pd.DataFrame) -> pd.DataFrame:
    """Grain (transaction_type). Completed only, excl outliers."""
    tx = transactions.copy()
    tx["is_outlier"] = tx.get("is_outlier", False).fillna(False).astype(bool)
    base = tx[(tx["status"] == "completed") & (~tx["is_outlier"])]
    if base.empty:
        return pd.DataFrame(columns=["transaction_type", "n", "total", "avg_ticket"])
    g = base.groupby("transaction_type")["amount"].agg(n="size", total="sum", avg_ticket="mean").reset_index()
    g["total"] = g["total"].round(2)
    g["avg_ticket"] = g["avg_ticket"].round(2)
    return g.sort_values("total", ascending=False).reset_index(drop=True)


def build_agg_branch(
    transactions: pd.DataFrame,
    accounts: pd.DataFrame,
    branches: pd.DataFrame,
) -> pd.DataFrame:
    """Grain (branch_id). Completed only, excl outliers, enriched with labels."""
    tx = transactions.copy()
    tx["is_outlier"] = tx.get("is_outlier", False).fillna(False).astype(bool)
    base = tx[(tx["status"] == "completed") & (~tx["is_outlier"])].copy()
    acct = accounts[["account_id", "branch_id"]].copy()
    merged = base.merge(acct, on="account_id", how="left", validate="many_to_one")
    merged = merged.dropna(subset=["branch_id"])
    labels = branches[["branch_id", "branch_name", "city", "state"]].drop_duplicates("branch_id")
    if merged.empty:
        return pd.DataFrame(columns=[
            "branch_id", "branch_name", "city", "state",
            "n_transactions", "total_volume", "avg_ticket", "n_accounts",
        ])
    agg = merged.groupby("branch_id").agg(
        n_transactions=("transaction_id", "size"),
        total_volume=("amount", "sum"),
        avg_ticket=("amount", "mean"),
        n_accounts=("account_id", "nunique"),
    ).reset_index()
    agg["total_volume"] = agg["total_volume"].round(2)
    agg["avg_ticket"] = agg["avg_ticket"].round(2)
    out = agg.merge(labels, on="branch_id", how="left", validate="many_to_one")
    return out.sort_values("total_volume", ascending=False).reset_index(drop=True)


def build_outliers(
    transactions: pd.DataFrame,
    accounts: pd.DataFrame,
    customers: pd.DataFrame,
    branches: pd.DataFrame,
) -> pd.DataFrame:
    """Grain (transaction_id). Enriched outlier rows for investigation."""
    tx = transactions.copy()
    tx["is_outlier"] = tx.get("is_outlier", False).fillna(False).astype(bool)
    out = tx[tx["is_outlier"]].copy()
    if out.empty:
        return pd.DataFrame(columns=[
            "transaction_id", "amount", "timestamp", "status", "transaction_type",
            "account_id", "customer_id", "branch_id", "branch_name", "full_name",
        ])
    out = out.merge(accounts[["account_id", "customer_id", "branch_id"]],
                    on="account_id", how="left", validate="many_to_one")
    out = out.merge(customers[["customer_id", "full_name"]],
                    on="customer_id", how="left", validate="many_to_one")
    out = out.merge(branches[["branch_id", "branch_name"]],
                    on="branch_id", how="left", validate="many_to_one")
    cols = ["transaction_id", "amount", "timestamp", "status", "transaction_type",
            "account_id", "customer_id", "branch_id", "branch_name", "full_name"]
    return out[cols].sort_values("amount", ascending=False).reset_index(drop=True)


def build_gold(
    silver_dir: Path,
    gold_dir: Path,
    tables: list[str] | None = None,
    ingestion_date: str | None = None,
) -> list[dict]:
    """Build all (or selected) Gold tables. Returns summary dicts."""
    selected = tables or list(GOLD_TABLES)
    unknown = [t for t in selected if t not in GOLD_TABLES]
    if unknown:
        raise GoldError(f"Unknown gold tables requested: {unknown}")
    gold_dir.mkdir(parents=True, exist_ok=True)

    frames, day = _load_silver(silver_dir, ingestion_date)
    built: dict[str, pd.DataFrame] = {}
    if "fact_daily_volume" in selected:
        built["fact_daily_volume"] = build_fact_daily_volume(frames["transactions"])
    if "agg_transaction_type" in selected:
        built["agg_transaction_type"] = build_agg_transaction_type(frames["transactions"])
    if "agg_branch" in selected:
        built["agg_branch"] = build_agg_branch(
            frames["transactions"], frames["accounts"], frames["branches"]
        )
    if "outliers" in selected:
        built["outliers"] = build_outliers(
            frames["transactions"], frames["accounts"], frames["customers"], frames["branches"]
        )

    summaries: list[dict] = []
    for t in selected:
        df = built[t].copy()
        df["_built_at"] = _utc_now_iso()
        out_dir = gold_dir / t / f"ingestion_date={day}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        try:
            df.to_parquet(out_path, index=False)
        except Exception as exc:
            raise GoldError(f"Failed to write gold {out_path}: {exc}") from exc
        summaries.append({
            "table": t,
            "rows": int(len(df)),
            "output_path": str(out_path),
            "ingestion_date": day,
        })
        logger.info("Gold OK: %s rows=%d -> %s", t, len(df), out_path)
    return summaries
