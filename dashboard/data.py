"""Camada de dados do dashboard (sem dependência de streamlit — testável).

Fontes:
  - bq (default): lê as views `engdta.gold.v_*` via google-cloud-bigquery (ADC).
  - local: lê `data/gold/*/ingestion_date=*/data.parquet` (offline/dev).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def _latest_partition(base: Path, table: str) -> Path:
    parts = sorted(p for p in (base / table).glob("ingestion_date=*")
                   if (p / "data.parquet").exists())
    if not parts:
        raise RuntimeError(f"Sem partição local para {table!r}")
    return parts[-1] / "data.parquet"


def load_local(gold_dir: Path) -> dict[str, pd.DataFrame]:
    """Carrega Gold local. Retorna frames crus (filtros/agregações à parte)."""
    out: dict[str, pd.DataFrame] = {}
    for t in ["fact_daily_volume", "agg_transaction_type", "agg_branch", "outliers"]:
        out[t] = pd.read_parquet(_latest_partition(gold_dir, t))
    out["fact_daily_volume"]["transaction_date"] = pd.to_datetime(
        out["fact_daily_volume"]["transaction_date"], utc=True
    )
    return out


def load_bq(project: str) -> dict[str, pd.DataFrame]:
    """Carrega as views do BQ. Retorna os mesmos frames de load_local."""
    try:
        from google.cloud import bigquery  # type: ignore
    except ImportError as exc:
        raise RuntimeError("dashboard BQ precisa de google-cloud-bigquery") from exc
    client = bigquery.Client(project=project)
    views = {
        "fact_daily_volume": "v_daily_volume",
        "agg_transaction_type": "v_ticket_by_type",
        "agg_branch": "v_branch_ranking",
        "outliers": "outliers",
    }
    out: dict[str, pd.DataFrame] = {}
    for key, obj in views.items():
        out[key] = client.query(f"SELECT * FROM `{project}.gold.{obj}`").to_dataframe()
    out["fact_daily_volume"]["transaction_date"] = pd.to_datetime(
        out["fact_daily_volume"]["transaction_date"], utc=True
    )
    return out


def kpis(fact: pd.DataFrame) -> dict:
    """Totais p/ scorecards (mesma semântica da view v_kpis)."""
    return {
        "n_days": int(fact["transaction_date"].nunique()),
        "total_transactions": int(fact["n_completed"].sum()),
        "total_volume": round(float(fact["total_completed"].sum()), 2),
        "total_outliers": int(fact["n_outliers"].sum()),
    }


def filter_period(fact: pd.DataFrame, start, end) -> pd.DataFrame:
    """Filtra fact por janela [start, end] (datas ou Timestamps)."""
    d = pd.to_datetime(fact["transaction_date"], utc=True)
    mask = (d >= pd.Timestamp(start, tz="UTC")) & (d <= pd.Timestamp(end, tz="UTC"))
    return fact[mask].copy()
