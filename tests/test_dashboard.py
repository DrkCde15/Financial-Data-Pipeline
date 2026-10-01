"""Tests for dashboard data layer (local parquet — no BQ, no streamlit)."""

from __future__ import annotations

import pandas as pd

from dashboard.data import filter_period, kpis, load_local


def test_kpis_match_gold_semantics() -> None:
    """Totais sobre completed sem outliers (igual v_kpis)."""
    fact = pd.DataFrame([
        {"transaction_date": "2024-06-01", "n_completed": 2, "total_completed": 300.0,
         "n_outliers": 0, "outlier_total": 0.0},
        {"transaction_date": "2024-06-02", "n_completed": 1, "total_completed": 0.0,
         "n_outliers": 1, "outlier_total": 999999.99},
    ])
    k = kpis(fact)
    assert k == {"n_days": 2, "total_transactions": 3,
                 "total_volume": 300.0, "total_outliers": 1}


def test_filter_period_slices_dates() -> None:
    """Slider de período filtra [start, end] inclusive."""
    fact = pd.DataFrame({
        "transaction_date": pd.to_datetime(["2024-06-01", "2024-06-02", "2024-06-03"], utc=True),
        "n_completed": [1, 1, 1], "total_completed": [10.0, 20.0, 30.0],
        "n_outliers": [0, 0, 0],
    })
    out = filter_period(fact, "2024-06-02", "2024-06-03")
    assert out["total_completed"].tolist() == [20.0, 30.0]


def test_load_local_reads_real_gold() -> None:
    """Fallback local lê as 4 tabelas Gold do disco."""
    from financial_pipeline.config import get_project_root

    frames = load_local(get_project_root() / "data" / "gold")
    assert set(frames) == {"fact_daily_volume", "agg_transaction_type",
                            "agg_branch", "outliers"}
    assert len(frames["fact_daily_volume"]) == 28
    assert len(frames["outliers"]) == 2
