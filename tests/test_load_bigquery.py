"""Tests for the BigQuery loader (Client mocked — no sandbox needed)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.load_bigquery import (
    LAYERS,
    apply_views,
    ensure_datasets,
    load_table,
    run_checks,
)


def _client() -> MagicMock:
    return MagicMock()


def test_layers_cover_medallion() -> None:
    """Loader deve espelhar as 3 camadas (5+5+4 tabelas)."""
    assert set(LAYERS) == {"bronze", "silver", "gold"}
    assert len(LAYERS["gold"]) == 4


def test_ensure_datasets_sets_60d_expiration() -> None:
    """Datasets criados com expiração 60d (regra do sandbox)."""
    client = _client()
    with patch("google.cloud.bigquery.Dataset") as mock_ds:
        ensure_datasets(client, "proj", ["gold"])
    ds_arg = mock_ds.call_args[0][0]
    assert ds_arg == "proj.gold"
    assert client.create_dataset.call_args[1].get("exists_ok") is True
    created = client.create_dataset.call_args[0][0]
    assert created.default_table_expiration_ms == 60 * 24 * 3600 * 1000


def test_load_table_truncates_and_adds_ingestion_date(tmp_path: Path) -> None:
    """Load job WRITE_TRUNCATE + coluna ingestion_date injetada."""
    base = tmp_path / "gold"
    day = "2024-01-01"
    d = base / "fact_daily_volume" / f"ingestion_date={day}"
    d.mkdir(parents=True)
    pd.DataFrame([{"transaction_date": "2024-06-01", "n_total": 2}]).to_parquet(d / "data.parquet")

    client = _client()
    with patch("google.cloud.bigquery.LoadJobConfig") as mock_cfg, \
         patch("google.cloud.bigquery.SourceFormat"), \
         patch("google.cloud.bigquery.WriteDisposition"):
        s = load_table(client, "proj", "gold", base, "fact_daily_volume", day)
    assert s["rows"] == 1 and s["ingestion_date"] == day
    assert mock_cfg.call_args[1].get("write_disposition") is not None
    dest = client.load_table_from_file.call_args[0][1]
    assert dest == "proj.gold.fact_daily_volume"


def test_run_checks_parses_rows() -> None:
    """01 com 0 rows = ok; 02 com 1 row = falha."""
    from financial_pipeline.config import get_project_root

    client = _client()

    def fake_query(sql):
        job = MagicMock()
        if "GROUP BY" in sql:
            job.result.return_value = []
        else:
            job.result.return_value = [{"tbl": "outliers"}]
        return job

    client.query.side_effect = fake_query
    res = run_checks(client, "proj", get_project_root())
    assert res["01_dup_pks.sql"]["ok"] is True
    assert res["02_null_keys.sql"]["ok"] is False
    assert res["02_null_keys.sql"]["violations"] == 1


def test_apply_views_replaces_project(tmp_path: Path) -> None:
    """Placeholder {project} substituído antes de enviar o DDL."""
    root = tmp_path
    vdir = root / "sql" / "bigquery" / "views"
    vdir.mkdir(parents=True)
    (vdir / "v.sql").write_text("CREATE OR REPLACE VIEW `{project}.gold.v` AS SELECT 1 AS a")
    client = _client()
    apply_views(client, "myproj", root)
    sent = client.query.call_args[0][0]
    assert "{project}" not in sent and "myproj.gold.v" in sent


def test_missing_partition_raises(tmp_path: Path) -> None:
    """Partição ausente -> erro claro (fail-fast)."""
    with pytest.raises(RuntimeError, match="não encontrada"):
        load_table(_client(), "proj", "gold", tmp_path / "empty", "fact_daily_volume", "2024-01-01")
