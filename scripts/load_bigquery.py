"""Loader: camadas locais (parquet) -> BigQuery sandbox (serving cloud).

Uso:
    python scripts/load_bigquery.py [--project engdta] [--date YYYY-MM-DD]
    python scripts/load_bigquery.py --layer gold            # só serving (default: all)
    python scripts/load_bigquery.py --ddl-only              # datasets + views
    python scripts/load_bigquery.py --checks                # só roda checks no BQ

Desenho sandbox-friendly (ver docs/architecture.md):
  - Sem DML (sandbox restringe): carga via load jobs Parquet com
    WRITE_TRUNCATE por tabela = idempotente (re-run sobrescreve).
  - Datasets bronze/silver/gold com expiração default 60 dias (regra do sandbox).
  - `ingestion_date` vai como STRING ISO (coluna de linhagem, não partição —
    particionar por ela no BQ seria o próximo passo pago/escala).
  - Auth via Application Default Credentials (`gcloud auth application-default
    login` no host; no Airflow, `~/.config/gcloud` montado — ver compose).

Requires: pip install google-cloud-bigquery (extra `cloud` no pyproject).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_pipeline.config import get_project_root, load_settings, setup_logging

logger = setup_logging()

LAYERS = {
    "bronze": ["branches", "products", "customers", "accounts", "transactions"],
    "silver": ["branches", "products", "customers", "accounts", "transactions"],
    "gold": ["fact_daily_volume", "agg_transaction_type", "agg_branch", "outliers"],
}

# 60 dias em ms — expiração padrão exigida pelo sandbox.
SANDBOX_TABLE_EXPIRATION_MS = 60 * 24 * 3600 * 1000


def _client(project: str, location: str):
    try:
        from google.cloud import bigquery  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "BQ load needs google-cloud-bigquery: pip install -e '.[cloud]'"
        ) from exc
    return bigquery.Client(project=project, location=location)


def ensure_datasets(client, project: str, layers: list[str]) -> None:
    """Cria datasets se ausentes (idempotente)."""
    from google.cloud import bigquery  # type: ignore

    for layer in layers:
        ds = bigquery.Dataset(f"{project}.{layer}")
        ds.default_table_expiration_ms = SANDBOX_TABLE_EXPIRATION_MS
        ds = client.create_dataset(ds, exists_ok=True)
        logger.info("dataset %s.%s OK (exp. %s)",
                    project, layer, ds.default_table_expiration_ms)


def apply_views(client, project: str, root: Path) -> None:
    """Cria/atualiza views Gold (CREATE OR REPLACE = idempotente)."""
    for view_file in sorted((root / "sql" / "bigquery" / "views").glob("*.sql")):
        ddl = view_file.read_text(encoding="utf-8").replace("{project}", project)
        client.query(ddl).result()
        logger.info("view OK: %s", view_file.name)


def _resolve_partition(base: Path, table: str, day: str | None) -> tuple[Path, str]:
    if day is not None:
        p = base / table / f"ingestion_date={day}" / "data.parquet"
        if not p.exists():
            raise RuntimeError(f"Partição não encontrada: {p}")
        return p, day
    tdir = base / table
    parts = sorted(p for p in tdir.glob("ingestion_date=*") if (p / "data.parquet").exists())
    if not parts:
        raise RuntimeError(f"Sem partições para {table!r} em {tdir}")
    latest = parts[-1]
    return latest / "data.parquet", latest.name.split("=", 1)[1]


def load_table(client, project: str, layer: str, base: Path, table: str,
               day: str | None) -> dict:
    """Lê parquet local (+ingestion_date), sobe via load job WRITE_TRUNCATE."""
    import pandas as pd
    from google.cloud import bigquery  # type: ignore

    src, resolved_day = _resolve_partition(base, table, day)
    df = pd.read_parquet(src).copy()
    df["ingestion_date"] = resolved_day
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    df.to_parquet(tmp_path, index=False)
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(
                f, f"{project}.{layer}.{table}", job_config=job_config
            )
        job.result()
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    logger.info("BQ LOAD OK: %s.%s rows=%d day=%s", layer, table, len(df), resolved_day)
    return {"layer": layer, "table": table, "rows": len(df), "ingestion_date": resolved_day}


def _rows_to_dicts(rows) -> list[dict]:
    return [dict(r) for r in rows]


def run_checks(client, project: str, root: Path) -> dict:
    """Roda sql/bigquery/checks/*.sql. 01-03: 0 rows = pass."""
    results: dict = {}
    for check in sorted((root / "sql" / "bigquery" / "checks").glob("*.sql")):
        sql = check.read_text(encoding="utf-8").replace("{project}", project)
        try:
            rows = _rows_to_dicts(client.query(sql).result())
        except Exception as exc:
            results[check.name] = {"ok": False, "error": str(exc)[:300]}
            continue
        if check.name == "04_freshness.sql":
            results[check.name] = {"ok": True, "rows": rows}
        else:
            results[check.name] = {"ok": len(rows) == 0, "violations": len(rows)}
    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load camadas locais -> BigQuery sandbox.")
    p.add_argument("--project", default=os.getenv("GCP_PROJECT", "engdta"))
    p.add_argument("--location", default=os.getenv("BQ_LOCATION", "US"))
    p.add_argument("--date", default=None, help="Partição ingestion_date (default: latest).")
    p.add_argument("--layer", nargs="*", default=None, choices=sorted(LAYERS),
                   help="Camadas (default: todas).")
    p.add_argument("--ddl-only", action="store_true")
    p.add_argument("--checks", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = get_project_root()
    settings = load_settings()
    base_of = {"bronze": settings.bronze_dir, "silver": settings.silver_dir,
               "gold": settings.gold_dir}
    layers = args.layer or sorted(LAYERS)

    try:
        client = _client(args.project, args.location)
        ensure_datasets(client, args.project, layers)
        apply_views(client, args.project, root)
        if args.ddl_only:
            print("datasets + views OK")
            return 0
        if args.checks:
            res = run_checks(client, args.project, root)
            print(json.dumps(res, indent=2, default=str))
            return 1 if any(not v.get("ok") for v in res.values()) else 0
        total = 0
        for layer in layers:
            for table in LAYERS[layer]:
                s = load_table(client, args.project, layer, base_of[layer], table, args.date)
                total += s["rows"]
                print(f"  - {layer}.{table:<22} rows={s['rows']:<5} day={s['ingestion_date']}")
        logger.info("BQ LOAD DONE: %d tabelas, %d rows",
                    sum(len(LAYERS[l]) for l in layers), total)
        return 0
    except Exception as exc:
        logger.error("BQ LOAD FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
