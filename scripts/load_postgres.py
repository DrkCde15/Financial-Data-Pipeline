"""Loader: Gold parquet -> PostgreSQL (serving).

Usage:
    export DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/finance
    python scripts/load_postgres.py [--date YYYY-MM-DD] [--tables fact_daily_volume ...]
    python scripts/load_postgres.py --ddl-only
    python scripts/load_postgres.py --checks

Idempotent: DELETE WHERE ingestion_date=<day> + INSERT per table.
Sem docker aqui? Suba com `podman compose up -d` (ver docker-compose.yml).
Testes usam SQLite (sem Postgres) — DDL foi escrita para rodar nos dois.

Notas de compatibilidade:
  - Airflow 2.6 prende o SQLAlchemy em 1.x (sem dialeto +psycopg v3):
    no container use sempre `postgresql+psycopg2://` (ver docker-compose.yml).
  - O loader aceita as duas URLs e faz fallback de dialeto sozinho.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_pipeline.config import get_project_root, load_settings, setup_logging

logger = setup_logging()

GOLD_TABLES = ["fact_daily_volume", "agg_transaction_type", "agg_branch", "outliers"]


def _is_sqlite(url: str) -> bool:
    return url.startswith("sqlite") or url == ":memory:"


def _make_engine(url: str):
    """Create a SQLAlchemy engine, with psycopg/psycopg2 dialect fallback."""
    try:
        from sqlalchemy import create_engine  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL load needs SQLAlchemy+psycopg: pip install -e '.[postgres]'"
        ) from exc
    try:
        return create_engine(url, future=True)
    except Exception as exc:
        if "+psycopg://" in url:
            alt = url.replace("+psycopg://", "+psycopg2://")
        elif "+psycopg2://" in url:
            alt = url.replace("+psycopg2://", "+psycopg://")
        else:
            raise
        if "Can't load plugin" in str(exc) or "NoSuchModule" in type(exc).__name__:
            logger.warning("dialect fallback para %s", alt.split("://")[0])
            return create_engine(alt, future=True)
        raise


def _split_stmts(path: Path) -> list[str]:
    return [s.strip() for s in path.read_text(encoding="utf-8").split(";") if s.strip()]


# ---- SQLite path (testes / local sem Postgres) ----

def _apply_ddl_sqlite(con: sqlite3.Connection, root: Path) -> None:
    for ddl in sorted((root / "sql" / "ddl").glob("*.sql")):
        for stmt in _split_stmts(ddl):
            con.execute(stmt)
    con.commit()
    logger.info("DDL OK")


def _load_table_sqlite(con: sqlite3.Connection, gold_dir: Path, table: str, day: str | None) -> dict:
    import pandas as pd

    path, resolved_day = _resolve_gold_path(gold_dir, table, day)
    df = pd.read_parquet(path).copy()
    df["ingestion_date"] = resolved_day
    con.execute(f"DELETE FROM {table} WHERE ingestion_date = '{resolved_day}'")
    con.commit()
    df.to_sql(table, con, if_exists="append", index=False)
    con.commit()
    logger.info("LOAD OK: %s rows=%d day=%s", table, len(df), resolved_day)
    return {"table": table, "rows": len(df), "ingestion_date": resolved_day}


# ---- SQLAlchemy path (Postgres real; funciona em 1.x e 2.x) ----

def _apply_ddl_sa(engine, root: Path) -> None:
    from sqlalchemy import text  # type: ignore

    with engine.begin() as conn:
        for ddl in sorted((root / "sql" / "ddl").glob("*.sql")):
            for stmt in _split_stmts(ddl):
                conn.execute(text(stmt))
    logger.info("DDL OK")


def _resolve_gold_path(gold_dir: Path, table: str, day: str | None) -> tuple[Path, str]:
    if day is not None:
        p = gold_dir / table / f"ingestion_date={day}" / "data.parquet"
        if not p.exists():
            raise RuntimeError(f"Gold partition not found: {p}")
        return p, day
    tdir = gold_dir / table
    parts = sorted(p for p in tdir.glob("ingestion_date=*") if (p / "data.parquet").exists())
    if not parts:
        raise RuntimeError(f"No Gold partitions for {table!r}")
    latest = parts[-1]
    return latest / "data.parquet", latest.name.split("=", 1)[1]


def _load_table_sa(engine, gold_dir: Path, table: str, day: str | None) -> dict:
    import pandas as pd
    from sqlalchemy import text  # type: ignore

    path, resolved_day = _resolve_gold_path(gold_dir, table, day)
    df = pd.read_parquet(path).copy()
    df["ingestion_date"] = resolved_day
    for c in df.columns:
        if str(df[c].dtype).startswith("datetime"):
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype(str)
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table} WHERE ingestion_date = :d"), {"d": resolved_day}
        )
    df.to_sql(table, engine, if_exists="append", index=False)
    logger.info("LOAD OK: %s rows=%d day=%s", table, len(df), resolved_day)
    return {"table": table, "rows": len(df), "ingestion_date": resolved_day}


def _run_checks_sa(engine, root: Path) -> dict:
    import pandas as pd

    # Conexão DBAPI crua: evita a matriz de compat pandas x SQLAlchemy
    # (no container o pip instala versões diferentes das do Airflow 2.6).
    results: dict = {}
    raw = engine.raw_connection()
    try:
        for check in sorted((root / "sql" / "quality_checks").glob("*.sql")):
            sql = check.read_text(encoding="utf-8")
            try:
                df = pd.read_sql_query(sql, raw)
            except Exception as exc:
                results[check.name] = {"ok": False, "error": str(exc)}
                continue
            if check.name == "04_freshness.sql":
                results[check.name] = {"ok": True, "rows": df.to_dict("records")}
            else:
                results[check.name] = {"ok": len(df) == 0, "violations": len(df)}
    finally:
        try:
            raw.close()
        except Exception:
            pass
    return results


def _run_checks_sqlite(con: sqlite3.Connection, root: Path) -> dict:
    import pandas as pd

    results: dict = {}
    for check in sorted((root / "sql" / "quality_checks").glob("*.sql")):
        sql = check.read_text(encoding="utf-8")
        try:
            df = pd.read_sql_query(sql, con)
        except Exception as exc:
            results[check.name] = {"ok": False, "error": str(exc)}
            continue
        if check.name == "04_freshness.sql":
            results[check.name] = {"ok": True, "rows": df.to_dict("records")}
        else:
            results[check.name] = {"ok": len(df) == 0, "violations": len(df)}
    return results


# ---- back-compat: nomes usados pelos testes ----

def apply_ddl(conn, root: Path) -> None:
    """Apply DDL over a sqlite3 connection (tests)."""
    _apply_ddl_sqlite(conn, root)


def load_table(conn, gold_dir: Path, table: str, day: str | None) -> dict:
    """DELETE+INSERT over a sqlite3 connection (tests)."""
    return _load_table_sqlite(conn, gold_dir, table, day)


def run_checks(conn, root: Path) -> dict:
    """Run quality checks over a sqlite3 connection (tests)."""
    return _run_checks_sqlite(conn, root)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load Gold parquet to PostgreSQL.")
    p.add_argument("--date", default=None, help="Ingestion date YYYY-MM-DD (default: latest).")
    p.add_argument("--tables", nargs="*", default=None, choices=GOLD_TABLES)
    p.add_argument("--ddl-only", action="store_true", help="Apply DDL and exit.")
    p.add_argument("--checks", action="store_true", help="Run quality checks and exit.")
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""),
                   help="SQLAlchemy URL (default: $DATABASE_URL).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = get_project_root()
    settings = load_settings()
    url = args.database_url or os.getenv("DATABASE_URL", "")
    if not url:
        url = "postgresql+psycopg2://postgres:postgres@localhost:5432/finance"
        logger.warning("DATABASE_URL ausente — tentando %s", url.split("@")[-1])

    try:
        if _is_sqlite(url):
            path = url.split("sqlite:///", 1)[1] if "sqlite:///" in url else ":memory:"
            conn = sqlite3.connect(path or ":memory:")
            _apply_ddl_sqlite(conn, root)
            if args.ddl_only:
                print("DDL OK")
                return 0
            if args.checks:
                import json
                res = _run_checks_sqlite(conn, root)
                print(json.dumps(res, indent=2, default=str))
                return 1 if any(not v.get("ok") for v in res.values()) else 0
            selected = args.tables or GOLD_TABLES
            total = 0
            for t in selected:
                s = _load_table_sqlite(conn, settings.gold_dir, t, args.date)
                total += s["rows"]
                print(f"  - {s['table']:<22} rows={s['rows']:<5} day={s['ingestion_date']}")
            logger.info("LOAD DONE: %d tables, %d rows", len(selected), total)
            return 0

        engine = _make_engine(url)
        _apply_ddl_sa(engine, root)
        if args.ddl_only:
            print("DDL OK")
            return 0
        if args.checks:
            import json
            res = _run_checks_sa(engine, root)
            print(json.dumps(res, indent=2, default=str))
            return 1 if any(not v.get("ok") for v in res.values()) else 0
        selected = args.tables or GOLD_TABLES
        total = 0
        for t in selected:
            s = _load_table_sa(engine, settings.gold_dir, t, args.date)
            total += s["rows"]
            print(f"  - {s['table']:<22} rows={s['rows']:<5} day={s['ingestion_date']}")
        logger.info("LOAD DONE: %d tables, %d rows", len(selected), total)
        return 0
    except Exception as exc:
        logger.error("LOAD FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
