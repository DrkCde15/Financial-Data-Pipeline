"""CLI entrypoint: run raw -> bronze ingestion.

Usage:
    python scripts/run_bronze_ingestion.py [--tables branches customers] [--date YYYY-MM-DD]

Requires raw files to exist (run scripts/generate_synthetic_data.py first).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_pipeline.config import load_settings, setup_logging
from financial_pipeline.ingestion.bronze import run_bronze_ingestion
from financial_pipeline.ingestion.schemas import SOURCE_FILES

logger = setup_logging()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Ingest raw files to Bronze (parquet).")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        choices=sorted(SOURCE_FILES),
        help="Subset of tables to ingest (default: all).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Ingestion date partition YYYY-MM-DD (default: today UTC).",
    )
    return parser.parse_args()


def main() -> int:
    """Run ingestion and print a summary. Returns exit code."""
    args = parse_args()
    settings = load_settings()
    settings.ensure_dirs()

    try:
        summaries = run_bronze_ingestion(
            raw_dir=settings.raw_dir,
            bronze_dir=settings.bronze_dir,
            tables=args.tables,
            ingestion_date=args.date,
        )
    except Exception as exc:  # explicit top-level failure with clear message
        logger.error("Bronze ingestion FAILED: %s", exc)
        return 1

    total_rows = sum(s["rows"] for s in summaries)
    logger.info("Bronze ingestion DONE: %d tables, %d rows total", len(summaries), total_rows)
    for s in summaries:
        print(f"  - {s['table']:<12} rows={s['rows']:<5} -> {s['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
