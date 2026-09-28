"""CLI entrypoint: run bronze -> silver transformation.

Usage:
    python scripts/run_silver.py [--tables branches customers] [--date YYYY-MM-DD]

Requires bronze partitions to exist (run scripts/run_bronze_ingestion.py first).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_pipeline.config import load_settings, setup_logging
from financial_pipeline.transformation.silver import CLEANERS

logger = setup_logging()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Transform bronze to Silver (parquet).")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        choices=sorted(CLEANERS),
        help="Subset of tables to transform (default: all).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Ingestion date partition YYYY-MM-DD (default: latest bronze partition).",
    )
    return parser.parse_args()


def main() -> int:
    """Run silver and print a summary. Returns exit code."""
    from financial_pipeline.transformation.silver import run_silver

    args = parse_args()
    settings = load_settings()
    settings.ensure_dirs()

    try:
        summaries = run_silver(
            bronze_dir=settings.bronze_dir,
            silver_dir=settings.silver_dir,
            tables=args.tables,
            ingestion_date=args.date,
        )
    except Exception as exc:  # explicit top-level failure with clear message
        logger.error("Silver FAILED: %s", exc)
        return 1

    total_clean = sum(s["rows_clean"] for s in summaries)
    total_q = sum(s["rows_quarantined"] for s in summaries)
    logger.info(
        "Silver DONE: %d tables, %d clean rows, %d quarantined",
        len(summaries), total_clean, total_q,
    )
    for s in summaries:
        print(
            f"  - {s['table']:<12} clean={s['rows_clean']:<5} "
            f"quar={s['rows_quarantined']:<4} -> {s['output_path']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
