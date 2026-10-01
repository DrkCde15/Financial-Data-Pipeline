"""CLI entrypoint: build silver -> gold aggregations.

Usage:
    python scripts/run_gold.py [--tables fact_daily_volume agg_branch] [--date YYYY-MM-DD]

Requires silver partitions to exist (run scripts/run_silver.py first).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_pipeline.config import load_settings, setup_logging
from financial_pipeline.transformation.gold import GOLD_TABLES

logger = setup_logging()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Build Gold aggregations (parquet).")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        choices=sorted(GOLD_TABLES),
        help="Subset of gold tables to build (default: all).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Ingestion date partition YYYY-MM-DD (default: latest silver partition).",
    )
    return parser.parse_args()


def main() -> int:
    """Build gold and print a summary. Returns exit code."""
    from financial_pipeline.transformation.gold import build_gold

    args = parse_args()
    settings = load_settings()
    settings.ensure_dirs()

    try:
        summaries = build_gold(
            silver_dir=settings.silver_dir,
            gold_dir=settings.gold_dir,
            tables=args.tables,
            ingestion_date=args.date,
        )
    except Exception as exc:  # explicit top-level failure with clear message
        logger.error("Gold FAILED: %s", exc)
        return 1

    logger.info("Gold DONE: %d tables", len(summaries))
    for s in summaries:
        print(f"  - {s['table']:<22} rows={s['rows']:<5} -> {s['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
