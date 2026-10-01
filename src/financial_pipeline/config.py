"""Centralized project configuration.

All paths resolve relative to the project root (folder containing pyproject.toml).
Settings can be overridden via environment variables (see .env.example).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


def get_project_root() -> Path:
    """Return project root (walks up from this file until pyproject.toml is found)."""
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback: src/financial_pipeline -> project root is 3 levels up
    return current.parents[2]


def setup_logging(level: str | None = None) -> logging.Logger:
    """Configure root logging once and return a namespaced logger."""
    resolved = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    return logging.getLogger("financial_pipeline")


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings."""

    project_root: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    synthetic_seed: int
    log_level: str

    def ensure_dirs(self) -> None:
        """Create runtime output dirs (bronze, silver, gold). Raw must already exist."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.gold_dir.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    """Load settings from environment with sensible local defaults."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:  # python-dotenv is optional at runtime
        pass
    else:
        root = get_project_root()
        env_file = root / ".env"
        if env_file.exists():
            load_dotenv(env_file)

    root = get_project_root()
    raw_dir = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
    bronze_dir = Path(os.getenv("BRONZE_DATA_DIR", "data/bronze"))
    silver_dir = Path(os.getenv("SILVER_DATA_DIR", "data/silver"))
    gold_dir = Path(os.getenv("GOLD_DATA_DIR", "data/gold"))
    if not raw_dir.is_absolute():
        raw_dir = root / raw_dir
    if not bronze_dir.is_absolute():
        bronze_dir = root / bronze_dir
    if not silver_dir.is_absolute():
        silver_dir = root / silver_dir
    if not gold_dir.is_absolute():
        gold_dir = root / gold_dir

    try:
        seed = int(os.getenv("SYNTHETIC_SEED", "42"))
    except ValueError as exc:
        raise ValueError("SYNTHETIC_SEED must be an integer") from exc

    return Settings(
        project_root=root,
        raw_dir=raw_dir,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        synthetic_seed=seed,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
