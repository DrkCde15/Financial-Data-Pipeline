"""Tests for centralized configuration."""

from __future__ import annotations

from financial_pipeline.config import get_project_root, load_settings


def test_get_project_root_contains_pyproject() -> None:
    """Project root must be the folder holding pyproject.toml."""
    root = get_project_root()
    assert (root / "pyproject.toml").exists()
    assert root.name == "financial-data-pipeline"


def test_load_settings_defaults() -> None:
    """Default settings resolve to <root>/data/raw and <root>/data/bronze."""
    settings = load_settings()
    assert settings.raw_dir == settings.project_root / "data" / "raw"
    assert settings.bronze_dir == settings.project_root / "data" / "bronze"
    assert settings.synthetic_seed == 42
    assert settings.log_level.upper() in ("DEBUG", "INFO", "WARNING", "ERROR")


def test_load_settings_rejects_bad_seed(monkeypatch) -> None:
    """Non-integer SYNTHETIC_SEED must fail fast with a clear error."""
    import pytest

    monkeypatch.setenv("SYNTHETIC_SEED", "not-an-int")
    with pytest.raises(ValueError, match="SYNTHETIC_SEED"):
        load_settings()
