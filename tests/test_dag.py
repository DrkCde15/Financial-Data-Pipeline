"""Tests for the Airflow DAG (static parse — no airflow install required)."""

from __future__ import annotations

import ast
from pathlib import Path

from financial_pipeline.config import get_project_root

DAG_FILE = get_project_root() / "dags" / "financial_pipeline.py"

EXPECTED_TASKS = [
    "generate_synthetic",
    "bronze",
    "silver",
    "gold",
    "load_postgres",
    "quality_checks",
]


def test_dag_file_exists_and_parses() -> None:
    """DAG file must exist and be valid Python."""
    assert DAG_FILE.exists(), f"missing {DAG_FILE}"
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    assert tree is not None


def test_dag_declares_expected_tasks_in_order() -> None:
    """All 6 task_ids declared; chain generate >> bronze >> ... >> checks."""
    src = DAG_FILE.read_text(encoding="utf-8")
    for t in EXPECTED_TASKS:
        assert f'task_id="{t}"' in src, f"task {t!r} missing in DAG"
    assert "generate >> bronze >> silver >> gold >> load >> checks" in src


def test_dag_targets_airflow_26_api() -> None:
    """Uses Airflow 2.6-compatible imports (no TaskFlow 3.x)."""
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "from airflow.operators.bash import BashOperator" in src
    assert "schedule=" in src or "schedule_interval" in src
    assert "catchup=False" in src


def test_compose_declares_postgres_and_airflow_26() -> None:
    """docker-compose must wire postgres:15 + airflow:2.6.3."""
    compose = (get_project_root() / "docker-compose.yml").read_text(encoding="utf-8")
    assert "postgres:15" in compose
    assert "apache/airflow:2.6.3" in compose
    assert "DATABASE_URL" in compose
