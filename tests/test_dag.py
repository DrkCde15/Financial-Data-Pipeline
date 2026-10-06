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
    "load_bigquery",
    "bq_checks",
]


def test_dag_file_exists_and_parses() -> None:
    """DAG file must exist and be valid Python."""
    assert DAG_FILE.exists(), f"missing {DAG_FILE}"
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    assert tree is not None


def test_dag_declares_expected_tasks_in_order() -> None:
    """As 6 task_ids em cadeia linear até o BQ (serving único)."""
    src = DAG_FILE.read_text(encoding="utf-8")
    for t in EXPECTED_TASKS:
        assert f'task_id="{t}"' in src, f"task {t!r} missing in DAG"
    assert "generate >> bronze >> silver >> gold >> bq_load >> bq_checks" in src
    assert "load_postgres" not in src, "serving Postgres foi removido"


def test_dag_targets_airflow_26_api() -> None:
    """Uses Airflow 2.6-compatible imports (no TaskFlow 3.x)."""
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "from airflow.operators.bash import BashOperator" in src
    assert "schedule=" in src or "schedule_interval" in src
    assert "catchup=False" in src


def test_compose_declares_postgres_and_airflow_26() -> None:
    """Compose: postgres:15 (só metadados do Airflow) + airflow:2.6.3 + ADC p/ BQ."""
    compose = (get_project_root() / "docker-compose.yml").read_text(encoding="utf-8")
    assert "postgres:15" in compose
    assert "apache/airflow:2.6.3" in compose
    assert "DATABASE_URL" not in compose, "serving Postgres foi removido"
    assert ".config/gcloud" in compose


def test_dag_wires_monitoring_callbacks() -> None:
    """DAG registra runs via callbacks (default_args), com import seguro."""
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "from monitoring_callbacks import" in src
    assert "on_success_callback" in src and "on_failure_callback" in src
    hook = (get_project_root() / "dags" / "monitoring_callbacks.py").read_text(
        encoding="utf-8"
    )
    assert "record_finished_run" in hook
    assert "except ImportError" in hook, "hook precisa ser no-op sem monitoring"


def test_compose_wires_monitoring() -> None:
    """Compose monta o src do monitoring e aponta o SQLite p/ o data/ local."""
    compose = (get_project_root() / "docker-compose.yml").read_text(encoding="utf-8")
    assert "pipeline-monitoring/src:/opt/monitoring/src" in compose
    assert "DATABASE_PATH" in compose
    assert "monitoring.db" in compose
    gitignore = (get_project_root() / ".gitignore").read_text(encoding="utf-8")
    assert "monitoring.db" in gitignore
