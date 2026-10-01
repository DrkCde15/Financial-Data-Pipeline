"""Airflow DAG: financial-data-pipeline (generate -> bronze -> silver -> gold -> BQ).

Serving único: BigQuery sandbox. (O Postgres do compose é só o banco de
metadados do Airflow — não entra no pipeline.)

Compatível com Airflow 2.6 (BashOperator clássicos, sem TaskFlow 3.x).
Agendamento diário, backfill por logical date: cada run materializa a partição
ingestion_date={{ ds }} (idempotente — re-run sobrescreve).

Requer: pip install -e ".[cloud]" + apache-airflow==2.6.* (ver docker-compose.yml).
BQ usa o ADC do host montado no container. Local sem Airflow: README na ordem.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

PROJECT_ROOT = os.getenv("FINANCIAL_PIPELINE_ROOT", "/opt/airflow/financial-data-pipeline")

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator

    _HAS_AIRFLOW = True
except ImportError:  # import seguro p/ testes sem airflow instalado
    DAG = None  # type: ignore
    BashOperator = None  # type: ignore
    _HAS_AIRFLOW = False


def _cmd(script: str, extra: str = "") -> str:
    ds = "{{ ds }}"
    date_flag = f" --date {ds}" if "run_" in script or "load_" in script else ""
    return f"cd {PROJECT_ROOT} && python scripts/{script}{date_flag}{extra}"


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

TASK_IDS = [
    "generate_synthetic",
    "bronze",
    "silver",
    "gold",
    "load_bigquery",
    "bq_checks",
]

if _HAS_AIRFLOW:
    with DAG(
        dag_id="financial_data_pipeline",
        description="Medallion: synthetic -> bronze -> silver -> gold -> BigQuery",
        schedule="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["medallion", "portfolio", "bigquery"],
    ) as dag:
        generate = BashOperator(
            task_id="generate_synthetic",
            bash_command=_cmd("generate_synthetic_data.py"),
        )
        bronze = BashOperator(
            task_id="bronze",
            bash_command=_cmd("run_bronze_ingestion.py"),
        )
        silver = BashOperator(
            task_id="silver",
            bash_command=_cmd("run_silver.py"),
        )
        gold = BashOperator(
            task_id="gold",
            bash_command=_cmd("run_gold.py"),
        )
        bq_load = BashOperator(
            task_id="load_bigquery",
            bash_command=_cmd("load_bigquery.py"),
        )
        bq_checks = BashOperator(
            task_id="bq_checks",
            bash_command=_cmd("load_bigquery.py", " --checks"),
        )

        generate >> bronze >> silver >> gold >> bq_load >> bq_checks
