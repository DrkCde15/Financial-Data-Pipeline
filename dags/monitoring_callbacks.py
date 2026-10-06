"""Callbacks do Airflow -> pipeline-monitoring (import seguro).

Registra uma run por task (dag_id.task_id) com início/fim reais via
record_finished_run. Falha no monitoring nunca quebra a task (só loga).
Sem pipeline_monitoring instalado, os callbacks viram no-op e a DAG
continua parseando normalmente.

No compose: montar pipeline-monitoring/src em /opt/monitoring/src,
PYTHONPATH com /opt/monitoring/src e DATABASE_PATH absoluto para o
data/ do projeto. Callbacks não reportam linhas (rows=0): ajuste
MIN_ROWS_EXPECTED=0 ou a regra de volume dispara para toda task.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

try:
    from pipeline_monitoring.alerts import evaluate_run
    from pipeline_monitoring.config import load_settings
    from pipeline_monitoring.store import (
        init_db,
        record_finished_run,
        register_pipeline,
    )

    _HAS_MONITORING = True
except ImportError:  # Airflow sem monitoring: callbacks viram no-op
    _HAS_MONITORING = False

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record(context, status: str) -> None:
    """Registra a task finalizada; nunca levanta."""
    if not _HAS_MONITORING:
        return
    try:
        ti = context["task_instance"]
        settings = load_settings()
        db = settings.database_path
        init_db(db)
        name = f"{ti.dag_id}.{ti.task_id}"
        register_pipeline(db, name)
        start = ti.start_date.isoformat() if ti.start_date else _utcnow_iso()
        end = ti.end_date.isoformat() if ti.end_date else _utcnow_iso()
        error = "" if status == "success" else str(
            context.get("exception") or "task failed"
        )
        run = record_finished_run(
            db, name, status=status,
            started_at=start, finished_at=end, error_message=error,
        )
        for alert in evaluate_run(run, settings.thresholds):
            logger.warning("[monitoring:%s] %s", alert.rule, alert.message)
    except Exception:
        logger.exception("monitoring callback falhou (ignorado)")


def on_task_success(context) -> None:
    """Callback de sucesso: registra a run da task."""
    _record(context, "success")


def on_task_failure(context) -> None:
    """Callback de falha: registra a run com o erro sanitizado."""
    _record(context, "failed")
