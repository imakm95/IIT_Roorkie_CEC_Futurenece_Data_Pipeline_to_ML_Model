"""
include/nammamart/dag_utils.py
------------------------------
Airflow pieces shared by the two NammaMart DAGs: schedule, default_args, run time in IST,
the Slack failure alert and the run-audit (pipeline_runs) writer.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pendulum

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import Variable
except ImportError:  # Airflow 2 fallback
    from airflow.models import Variable

from nammamart import config, warehouse

IST = pendulum.timezone("Asia/Kolkata")

# 09:00 Monday to Saturday. The cron is read in the DAG's timezone, which comes from the
# timezone-aware start_date below, so this is 09:00 IST (03:30 UTC), not 09:00 UTC.
SCHEDULE = "0 9 * * 1-6"
START_DATE = pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")


def notify_failure(context) -> None:
    """on_failure_callback: post to Slack if the SLACK_WEBHOOK_URL Variable is set, else log."""
    from slack_notifier import send_slack_alert  # include/slack_notifier.py (trainer's helper)

    ti = context["ti"]
    message = (
        f":red_circle: NammaMart pipeline failure\n"
        f"DAG: {ti.dag_id}\nTask: {ti.task_id} (try {ti.try_number})\n"
        f"Run: {context['run_id']}\nError: {context.get('exception')}"
    )
    webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)
    send_slack_alert(message, webhook_url=webhook)


DEFAULT_ARGS = {
    "owner": f"fde-{config.LEARNER}",
    "retries": 2,                                # a locked warehouse or a late file gets two more tries
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),  # no single task may hang past the 9:30 stand-up
    "on_failure_callback": notify_failure,
}


def run_ts(context) -> pd.Timestamp:
    """The run's time in IST (naive), used by the 'not in the future' and freshness checks."""
    dag_run = context.get("dag_run")
    when = (getattr(dag_run, "run_after", None) or context.get("logical_date")
            or pendulum.now("UTC"))
    return pd.Timestamp(pendulum.instance(when).in_timezone(IST).naive())


def task_states(context) -> dict[str, str]:
    """State of every task in this run (Airflow 3 task SDK first, Airflow 2 fallback)."""
    ti, run_id = context["ti"], context["run_id"]
    try:
        states = ti.get_task_states(dag_id=ti.dag_id, run_ids=[run_id])
        return {k: str(v) for k, v in states.get(run_id, {}).items()}
    except AttributeError:
        return {t.task_id: str(t.state) for t in context["dag_run"].get_task_instances()}


def write_run_audit(context, dag_id, rows_extracted, rows_loaded, rows_quarantined) -> str:
    """Final all_done task: one row per run in pipeline_runs."""
    states = task_states(context)
    states.pop(context["ti"].task_id, None)
    failed = sorted(t for t, s in states.items()
                    if s.split(".")[-1].lower() in ("failed", "upstream_failed"))
    status = "failed" if failed else "success"
    dag_run = context["dag_run"]
    start = getattr(dag_run, "start_date", None)
    start = pendulum.instance(start).in_timezone(IST).naive() if start else None
    con = config.connect()
    end = pendulum.now(IST).naive()
    warehouse.write_pipeline_run(con, context["run_id"], dag_id, start, end, rows_extracted,
                                 rows_loaded, rows_quarantined, status, ", ".join(failed))
    con.close()
    print(f"[Audit] {dag_id} run {context['run_id']}: {status}. Failed tasks: {failed or 'none'}")
    if failed:
        # The audit task is the DAG's last task; failing it keeps the run red in the UI.
        raise RuntimeError(f"Run recorded as failed in pipeline_runs; failed tasks: {failed}")
    return status
