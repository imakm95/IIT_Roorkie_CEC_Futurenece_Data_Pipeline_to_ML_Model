"""
dags/nammamart_etl_ashwani.py
---------------------------
NammaMart ETL pipeline: Extract -> Validate -> Transform -> Load -> Reconcile -> Report.

Flow:
  extract_orders/payments/customers/products/stores (in parallel)
  -> validate (quarantine + dq_results) -> transform -> load -> reconcile -> report
  -> run_audit (all_done)

Bad rows are removed in Python BEFORE the warehouse: only rows that pass every Critical
check reach etl_fact_orders. Failed rows go to include/quarantine/ashwani/<file>_failed_<run>.csv
with a failure_reason column, and to the etl_quarantine table.

Logic lives in include/nammamart/etl.py; this file only wires tasks together.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # Airflow 2 fallback
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

from nammamart import config, dag_utils, etl

SOURCES = ["orders", "payments", "customers", "products", "stores"]


@dag(
    dag_id=etl.DAG_ID,
    description="NammaMart ETL: validate in Python, load only clean rows to a DuckDB star schema",
    schedule=dag_utils.SCHEDULE,           # 09:00 IST, Monday to Saturday
    start_date=dag_utils.START_DATE,       # timezone-aware (Asia/Kolkata)
    catchup=False,                         # each run re-reads the full export; no backfill needed
    max_active_runs=1,                     # two runs at once would fight over the warehouse
    dagrun_timeout=dag_utils.timedelta(minutes=25),
    default_args=dag_utils.DEFAULT_ARGS,
    tags=["nammamart", "etl", config.LEARNER],
)
def nammamart_etl_ashwani():

    def _ctx():
        ctx = get_current_context()
        return ctx, ctx["run_id"], dag_utils.run_ts(ctx)

    @task
    def extract(file_key: str) -> dict:
        _, run_id, _ = _ctx()
        return etl.extract(file_key, run_id)

    @task
    def validate(extracts: list) -> dict:
        _, run_id, ts = _ctx()
        print(f"[Validate] run time (IST) used for timeliness checks: {ts}")
        return etl.validate(run_id, ts)

    @task
    def transform(validation: dict) -> dict:
        _, run_id, ts = _ctx()
        return etl.transform(run_id, ts)

    @task
    def load(transformed: dict) -> dict:
        _, run_id, _ = _ctx()
        return etl.load(run_id)

    @task
    def reconcile(loaded: dict) -> int:
        _, run_id, _ = _ctx()
        return etl.reconcile(run_id)

    @task
    def report(mismatches: int) -> dict:
        _, run_id, _ = _ctx()
        return etl.report(run_id)

    @task(trigger_rule="all_done", retries=0, on_failure_callback=None)
    def run_audit() -> str:
        """Runs even if something upstream failed, and records the run in pipeline_runs."""
        ctx, _, _ = _ctx()
        ti = ctx["ti"]
        # xcom_pull returns None for tasks that failed before returning anything.
        extracts = [ti.xcom_pull(task_ids=f"extract_{name}") for name in SOURCES]
        validation = ti.xcom_pull(task_ids="validate") or {}
        loaded = ti.xcom_pull(task_ids="load") or {}
        extracted = sum(e["rows"] for e in extracts if e)
        quarantined = sum(v["quarantined"] for v in validation.values())
        rows_loaded = loaded.get("inserted", 0) + loaded.get("upserted_existing", 0)
        return dag_utils.write_run_audit(ctx, etl.DAG_ID, extracted, rows_loaded, quarantined)

    extracts = [extract.override(task_id=f"extract_{name}")(name) for name in SOURCES]
    loaded = load(transform(validate(extracts)))
    report(reconcile(loaded)) >> run_audit()


nammamart_etl_ashwani()
