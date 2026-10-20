"""
dags/nammamart_elt_ashwani.py
---------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in the warehouse with SQL.

Flow:
  setup -> load_raw_* (6 files, in parallel)
  load_raw_orders + load_raw_orders_increment -> stg_orders_merge
  load_raw_stores/products/customers -> stg_stores/products/customers
  -> stg_orders_dq -> stg_payments -> dq_results -> 6 mart tasks -> run_audit (all_done)

Raw    raw_*      every source row exactly as received, as text, + _loaded_at/_source_file/_run_id
Stage  stg_*      typed, standardised, de-duplicated, PII masked, with dq_status + failure_reason
Mart   mart_*     business tables built only from rows that did not FAIL

Every transformation is a .sql file in include/sql/elt/. Python only copies files into raw.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # Airflow 2 fallback
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

from nammamart import config, dag_utils, elt

SOURCES = ["orders", "payments", "customers", "products", "stores"]
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_mode_revenue", "mart_top_customers", "mart_payment_reconciliation"]


@dag(
    dag_id=elt.DAG_ID,
    description="NammaMart ELT: load every raw row to DuckDB, then clean and model with SQL",
    schedule=dag_utils.SCHEDULE,           # 09:00 IST, Monday to Saturday
    start_date=dag_utils.START_DATE,       # timezone-aware (Asia/Kolkata)
    catchup=False,                         # raw keeps history already; no backfill of missed days
    max_active_runs=1,
    dagrun_timeout=dag_utils.timedelta(minutes=25),
    default_args=dag_utils.DEFAULT_ARGS,
    tags=["nammamart", "elt", config.LEARNER],
)
def nammamart_elt_ashwani():

    def _ctx():
        ctx = get_current_context()
        return ctx, ctx["run_id"], dag_utils.run_ts(ctx)

    @task
    def setup() -> None:
        _, run_id, ts = _ctx()
        elt.setup(run_id, ts)

    @task
    def load_raw(file_key: str) -> dict:
        _, run_id, _ = _ctx()
        return elt.load_raw(file_key, run_id)

    @task
    def load_raw_orders_increment() -> dict:
        _, run_id, _ = _ctx()
        return elt.load_raw_increments(run_id)

    @task
    def stg_table(name: str) -> None:
        _, run_id, ts = _ctx()
        elt.run_sql(f"staging/{name}.sql", run_id, ts)

    @task
    def stg_orders_merge() -> dict:
        _, run_id, ts = _ctx()
        return elt.merge_orders(run_id, ts)

    @task
    def dq_results() -> dict:
        _, run_id, ts = _ctx()
        return elt.write_dq_results(run_id, ts)

    @task
    def mart(name: str) -> int:
        _, run_id, ts = _ctx()
        return elt.build_mart(name, run_id, ts)

    @task(trigger_rule="all_done", retries=0, on_failure_callback=None)
    def run_audit() -> str:
        """Runs even if something upstream failed, and records the run in pipeline_runs."""
        ctx, _, _ = _ctx()
        ti = ctx["ti"]
        raws = [ti.xcom_pull(task_ids=f"load_raw_{name}") for name in SOURCES]
        raws.append(ti.xcom_pull(task_ids="load_raw_orders_increment"))
        dq = ti.xcom_pull(task_ids="dq_results") or {}
        extracted = sum(r["rows"] for r in raws if r)
        appended = sum(r["appended"] for r in raws if r)   # rows_loaded = rows added to raw
        failed_rows = sum(t["fail"] for t in dq.values())  # rows marked FAIL in staging
        if dq:
            print(f"[Audit] staging summary: {dq}")
        return dag_utils.write_run_audit(ctx, elt.DAG_ID, extracted, appended, failed_rows)

    start = setup()
    raw = {name: load_raw.override(task_id=f"load_raw_{name}")(name) for name in SOURCES}
    raw_increment = load_raw_orders_increment()
    start >> list(raw.values()) + [raw_increment]

    stg_masters = []
    for name in ["stores", "products", "customers"]:
        t = stg_table.override(task_id=f"stg_{name}")(f"stg_{name}")
        raw[name] >> t
        stg_masters.append(t)

    merged = stg_orders_merge()
    [raw["orders"], raw_increment] >> merged

    orders_dq = stg_table.override(task_id="stg_orders_dq")("stg_orders_dq")
    [merged, *stg_masters] >> orders_dq

    payments = stg_table.override(task_id="stg_payments")("stg_payments")
    [orders_dq, raw["payments"]] >> payments

    checks = dq_results()
    payments >> checks

    marts = [mart.override(task_id=name)(name) for name in MARTS]
    checks >> marts
    marts >> run_audit()


nammamart_elt_ashwani()
