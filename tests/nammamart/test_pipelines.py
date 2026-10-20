"""
Runs the ETL and ELT task logic end to end outside Airflow, on a throwaway warehouse,
and checks the acceptance criteria that can be tested automatically:
  - ETL and ELT agree on every quality check and on September totals
  - re-running changes nothing (idempotency)
  - the increment file appends 150 orders and turns 10 into RETURNED, with no duplicates

Run from the project root:  pytest tests/nammamart -q
(Needs only pandas, duckdb and pytest; Airflow is not required.)
"""

import importlib
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "include"))

RUN_TS = pd.Timestamp("2026-10-14 09:00:00")
FILES = ["orders", "payments", "customers", "products", "stores"]
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_mode_revenue", "mart_top_customers", "mart_payment_reconciliation"]


@pytest.fixture()
def env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    for f in FILES:
        shutil.copy(ROOT / "assignment" / "nammamart_dataset" / f"{f}.csv", data)
    monkeypatch.setenv("NAMMAMART_DATA_DIR", str(data))
    monkeypatch.setenv("NAMMAMART_WAREHOUSE", str(tmp_path / "wh.duckdb"))
    monkeypatch.setenv("NAMMAMART_WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("NAMMAMART_QUARANTINE_DIR", str(tmp_path / "quarantine"))
    from nammamart import config, warehouse, quality, etl, elt
    for m in (config, warehouse, quality, etl, elt):
        importlib.reload(m)
    return {"data": data, "config": config, "etl": etl, "elt": elt}


def run_etl(etl, run_id):
    for f in FILES:
        etl.extract(f, run_id)
    etl.validate(run_id, RUN_TS)
    etl.transform(run_id, RUN_TS)
    load = etl.load(run_id)
    etl.reconcile(run_id)
    return etl.report(run_id), load


def run_elt(elt, run_id):
    elt.setup(run_id, RUN_TS)
    for f in FILES:
        elt.load_raw(f, run_id)
    elt.load_raw_increments(run_id)
    for t in ["stg_stores", "stg_products", "stg_customers"]:
        elt.run_sql(f"staging/{t}.sql", run_id, RUN_TS)
    merge = elt.merge_orders(run_id, RUN_TS)
    elt.run_sql("staging/stg_orders_dq.sql", run_id, RUN_TS)
    elt.run_sql("staging/stg_payments.sql", run_id, RUN_TS)
    elt.write_dq_results(run_id, RUN_TS)
    for m in MARTS:
        elt.build_mart(m, run_id, RUN_TS)
    return elt.summary_totals(), merge


def query(config, sql, params=None):
    con = config.connect()
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def test_etl_and_elt_agree(env):
    etl_totals, _ = run_etl(env["etl"], "r1")
    elt_totals, _ = run_elt(env["elt"], "r1")
    assert etl_totals == elt_totals
    diff = query(env["config"], """
        SELECT e.check_code, e.rows_failed, l.rows_failed
        FROM dq_results e JOIN dq_results l USING (run_id, check_code)
        WHERE e.dag_id LIKE 'nammamart_etl%' AND l.dag_id LIKE 'nammamart_elt%'
          AND e.rows_failed <> l.rows_failed
    """)
    assert diff == []
    recon = query(env["config"], """
        SELECT (SELECT count(*) FROM etl_payment_reconciliation),
               (SELECT count(*) FROM mart_payment_reconciliation)
    """)[0]
    assert recon[0] == recon[1] > 0
    # PII never appears unmasked in a reporting table.
    leaks = query(env["config"], """
        SELECT count(*) FROM etl_dim_customer WHERE email_masked NOT LIKE '%***@%'
           OR phone_masked NOT LIKE '%*%'
    """)[0][0]
    assert leaks == 0


def test_rerun_is_idempotent(env):
    run_etl(env["etl"], "r1")
    run_elt(env["elt"], "r1")
    counts = "SELECT (SELECT count(*) FROM etl_fact_orders), (SELECT count(*) FROM raw_orders), " \
             "(SELECT count(*) FROM stg_orders), (SELECT sum(net_revenue) FROM mart_daily_store_kpis)"
    first = query(env["config"], counts)
    run_etl(env["etl"], "r2")
    run_elt(env["elt"], "r2")
    assert query(env["config"], counts) == first


def test_increment_upserts(env):
    run_etl(env["etl"], "r1")
    run_elt(env["elt"], "r1")
    shutil.copy(ROOT / "assignment" / "nammamart_dataset" / "orders_2026-10-01_increment.csv", env["data"])
    _, load = run_etl(env["etl"], "r2")
    _, merge = run_elt(env["elt"], "r2")
    assert load["fact_rows_after"] - load["fact_rows_before"] == 150
    assert merge["orders_2026-10-01_increment.csv"] == {"inserted": 150, "updated_from_other_file": 10}
    dupes = query(env["config"], "SELECT count(*) - count(DISTINCT order_id) FROM etl_fact_orders")[0][0]
    assert dupes == 0
    returned = query(env["config"], """
        SELECT count(*) FROM etl_fact_orders f JOIN stg_orders s USING (order_id)
        WHERE f.order_id IN ('ORD000017','ORD000271','ORD000332','ORD000337','ORD000584',
                             'ORD000708','ORD000822','ORD001025','ORD001067','ORD001990')
          AND f.order_status = 'RETURNED' AND s.order_status = 'RETURNED'
    """)[0][0]
    assert returned == 10
    totals = query(env["config"], """
        SELECT (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders),
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis)
    """)[0]
    assert totals[0] == totals[1]
