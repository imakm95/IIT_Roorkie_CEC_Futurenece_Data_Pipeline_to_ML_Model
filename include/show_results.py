"""
Prints the tables needed for the assignment screenshots, read-only, from the warehouse.

Run inside Astro after both DAGs are green:
    astro dev bash
    python include/show_results.py            # all sections
    python include/show_results.py pipeline   # one section: pipeline | dq | recon | quarantine
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from nammamart import config  # noqa: E402

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.max_rows", 200)

# latest run of each DAG (scheduled runs of both DAGs share a run_id, so match on dag_id too)
LATEST_RUN = ("(SELECT dag_id, run_id FROM pipeline_runs "
              "QUALIFY row_number() OVER (PARTITION BY dag_id ORDER BY end_time DESC) = 1)")

SECTIONS = {
    "pipeline": [
        ("pipeline_runs (latest 10)",
         "SELECT dag_id, run_id, status, start_time, end_time, "
         "round(epoch(end_time - start_time), 1) AS duration_s, rows_extracted, rows_loaded, "
         "rows_quarantined, failed_tasks FROM pipeline_runs ORDER BY end_time DESC LIMIT 10"),
    ],
    "dq": [
        ("dq_results summary by DAG and dimension (all 6 dimensions)",
         "SELECT d.dag_id, d.dimension, d.severity, count(*) AS checks, CAST(sum(d.rows_failed) AS BIGINT) AS rows_failed "
         "FROM dq_results d JOIN " + LATEST_RUN + " USING (dag_id, run_id) "
         "GROUP BY ALL ORDER BY d.dag_id, d.dimension, d.severity"),
        ("dq_results failing checks, latest run of each DAG",
         "SELECT d.dag_id, d.check_name, d.dimension, d.severity, d.rows_checked, d.rows_failed, d.status "
         "FROM dq_results d JOIN " + LATEST_RUN + " USING (dag_id, run_id) WHERE d.rows_failed > 0 "
         "ORDER BY d.dag_id, d.severity, d.rows_failed DESC"),
    ],
    "recon": [
        ("etl_payment_reconciliation (summary)",
         "SELECT coalesce(mismatch_type, 'TOTAL') AS mismatch, count(*) AS rows, "
         "round(sum(abs(amount_difference)), 2) AS unreconciled_amount "
         "FROM etl_payment_reconciliation GROUP BY ROLLUP (mismatch_type) ORDER BY grouping(mismatch_type), 1"),
        ("mart_payment_reconciliation (summary)",
         "SELECT coalesce(mismatch_type, 'TOTAL') AS mismatch, count(*) AS rows, "
         "round(sum(abs(amount_difference)), 2) AS unreconciled_amount "
         "FROM mart_payment_reconciliation GROUP BY ROLLUP (mismatch_type) ORDER BY grouping(mismatch_type), 1"),
        ("mart_payment_reconciliation (first 15 rows)",
         "SELECT order_id, payment_id, mismatch_type, order_status, payment_status, order_amount, amount_paid, "
         "amount_difference FROM mart_payment_reconciliation ORDER BY order_id LIMIT 15"),
    ],
}


def show_quarantine():
    print(f"\n=== Quarantine files in {config.QUARANTINE_DIR} ===")
    files = sorted(config.QUARANTINE_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files[:10]:
        print(" ", p.name)
    orders = [p for p in files if p.name.startswith("orders_")]
    if orders:
        print(f"\n--- first 15 rows of {orders[0].name} ---")
        df = pd.read_csv(orders[0], dtype=str)
        cols = [c for c in ("order_id", "order_ts", "customer_id", "store_id", "quantity", "order_status",
                            "failure_reason") if c in df.columns]
        print(f"{len(df)} rows rejected")
        print(df[cols].head(15).to_string(index=False))


def main(wanted):
    con = config.connect(read_only=True)
    try:
        for key, queries in SECTIONS.items():
            if wanted and key not in wanted:
                continue
            for title, sql in queries:
                print(f"\n=== {title} ===")
                try:
                    print(con.execute(sql).df().to_string(index=False))
                except Exception as exc:  # table missing until the DAG has run
                    print(f"  (not available yet: {exc.__class__.__name__}: {str(exc).splitlines()[0]})")
    finally:
        con.close()
    if not wanted or "quarantine" in wanted:
        show_quarantine()


if __name__ == "__main__":
    main(set(sys.argv[1:]))
