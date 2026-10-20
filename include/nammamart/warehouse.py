"""
include/nammamart/warehouse.py
------------------------------
Audit and quality tables shared by both DAGs: dq_results, pipeline_runs, dq_check_catalog.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from nammamart import config
from nammamart.dq_catalog import catalog_rows

AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_code    VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,
    checked_at    TIMESTAMP
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR,
    failed_tasks      VARCHAR
);
"""


def ensure_audit_tables(con) -> None:
    con.execute(AUDIT_DDL)
    con.execute("""
        CREATE OR REPLACE TABLE dq_check_catalog (
            code VARCHAR, file VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, reason VARCHAR, ordinal INTEGER)
    """)
    con.executemany(
        "INSERT INTO dq_check_catalog VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(*row, i) for i, row in enumerate(catalog_rows())],
    )


def write_dq_results(con, run_id: str, dag_id: str, rows: list[dict]) -> None:
    """Replace this run's results for the files checked, so a retried task doesn't double count."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["run_id"], df["dag_id"], df["checked_at"] = run_id, dag_id, datetime.now()
    con.register("dq_new", df)
    con.execute("""
        DELETE FROM dq_results
        WHERE run_id = ? AND dag_id = ? AND check_code IN (SELECT check_code FROM dq_new)
    """, [run_id, dag_id])
    con.execute("""
        INSERT INTO dq_results
        SELECT run_id, dag_id, check_code, check_name, dimension, severity,
               rows_checked, rows_failed, status, checked_at
        FROM dq_new
    """)
    con.unregister("dq_new")


def previous_order_rows(con, dag_id: str, run_id: str) -> int | None:
    """Order rows checked on the last successful run of this DAG, for the volume check."""
    con.execute(AUDIT_DDL)
    row = con.execute("""
        SELECT d.rows_checked
        FROM dq_results d
        JOIN pipeline_runs r USING (run_id, dag_id)
        WHERE d.dag_id = ? AND d.run_id <> ? AND r.status = 'success'
          AND d.check_code = 'ord_volume_drop'
        ORDER BY r.end_time DESC LIMIT 1
    """, [dag_id, run_id]).fetchone()
    return row[0] if row else None


def write_pipeline_run(con, run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded,
                       rows_quarantined, status, failed_tasks="") -> None:
    con.execute(AUDIT_DDL)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded,
         rows_quarantined, status, failed_tasks],
    )
    print(con.execute(
        "SELECT * FROM pipeline_runs WHERE dag_id = ? ORDER BY end_time DESC LIMIT 5", [dag_id]
    ).df().to_string(index=False))
