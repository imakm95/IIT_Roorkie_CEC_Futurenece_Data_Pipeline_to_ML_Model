"""
include/nammamart/elt.py
------------------------
Task bodies for the ELT DAG (dags/nammamart_elt_ashwani.py).

Extract + Load raw (no cleaning) -> Staging SQL -> Mart SQL, all inside DuckDB.
Python here only moves files into raw tables and runs the .sql files in include/sql/elt/.
Every transformation is SQL.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from nammamart import config, warehouse
from nammamart.dq_catalog import CHECKS

DAG_ID = f"nammamart_elt_{config.LEARNER}"

RAW_LOG_DDL = """
CREATE TABLE IF NOT EXISTS raw_load_log (
    run_id       VARCHAR,     -- run that saw the file
    source_file  VARCHAR,
    file_md5     VARCHAR,
    rows_in_file BIGINT,
    rows_loaded  BIGINT,      -- 0 when identical content was already in raw
    data_run_id  VARCHAR,     -- run whose raw rows hold this content
    status       VARCHAR,     -- loaded / skipped_identical
    logged_at    TIMESTAMP
)
"""


def _md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def _set_vars(con, run_id: str, run_ts: pd.Timestamp, **extra) -> None:
    con.execute("SET VARIABLE run_id = ?", [run_id])
    con.execute("SET VARIABLE run_ts = ?::TIMESTAMP", [run_ts.strftime("%Y-%m-%d %H:%M:%S")])
    for name, value in extra.items():
        con.execute(f"SET VARIABLE {name} = ?", [value])


def run_sql(rel_path: str, run_id: str, run_ts: pd.Timestamp, con=None, **variables) -> None:
    """Execute one .sql file from include/sql/elt/ with run variables set."""
    own = con is None
    con = con or config.connect()
    _set_vars(con, run_id, run_ts, **variables)
    sql = (config.SQL_DIR / rel_path).read_text()
    print(f"[SQL] {rel_path}")
    con.execute(sql)
    if own:
        con.close()


# ---------------------------------------------------------------------- setup

def setup(run_id: str, run_ts: pd.Timestamp) -> None:
    """Audit tables, check catalog, reference data and SQL macros."""
    con = config.connect()
    warehouse.ensure_audit_tables(con)
    con.execute(RAW_LOG_DDL)
    con.execute("CREATE OR REPLACE TABLE ref_payment_mode_alias (alias VARCHAR, standard VARCHAR)")
    con.executemany("INSERT INTO ref_payment_mode_alias VALUES (?, ?)",
                    list(config.PAYMENT_MODE_ALIASES.items()))
    run_sql("00_setup.sql", run_id, run_ts, con=con)
    con.close()


# ------------------------------------------------------------- extract + load raw

def _load_raw_file(con, path: Path, file_key: str, run_id: str) -> dict:
    """
    Append a file to raw_<file_key> exactly as received (every column as text) plus
    _loaded_at, _source_file, _run_id and _row_num. If the identical file content is
    already in raw, nothing is appended (a re-run must not double the raw table), but
    the run is still logged so staging knows which version is current.
    """
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    expected = config.EXPECTED_COLUMNS[file_key]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Schema change in {path.name}: missing column(s) {missing}. "
                         f"Found columns {list(df.columns)}")
    table = f"raw_{file_key}"
    cols = ", ".join(f'"{c}" VARCHAR' for c in expected)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
        {cols}, _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _row_num INTEGER)""")
    con.execute(RAW_LOG_DDL)

    md5 = _md5(path)
    # Same run retried after a partial failure: clear its own rows first.
    con.execute(f"DELETE FROM {table} WHERE _run_id = ? AND _source_file = ?", [run_id, path.name])
    con.execute("DELETE FROM raw_load_log WHERE run_id = ? AND source_file = ?", [run_id, path.name])
    seen = con.execute("""
        SELECT data_run_id FROM raw_load_log
        WHERE source_file = ? AND file_md5 = ? AND status = 'loaded' LIMIT 1
    """, [path.name, md5]).fetchone()

    if seen:
        data_run, loaded, status = seen[0], 0, "skipped_identical"
    else:
        raw = df[expected].copy()
        raw["_source_file"], raw["_run_id"] = path.name, run_id
        raw["_row_num"] = range(1, len(raw) + 1)
        con.register("raw_src", raw)
        con.execute(f"""
            INSERT INTO {table} BY NAME
            SELECT * EXCLUDE (_row_num), now()::TIMESTAMP AS _loaded_at, _row_num::INTEGER AS _row_num
            FROM raw_src
        """)
        con.unregister("raw_src")
        data_run, loaded, status = run_id, len(raw), "loaded"
    con.execute("INSERT INTO raw_load_log VALUES (?, ?, ?, ?, ?, ?, ?, now()::TIMESTAMP)",
                [run_id, path.name, md5, len(df), loaded, data_run, status])
    print(f"[Load raw] {path.name}: {len(df)} rows in file, {loaded} appended to {table} ({status})")
    return {"file": path.name, "rows": len(df), "appended": loaded, "status": status}


def load_raw(file_key: str, run_id: str) -> dict:
    con = config.connect()
    result = _load_raw_file(con, config.DATA_DIR / config.SOURCE_FILES[file_key], file_key, run_id)
    con.close()
    return result


def load_raw_increments(run_id: str) -> dict:
    """Append any next-day order files (orders_YYYY-MM-DD_increment.csv) to raw_orders."""
    files = sorted(config.DATA_DIR.glob(config.INCREMENT_GLOB))
    if not files:
        print("[Load raw] No increment files present")
        return {"files": [], "rows": 0, "appended": 0}
    con = config.connect()
    results = [_load_raw_file(con, f, "orders", run_id) for f in files]
    con.close()
    return {"files": [r["file"] for r in results], "rows": sum(r["rows"] for r in results),
            "appended": sum(r["appended"] for r in results)}


# --------------------------------------------------------------------- staging

def merge_orders(run_id: str, run_ts: pd.Timestamp) -> dict:
    """MERGE orders.csv, then each increment file in date order, into stg_orders."""
    con = config.connect()
    run_sql("staging/stg_orders_create.sql", run_id, run_ts, con=con)
    versions = con.execute("""
        SELECT source_file, data_run_id FROM raw_load_log
        WHERE run_id = ? AND (source_file = 'orders.csv' OR source_file LIKE 'orders_%_increment.csv')
        ORDER BY CASE WHEN source_file = 'orders.csv' THEN 0 ELSE 1 END, source_file
    """, [run_id]).fetchall()
    stats = {}
    for src_file, src_run in versions:
        before = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
        matched = con.execute("""
            SELECT count(*) FROM stg_orders t
            WHERE t.order_id IN (SELECT trim(order_id) FROM raw_orders WHERE _source_file = ? AND _run_id = ?)
              AND t._source_file <> ?
        """, [src_file, src_run, src_file]).fetchone()[0]
        run_sql("staging/stg_orders_merge.sql", run_id, run_ts, con=con,
                src_file=src_file, src_run=src_run)
        after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
        stats[src_file] = {"inserted": after - before, "updated_from_other_file": matched}
        print(f"[MERGE] {src_file}: inserted {after - before}, "
              f"overwrote {matched} rows that came from an earlier file")
    con.close()
    return stats


# ----------------------------------------------------------------- dq_results

def write_dq_results(run_id: str, run_ts: pd.Timestamp) -> dict:
    """Count, in SQL, how many staging rows each check flagged, and store it in dq_results."""
    con = config.connect()
    tables = {"orders": "stg_orders", "payments": "stg_payments", "customers": "stg_customers",
              "products": "stg_products", "stores": "stg_stores"}
    counts = {}
    for file_key, table in tables.items():
        rows = con.execute(f"""
            SELECT c.code, (SELECT count(*) FROM {table}) AS checked,
                   (SELECT count(*) FROM {table} t WHERE list_contains(t.dq_codes, c.code)) AS failed
            FROM dq_check_catalog c WHERE c.file = ?
        """, [file_key]).fetchall()
        for code, checked, failed in rows:
            counts[code] = (checked, failed)

    # Dataset-level checks: freshness and volume of the order files received this run.
    newest = con.execute(
        "SELECT max(order_ts) FROM stg_orders WHERE order_ts <= ?::TIMESTAMP",
        [run_ts.strftime("%Y-%m-%d %H:%M:%S")]).fetchone()[0]
    stale = newest is None or pd.Timestamp(newest) < run_ts - pd.Timedelta(hours=config.FRESHNESS_HOURS)
    received = con.execute("""
        SELECT coalesce(sum(rows_in_file), 0) FROM raw_load_log
        WHERE run_id = ? AND (source_file = 'orders.csv' OR source_file LIKE 'orders_%_increment.csv')
    """, [run_id]).fetchone()[0]
    previous = warehouse.previous_order_rows(con, DAG_ID, run_id)
    dropped = bool(previous) and received < previous * (1 - config.VOLUME_DROP_PCT / 100)
    counts["ord_freshness"] = (1, int(stale))
    counts["ord_volume_drop"] = (int(received), int(dropped))
    print(f"[DQ] newest order_ts={newest}; order rows received={received}, previous good run={previous}")

    rows = []
    for code, (checked, failed) in counts.items():
        file_key, name, dim, sev, _ = CHECKS[code]
        if code == "ord_volume_drop":
            checked = received
        rows.append({"check_code": code, "check_name": f"{file_key}: {name}", "dimension": dim,
                     "severity": sev, "rows_checked": checked, "rows_failed": failed,
                     "status": "PASS" if failed == 0 else ("FAIL" if sev == "Critical" else "WARN")})
    warehouse.write_dq_results(con, run_id, DAG_ID, rows)
    print(con.execute("""
        SELECT check_name, dimension, severity, rows_checked, rows_failed, status
        FROM dq_results WHERE run_id = ? AND dag_id = ? AND rows_failed > 0
        ORDER BY severity, rows_failed DESC
    """, [run_id, DAG_ID]).df().to_string(index=False))
    summary = {t: dict(zip(["rows", "fail", "warn"], con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE dq_status = 'FAIL'), "
        f"count(*) FILTER (WHERE dq_status = 'WARN') FROM {t}").fetchone())) for t in tables.values()}
    con.close()
    return summary


# ------------------------------------------------------------------------ marts

def build_mart(name: str, run_id: str, run_ts: pd.Timestamp) -> int:
    con = config.connect()
    run_sql("mart/00_int_orders_clean.sql", run_id, run_ts, con=con)  # shared view of passed orders
    if name == "mart_payment_reconciliation":
        sql = (config.INCLUDE_DIR / "sql" / "reconciliation.sql").read_text().format(
            orders="int_orders_clean",
            payments="(SELECT *, dq_status AS payment_dq_status FROM stg_payments "
                     "WHERE _dup_rank = 1 AND payment_id IS NOT NULL)",
            target="mart_payment_reconciliation",
        )
        print("[SQL] include/sql/reconciliation.sql -> mart_payment_reconciliation")
        con.execute(sql)
    else:
        run_sql(f"mart/{name}.sql", run_id, run_ts, con=con)
    n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    print(con.execute(f"SELECT * FROM {name} LIMIT 10").df().to_string(index=False))
    con.close()
    return n


def summary_totals() -> dict:
    con = config.connect()
    row = con.execute("""
        SELECT sum(delivered_orders), round(sum(net_revenue), 2), round(sum(gross_margin), 2)
        FROM mart_daily_store_kpis
    """).fetchone()
    con.close()
    return {"delivered_orders": int(row[0]), "net_revenue": row[1], "gross_margin": row[2]}
