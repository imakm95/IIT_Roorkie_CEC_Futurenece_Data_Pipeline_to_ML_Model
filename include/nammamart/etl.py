"""
include/nammamart/etl.py
------------------------
Task bodies for the ETL DAG (dags/nammamart_etl_ashwani.py).

Extract -> Validate (pandas) -> Quarantine -> Transform (pandas) -> Load (DuckDB star schema)
-> Reconcile -> Report.

Tasks hand data to each other through pickle files in include/tmp/ashwani/etl/<run>/,
and only pass small dicts (paths and counts) through XCom.
"""

from __future__ import annotations

import pandas as pd

from nammamart import config, quality, warehouse

DAG_ID = f"nammamart_etl_{config.LEARNER}"
MASTERS = ["customers", "products", "stores"]


def _work(run_id: str):
    path = config.WORK_DIR / "etl" / config.run_slug(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _save(df: pd.DataFrame, run_id: str, name: str) -> str:
    path = _work(run_id) / f"{name}.pkl"
    df.to_pickle(path)
    return str(path)


def _load(run_id: str, name: str) -> pd.DataFrame:
    return pd.read_pickle(_work(run_id) / f"{name}.pkl")


# ------------------------------------------------------------------------ extract

def extract(file_key: str, run_id: str) -> dict:
    """Read one source file as text. For orders, also read any increment files."""
    if file_key == "orders":
        paths = quality.order_files()
    else:
        paths = [config.DATA_DIR / config.SOURCE_FILES[file_key]]
    frames = []
    for seq, path in enumerate(paths):
        df = quality.read_source(path, file_key)
        df["_file_seq"] = seq
        frames.append(df)
        print(f"[Extract] {path.name}: {len(df)} rows")
    df = pd.concat(frames, ignore_index=True)
    _save(df, run_id, f"raw_{file_key}")
    return {"file": file_key, "rows": len(df), "source_files": [p.name for p in paths]}


# ----------------------------------------------------------------------- validate

def _effective_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Apply increment files on top of orders.csv: for an order_id present in a later file,
    the later file's row wins (e.g. DELIVERED -> RETURNED). Superseded rows are history,
    not errors, so they are dropped here rather than quarantined. This mirrors the ELT
    MERGE, which matches on (order_id, copy number).
    """
    orders["_dup_rank"] = quality._dup_rank(orders, "order_id")
    latest = orders.groupby(["order_id", "_dup_rank"])["_file_seq"].transform("max")
    superseded = orders["order_id"].notna() & (orders["_file_seq"] < latest)
    updated = orders.loc[superseded, "order_id"].nunique()
    return orders.loc[~superseded].copy(), int(updated)


def validate(run_id: str, run_ts: pd.Timestamp) -> dict:
    """Run every check, split each file into passed / failed, quarantine failures."""
    con = config.connect()
    warehouse.ensure_audit_tables(con)
    previous_rows = warehouse.previous_order_rows(con, DAG_ID, run_id)

    data = {k: quality.standardise(_load(run_id, f"raw_{k}"), k) for k in config.SOURCE_FILES}

    flags = {
        "customers": quality.check_customers(data["customers"], run_ts),
        "products": quality.check_products(data["products"], run_ts),
        "stores": quality.check_stores(data["stores"], run_ts),
    }
    received = len(data["orders"])
    orders, updated = _effective_orders(data["orders"])
    data["orders"] = orders
    flags["orders"], dataset = quality.check_orders(
        orders, run_ts, data["customers"], data["products"], data["stores"], received, previous_rows
    )
    payments = data["payments"]
    payments["_dup_rank"] = quality._dup_rank(payments, "payment_id")
    flags["payments"] = quality.check_payments(payments, run_ts, data["orders"], orders)

    dq, summary = [], {}
    slug = config.run_slug(run_id)
    config.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    quarantined_all = []
    for key, df in data.items():
        checked = quality.apply_flags(df, flags[key])
        dq += quality.dq_rows(key, len(df), flags[key], dataset if key == "orders" else None)
        passed = checked[checked["dq_status"] != "FAIL"]
        failed = checked[checked["dq_status"] == "FAIL"]
        _save(checked, run_id, f"checked_{key}")
        q_path = None
        if len(failed):
            # Quarantine the row exactly as received, plus where it came from and why.
            raw_cols = config.EXPECTED_COLUMNS[key]
            q = failed[[c + "_raw" for c in raw_cols]].copy()
            q.columns = raw_cols
            q["_source_file"], q["_row_num"] = failed["_source_file"], failed["_row_num"]
            q["failure_reason"] = failed["failure_reason"]
            if key == "customers":
                # Quarantine is readable by data engineers, so personal data is masked here too.
                # The exact original stays in the source file at _row_num (restricted folder).
                q["full_name"] = q["full_name"].map(quality.mask_name)
                q["email"] = q["email"].map(lambda e: quality.mask_email(str(e).strip()) or "<invalid>")
                q["phone"] = q["phone"].map(quality.mask_phone)
            q_path = config.QUARANTINE_DIR / f"{key}_failed_{slug}.csv"
            q.to_csv(q_path, index=False)
            q.insert(0, "file", key)
            quarantined_all.append(q)
        summary[key] = {"rows_in": len(df), "passed": len(passed), "quarantined": len(failed),
                        "warnings": int((checked["dq_status"] == "WARN").sum()),
                        "quarantine_file": str(q_path) if q_path else None}
        print(f"[Validate] {key:<9} in={len(df):>5} passed={len(passed):>5} "
              f"quarantined={len(failed):>4} warnings={summary[key]['warnings']:>4}")
        if len(failed):
            print(failed["failure_reason"].value_counts().to_string())

    summary["orders"]["updated_by_increment"] = updated
    warehouse.write_dq_results(con, run_id, DAG_ID, dq)

    # Quarantined rows also go to a table, so finance can query them (audit question).
    con.execute("""
        CREATE TABLE IF NOT EXISTS etl_quarantine (
            run_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num INTEGER,
            row_key VARCHAR, failure_reason VARCHAR, raw_record JSON, quarantined_at TIMESTAMP)
    """)
    con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run_id])
    if quarantined_all:
        rows = []
        for q in quarantined_all:
            key_col = config.EXPECTED_COLUMNS[q["file"].iloc[0]][0]
            raw_cols = config.EXPECTED_COLUMNS[q["file"].iloc[0]]
            for _, r in q.iterrows():
                rows.append((run_id, r["file"], r["_source_file"], int(r["_row_num"]), r[key_col],
                             r["failure_reason"], r[raw_cols].to_json(), pd.Timestamp.now()))
        con.executemany("INSERT INTO etl_quarantine VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()
    return summary


# ---------------------------------------------------------------------- transform

def transform(run_id: str, run_ts: pd.Timestamp) -> dict:
    """Clean dimensions (deduped, PII masked) and the enriched order fact."""
    c = _load(run_id, "checked_customers")
    c = c[c["dq_status"] != "FAIL"].copy()
    signup = pd.to_datetime(c["signup_date"], format=quality.DATE_FORMAT, errors="coerce")
    dim_customer = pd.DataFrame({
        "customer_id": c["customer_id"],
        "customer_name_masked": c["full_name"].map(quality.mask_name),
        "email_masked": c["email"].map(quality.mask_email),
        "phone_masked": c["phone"].map(quality.mask_phone),
        "city": c["city"],
        "area": c["area"],
        "pincode": c["pincode"].where(c["pincode"].fillna("").str.match(quality.PINCODE_RE)),
        "signup_date": signup.where(signup <= run_ts.normalize()).dt.date,
        "loyalty_tier": c["loyalty_tier"],
        "is_active": c["is_active"] == "Y",
        "dq_status": c["dq_status"],
        "warning_reason": c["warning_reason"],
    })

    p = _load(run_id, "checked_products")
    p = p[p["dq_status"] != "FAIL"].copy()
    dim_product = pd.DataFrame({
        "product_id": p["product_id"],
        "product_name": p["product_name"],
        "category": p["category"].fillna("Unknown"),
        "sub_category": p["sub_category"],
        "brand": p["brand"],
        "mrp": quality._num(p["mrp"]),
        "cost_price": quality._num(p["cost_price"]),
        "unit": p["unit"],
        "is_perishable": p["is_perishable"].str.lower() == "true",
        "launch_date": pd.to_datetime(p["launch_date"], errors="coerce").dt.date,
        "dq_status": p["dq_status"],
        "warning_reason": p["warning_reason"],
    })

    s = _load(run_id, "checked_stores")
    s = s[s["dq_status"] != "FAIL"].copy()
    dim_store = pd.DataFrame({
        "store_id": s["store_id"],
        "store_name": s["store_name"],
        "zone": s["zone"].where(s["zone"].isin(config.VALID_ZONE), "Unknown"),
        "area": s["area"],
        "pincode": s["pincode"],
        "open_date": pd.to_datetime(s["open_date"], errors="coerce").dt.date,
        "manager_name": s["manager_name"],
        "capacity_orders_per_day": quality._num(s["capacity_orders_per_day"]).astype("Int64"),
        "dq_status": s["dq_status"],
        "warning_reason": s["warning_reason"],
    })

    o = _load(run_id, "checked_orders")
    o = o[o["dq_status"] != "FAIL"].copy()
    fact = pd.DataFrame({
        "order_id": o["order_id"],
        "order_ts": pd.to_datetime(o["order_ts"], format=quality.TS_FORMAT),
        "customer_id": o["customer_id"],
        "store_id": o["store_id"],
        "product_id": o["product_id"],
        "quantity": quality._num(o["quantity"]).astype(int),
        "unit_price": quality._num(o["unit_price"]),
        "discount_pct": quality._num(o["discount_pct"]),
        "order_amount": quality._num(o["order_amount"]),
        "payment_mode": o["payment_mode"],
        "order_status": o["order_status"],
        "channel": o["channel"],
        "delivery_minutes": quality._num(o["delivery_minutes"]).astype("Int64"),
    })
    fact["order_date"] = fact["order_ts"].dt.date
    fact = fact.merge(dim_product[["product_id", "category", "cost_price"]], on="product_id", how="left")
    fact = fact.merge(dim_store[["store_id", "zone"]], on="store_id", how="left")
    delivered = fact["order_status"] == "DELIVERED"
    fact["net_revenue"] = fact["order_amount"].where(delivered, 0.0).round(2)
    fact["cost_amount"] = (fact["quantity"] * fact["cost_price"]).where(delivered, 0.0).round(2)
    fact["gross_margin"] = (fact["net_revenue"] - fact["cost_amount"]).round(2)
    fact["within_15_min"] = delivered & (fact["delivery_minutes"] <= config.SLA_MINUTES).fillna(False)
    fact["dq_status"] = o["dq_status"].values
    fact["warning_reason"] = o["warning_reason"].values
    fact["_source_file"] = o["_source_file"].values
    fact["_source_row"] = o["_row_num"].values
    fact = fact.drop(columns=["cost_price"])

    # Orders that were valid before but whose latest version now fails must leave the fact.
    failed_orders = _load(run_id, "checked_orders")
    failed_ids = set(failed_orders.loc[(failed_orders["dq_status"] == "FAIL")
                                       & (failed_orders["_dup_rank"] == 1), "order_id"].dropna())
    failed_ids -= set(fact["order_id"])

    for name, df in {"dim_customer": dim_customer, "dim_product": dim_product,
                     "dim_store": dim_store, "fact_orders": fact}.items():
        _save(df, run_id, name)
    _save(pd.DataFrame({"order_id": sorted(failed_ids)}, dtype="string"), run_id, "fact_remove")
    print(f"[Transform] fact rows={len(fact)} customers={len(dim_customer)} "
          f"products={len(dim_product)} stores={len(dim_store)}")
    return {"fact_rows": len(fact), "dim_customer": len(dim_customer),
            "dim_product": len(dim_product), "dim_store": len(dim_store)}


# --------------------------------------------------------------------------- load

FACT_DDL = """
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, customer_id VARCHAR, store_id VARCHAR,
    product_id VARCHAR, quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE,
    order_amount DOUBLE, payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
    delivery_minutes INTEGER, order_date DATE, category VARCHAR, zone VARCHAR,
    net_revenue DOUBLE, cost_amount DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN,
    dq_status VARCHAR, warning_reason VARCHAR, _source_file VARCHAR, _source_row INTEGER,
    _run_id VARCHAR, _loaded_at TIMESTAMP
)
"""


def load(run_id: str) -> dict:
    """
    Idempotent load. Dimensions are replaced in full; the fact is upserted on order_id
    (INSERT OR REPLACE), so running twice, or re-reading the same file, never duplicates.
    """
    con = config.connect()
    con.execute("BEGIN TRANSACTION")
    for name, key in [("dim_customer", "customer_id"), ("dim_product", "product_id"),
                      ("dim_store", "store_id")]:
        df = _load(run_id, name)
        con.register("src", df)
        con.execute(f"CREATE TABLE IF NOT EXISTS etl_{name} AS SELECT * FROM src WHERE false")
        con.execute(f"DELETE FROM etl_{name}")
        con.execute(f"INSERT INTO etl_{name} BY NAME SELECT * FROM src")
        con.unregister("src")

    con.execute(FACT_DDL)
    fact = _load(run_id, "fact_orders")
    before = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    con.register("src", fact)
    updated = con.execute(
        "SELECT count(*) FROM etl_fact_orders f JOIN src USING (order_id)"
    ).fetchone()[0]
    con.execute(f"""
        INSERT OR REPLACE INTO etl_fact_orders BY NAME
        SELECT *, '{run_id}' AS _run_id, now()::TIMESTAMP AS _loaded_at FROM src
    """)
    con.unregister("src")
    remove = _load(run_id, "fact_remove")
    con.register("rm", remove)
    removed = con.execute(
        "DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM rm)"
    ).fetchone()[0]
    con.execute("COMMIT")
    after = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    con.close()
    result = {"fact_rows_before": before, "fact_rows_after": after,
              "inserted": after - before + removed, "upserted_existing": updated,
              "removed_now_failing": removed}
    print(f"[Load] {result}")
    return result


# ---------------------------------------------------------------------- reconcile

def reconcile(run_id: str) -> int:
    """Compare clean orders with every (de-duplicated) payment; one row per mismatch."""
    checked = _load(run_id, "checked_payments")
    payments = checked[(checked["_dup_rank"] == 1) & checked["payment_id"].notna()]
    payments = pd.DataFrame({
        "payment_id": payments["payment_id"],
        "order_id": payments["order_id"],
        "payment_mode": payments["payment_mode"],
        "amount_paid": quality._num(payments["amount_paid"]),
        "payment_status": payments["payment_status"],
        "payment_dq_status": payments["dq_status"],
    })
    con = config.connect()
    con.register("payments_src", payments)
    sql = (config.INCLUDE_DIR / "sql" / "reconciliation.sql").read_text()
    sql = sql.format(orders="etl_fact_orders", payments="payments_src",
                     target="etl_payment_reconciliation")
    con.execute(sql)
    n = con.execute("SELECT count(*) FROM etl_payment_reconciliation").fetchone()[0]
    print(con.execute("""
        SELECT mismatch_type, count(*) AS rows, round(sum(abs(amount_difference)), 2) AS amount
        FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC
    """).df().to_string(index=False))
    con.close()
    return n


# ------------------------------------------------------------------------- report

def report(run_id: str) -> dict:
    con = config.connect()
    con.execute("""
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, f.zone, f.store_id, s.store_name,
               count(*) AS delivered_orders,
               round(sum(f.net_revenue), 2) AS net_revenue,
               round(sum(f.gross_margin), 2) AS gross_margin
        FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL ORDER BY f.order_date, f.zone, f.store_id
    """)
    con.execute("""
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT category,
               count(*) AS delivered_orders,
               sum(quantity) AS units,
               round(sum(net_revenue), 2) AS net_revenue,
               round(sum(cost_amount), 2) AS cost,
               round(sum(gross_margin), 2) AS gross_margin,
               round(100 * sum(gross_margin) / nullif(sum(net_revenue), 0), 2) AS margin_pct
        FROM etl_fact_orders WHERE order_status = 'DELIVERED'
        GROUP BY 1 ORDER BY net_revenue DESC
    """)
    print("[Report] Net revenue by zone (all days):")
    print(con.execute("""
        SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
        FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 3 DESC
    """).df().to_string(index=False))
    print("[Report] Category margin:")
    print(con.execute("SELECT * FROM etl_category_margin").df().to_string(index=False))
    totals = con.execute("""
        SELECT count(*), round(sum(net_revenue), 2), round(sum(gross_margin), 2)
        FROM etl_fact_orders WHERE order_status = 'DELIVERED'
    """).fetchone()
    con.close()
    return {"delivered_orders": totals[0], "net_revenue": totals[1], "gross_margin": totals[2]}
