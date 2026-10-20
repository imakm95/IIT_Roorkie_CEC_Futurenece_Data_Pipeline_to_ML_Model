-- include/sql/elt/staging/stg_orders_create.sql
-- stg_orders is persistent: each run MERGEs the order files it received into it.
-- stg_key = order_id + copy number, so an exact duplicate inside one file stays a
-- separate (failing) row instead of overwriting the first copy.
CREATE TABLE IF NOT EXISTS stg_orders (
    stg_key VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    order_ts TIMESTAMP,
    order_ts_raw VARCHAR,
    customer_id VARCHAR,
    store_id VARCHAR,
    product_id VARCHAR,
    quantity DOUBLE,
    unit_price DOUBLE,
    discount_pct DOUBLE,
    order_amount DOUBLE,
    payment_mode VARCHAR,
    payment_mode_raw VARCHAR,
    order_status VARCHAR,
    order_status_raw VARCHAR,
    channel VARCHAR,
    delivery_minutes DOUBLE,
    _dup_rank INTEGER,
    _source_file VARCHAR,
    _run_id VARCHAR,
    _row_num INTEGER,
    _merged_at TIMESTAMP,
    dq_codes VARCHAR[],
    failure_reason VARCHAR,
    warning_reason VARCHAR,
    dq_status VARCHAR
);
