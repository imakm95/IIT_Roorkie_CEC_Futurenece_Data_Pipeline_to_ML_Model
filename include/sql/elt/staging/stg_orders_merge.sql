-- include/sql/elt/staging/stg_orders_merge.sql
-- Upsert one order file version into stg_orders.
-- Variables: src_file (e.g. orders_2026-10-01_increment.csv), src_run (run that holds its raw rows).
-- Matched rows (same order_id and copy number) are overwritten, so a status correction such as
-- DELIVERED -> RETURNED replaces the old row; unmatched rows are inserted as new orders.
MERGE INTO stg_orders AS t
USING (
    SELECT
        coalesce(std_text(order_id), 'NULL:' || _source_file || ':' || _row_num) || '#' || dup_n AS stg_key,
        std_text(order_id)                                       AS order_id,
        try_strptime(std_text(order_ts), '%Y-%m-%d %H:%M:%S')    AS order_ts,
        order_ts                                                 AS order_ts_raw,
        std_text(customer_id)                                    AS customer_id,
        std_text(store_id)                                       AS store_id,
        std_text(product_id)                                     AS product_id,
        try_cast(std_text(quantity) AS DOUBLE)                   AS quantity,
        try_cast(std_text(unit_price) AS DOUBLE)                 AS unit_price,
        try_cast(std_text(discount_pct) AS DOUBLE)               AS discount_pct,
        try_cast(std_text(order_amount) AS DOUBLE)               AS order_amount,
        std_payment_mode(payment_mode)                           AS payment_mode,
        payment_mode                                             AS payment_mode_raw,
        upper(std_text(order_status))                            AS order_status,
        order_status                                             AS order_status_raw,
        std_text(channel)                                        AS channel,
        try_cast(std_text(delivery_minutes) AS DOUBLE)           AS delivery_minutes,
        CASE WHEN std_text(order_id) IS NULL THEN 1 ELSE dup_n END AS _dup_rank,
        _source_file, _run_id, _row_num
    FROM (
        SELECT *, row_number() OVER (PARTITION BY std_text(order_id) ORDER BY _row_num) AS dup_n
        FROM raw_orders
        WHERE _source_file = getvariable('src_file') AND _run_id = getvariable('src_run')
    )
) AS s
ON t.stg_key = s.stg_key
WHEN MATCHED THEN UPDATE SET
    order_id = s.order_id, order_ts = s.order_ts, order_ts_raw = s.order_ts_raw,
    customer_id = s.customer_id, store_id = s.store_id, product_id = s.product_id,
    quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct,
    order_amount = s.order_amount, payment_mode = s.payment_mode, payment_mode_raw = s.payment_mode_raw,
    order_status = s.order_status, order_status_raw = s.order_status_raw, channel = s.channel,
    delivery_minutes = s.delivery_minutes, _dup_rank = s._dup_rank, _source_file = s._source_file,
    _run_id = s._run_id, _row_num = s._row_num, _merged_at = now()::TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    stg_key, order_id, order_ts, order_ts_raw, customer_id, store_id, product_id, quantity,
    unit_price, discount_pct, order_amount, payment_mode, payment_mode_raw, order_status,
    order_status_raw, channel, delivery_minutes, _dup_rank, _source_file, _run_id, _row_num, _merged_at
) VALUES (
    s.stg_key, s.order_id, s.order_ts, s.order_ts_raw, s.customer_id, s.store_id, s.product_id, s.quantity,
    s.unit_price, s.discount_pct, s.order_amount, s.payment_mode, s.payment_mode_raw, s.order_status,
    s.order_status_raw, s.channel, s.delivery_minutes, s._dup_rank, s._source_file, s._run_id, s._row_num,
    now()::TIMESTAMP
);
