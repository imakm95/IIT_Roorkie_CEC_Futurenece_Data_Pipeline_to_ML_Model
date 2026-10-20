-- include/sql/elt/staging/stg_orders_dq.sql
-- Re-evaluate every order quality rule after the merge (masters may have changed too).
-- Variable: run_ts (run time in IST).
CREATE OR REPLACE TABLE stg_orders AS
WITH cur_products AS (
    SELECT product_id, mrp FROM stg_products WHERE _dup_rank = 1 AND product_id IS NOT NULL
),
cur_stores AS (
    SELECT store_id, capacity_orders_per_day FROM stg_stores WHERE _dup_rank = 1 AND store_id IS NOT NULL
),
base AS (
    SELECT o.* EXCLUDE (dq_codes, failure_reason, warning_reason, dq_status),
           p.mrp, s.capacity_orders_per_day,
           count(*) OVER (PARTITION BY o.store_id, o.order_ts::DATE) AS orders_that_day
    FROM stg_orders o
    LEFT JOIN cur_products p USING (product_id)
    LEFT JOIN cur_stores s USING (store_id)
),
flagged AS (
    SELECT *, list_filter([
        CASE WHEN order_id IS NULL THEN 'ord_order_id_not_null' END,
        CASE WHEN customer_id IS NULL THEN 'ord_customer_id_not_null' END,
        CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'ord_delivered_has_minutes' END,
        CASE WHEN _dup_rank > 1 THEN 'ord_order_id_unique' END,
        CASE WHEN order_ts IS NULL THEN 'ord_ts_parses' END,
        CASE WHEN NOT coalesce(order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED'), false) THEN 'ord_status_valid' END,
        CASE WHEN NOT coalesce(payment_mode IN (SELECT standard FROM ref_payment_mode_alias), false) THEN 'ord_payment_mode_valid' END,
        CASE WHEN NOT coalesce(discount_pct BETWEEN 0 AND 100, false) THEN 'ord_discount_range' END,
        CASE WHEN NOT coalesce(quantity > 0, false) THEN 'ord_quantity_positive' END,
        CASE WHEN NOT coalesce(abs(order_amount - quantity * unit_price * (1 - discount_pct / 100)) <= 0.0100001, false) THEN 'ord_amount_formula' END,
        CASE WHEN delivery_minutes > 120 OR delivery_minutes < 0 THEN 'ord_delivery_minutes_max' END,
        CASE WHEN customer_id IS NOT NULL AND customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE customer_id IS NOT NULL) THEN 'ord_customer_fk' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM stg_stores x WHERE x.store_id = base.store_id) THEN 'ord_store_fk' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM stg_products x WHERE x.product_id = base.product_id) THEN 'ord_product_fk' END,
        CASE WHEN order_ts > getvariable('run_ts') THEN 'ord_ts_not_future' END,
        CASE WHEN order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND order_status_raw <> order_status THEN 'ord_status_casing' END,
        CASE WHEN payment_mode IN (SELECT standard FROM ref_payment_mode_alias) AND payment_mode_raw <> payment_mode THEN 'ord_payment_mode_alias' END,
        CASE WHEN delivery_minutes BETWEEN 0 AND 120 AND NOT delivery_minutes BETWEEN 8 AND 35 THEN 'ord_delivery_minutes_unusual' END,
        CASE WHEN mrp > 0 AND (unit_price / mrp > 1.5 OR unit_price / mrp < 1 / 1.5) THEN 'ord_price_vs_mrp' END,
        CASE WHEN capacity_orders_per_day > 0 AND orders_that_day > capacity_orders_per_day THEN 'ord_store_capacity' END
    ], x -> x IS NOT NULL) AS dq_codes
    FROM base
)
SELECT * EXCLUDE (mrp, capacity_orders_per_day, orders_that_day),
       dq_reason(dq_codes, 'Critical') AS failure_reason,
       dq_reason(dq_codes, 'Warning')  AS warning_reason,
       dq_status_of(dq_codes)          AS dq_status
FROM flagged;

-- CREATE OR REPLACE drops the primary key, which the next MERGE relies on; restore it.
ALTER TABLE stg_orders ADD PRIMARY KEY (stg_key);
