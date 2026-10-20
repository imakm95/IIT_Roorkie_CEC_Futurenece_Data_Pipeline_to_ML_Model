-- include/sql/elt/mart/00_int_orders_clean.sql
-- Passed/warned orders joined to the current store and product rows. Every mart reads this.
CREATE OR REPLACE VIEW int_orders_clean AS
SELECT
    o.order_id, o.order_ts, o.order_ts::DATE AS order_date,
    o.customer_id, o.store_id, s.store_name, s.zone,
    o.product_id, p.category,
    o.quantity::INTEGER AS quantity, o.unit_price, o.discount_pct, o.order_amount,
    o.payment_mode, o.order_status, o.channel, o.delivery_minutes::INTEGER AS delivery_minutes,
    CASE WHEN o.order_status = 'DELIVERED' THEN round(o.order_amount, 2) ELSE 0 END AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN round(o.quantity * p.cost_price, 2) ELSE 0 END AS cost_amount,
    round(
        CASE WHEN o.order_status = 'DELIVERED' THEN round(o.order_amount, 2) ELSE 0 END
      - CASE WHEN o.order_status = 'DELIVERED' THEN round(o.quantity * p.cost_price, 2) ELSE 0 END, 2
    ) AS gross_margin,
    o.order_status = 'DELIVERED' AND coalesce(o.delivery_minutes <= 15, false) AS within_15_min,
    o.dq_status, o.warning_reason, o._source_file, o._row_num, o._run_id
FROM stg_orders o
LEFT JOIN stg_products p ON p.product_id = o.product_id AND p._dup_rank = 1
LEFT JOIN stg_stores s ON s.store_id = o.store_id AND s._dup_rank = 1
WHERE o.dq_status <> 'FAIL';
