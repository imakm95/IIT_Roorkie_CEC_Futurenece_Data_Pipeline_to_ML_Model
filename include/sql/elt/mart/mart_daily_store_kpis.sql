-- include/sql/elt/mart/mart_daily_store_kpis.sql
-- Business question 1 (and the daily ops view): one row per day per store.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date, zone, store_id, store_name,
    count(*)                                                    AS total_orders,
    count(*) FILTER (WHERE order_status = 'DELIVERED')          AS delivered_orders,
    count(*) FILTER (WHERE order_status = 'CANCELLED')          AS cancelled_orders,
    count(*) FILTER (WHERE order_status = 'RETURNED')           AS returned_orders,
    round(sum(net_revenue), 2)                                  AS net_revenue,
    round(sum(gross_margin), 2)                                 AS gross_margin,
    round(avg(delivery_minutes) FILTER (WHERE order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
    round(100.0 * count(*) FILTER (WHERE within_15_min)
          / nullif(count(*) FILTER (WHERE order_status = 'DELIVERED'), 0), 1) AS pct_within_15_min
FROM int_orders_clean
GROUP BY ALL
ORDER BY order_date, zone, store_id;
