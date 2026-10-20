-- include/sql/elt/mart/mart_delivery_sla.sql
-- Business question 3: average delivery time per store and % delivered within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id, store_name, zone,
    count(*)                                            AS delivered_orders,
    round(avg(delivery_minutes), 1)                     AS avg_delivery_minutes,
    count(*) FILTER (WHERE within_15_min)               AS delivered_within_15_min,
    round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 1) AS pct_within_15_min
FROM int_orders_clean
WHERE order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY store_id;
