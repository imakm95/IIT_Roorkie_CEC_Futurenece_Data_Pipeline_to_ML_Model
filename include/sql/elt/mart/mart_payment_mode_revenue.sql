-- include/sql/elt/mart/mart_payment_mode_revenue.sql
-- Business question 4 (first half): net revenue by payment mode.
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT
    payment_mode,
    count(*)                    AS delivered_orders,
    round(sum(net_revenue), 2)  AS net_revenue
FROM int_orders_clean
WHERE order_status = 'DELIVERED'
GROUP BY 1
ORDER BY net_revenue DESC;
