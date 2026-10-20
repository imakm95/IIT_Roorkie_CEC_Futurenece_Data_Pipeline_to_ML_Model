-- include/sql/elt/mart/mart_category_margin.sql
-- Business question 2: revenue and gross margin by product category (delivered orders).
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    count(*)                                   AS delivered_orders,
    sum(quantity)                              AS units,
    round(sum(net_revenue), 2)                 AS net_revenue,
    round(sum(cost_amount), 2)                 AS cost,
    round(sum(gross_margin), 2)                AS gross_margin,
    round(100 * sum(gross_margin) / nullif(sum(net_revenue), 0), 2) AS margin_pct
FROM int_orders_clean
WHERE order_status = 'DELIVERED'
GROUP BY 1
ORDER BY net_revenue DESC;
