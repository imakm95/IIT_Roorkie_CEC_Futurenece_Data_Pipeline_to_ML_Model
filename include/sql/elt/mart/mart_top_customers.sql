-- include/sql/elt/mart/mart_top_customers.sql
-- Business question 5: customers ranked by delivered spend. Only masked PII is exposed.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    rank() OVER (ORDER BY sum(o.net_revenue) DESC) AS spend_rank,
    o.customer_id,
    c.customer_name_masked,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    count(*)                    AS delivered_orders,
    round(sum(o.net_revenue), 2) AS total_spend
FROM int_orders_clean o
LEFT JOIN stg_customers c ON c.customer_id = o.customer_id AND c._dup_rank = 1
WHERE o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY spend_rank, o.customer_id;
