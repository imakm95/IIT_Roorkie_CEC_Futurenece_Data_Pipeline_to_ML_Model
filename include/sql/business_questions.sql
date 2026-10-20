-- include/sql/business_questions.sql
-- The six client questions, each answered with one query per pipeline.
-- Run in VS Code SQLTools (or `duckdb include/warehouse_ashwani.duckdb`) after both DAGs succeed.

-- Q1. Daily net revenue by zone and by store (delivered orders only, after discount)
-- ETL
SELECT order_date, zone, store_id, store_name, net_revenue
FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id;
-- ELT
SELECT order_date, zone, store_id, store_name, net_revenue
FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id;
-- Zone roll-up, both pipelines side by side
SELECT e.zone, e.net_revenue AS etl_net_revenue, l.net_revenue AS elt_net_revenue
FROM (SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY 1) e
JOIN (SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY 1) l USING (zone)
ORDER BY 2 DESC;

-- Q2. Revenue and gross margin by product category
-- ETL
SELECT category, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC;
-- ELT
SELECT category, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
-- ETL
SELECT store_id, round(avg(delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 1) AS pct_within_15_min
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1;
-- ELT
SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1;

-- Q4. Revenue by payment mode, and total amount that doesn't reconcile with the gateway
-- ETL
SELECT payment_mode, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 2 DESC;
SELECT mismatch_type, count(*) AS rows, round(sum(abs(amount_difference)), 2) AS unreconciled_amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (1) ORDER BY 1 NULLS LAST;
-- ELT
SELECT payment_mode, net_revenue FROM mart_payment_mode_revenue ORDER BY 2 DESC;
SELECT mismatch_type, count(*) AS rows, round(sum(abs(amount_difference)), 2) AS unreconciled_amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (1) ORDER BY 1 NULLS LAST;

-- Q5. Top 10 customers by spend, with loyalty tier (no raw email or phone)
-- ETL
SELECT f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC, f.customer_id LIMIT 10;
-- ELT
SELECT customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier, total_spend
FROM mart_top_customers ORDER BY spend_rank, customer_id LIMIT 10;

-- Q6. Number of rows rejected today, by file and by reason (latest run of each DAG)
SELECT d.dag_id, split_part(d.check_name, ':', 1) AS file, d.check_name AS reason, d.rows_failed
FROM dq_results d
WHERE d.severity = 'Critical' AND d.rows_failed > 0
  AND d.run_id = (SELECT run_id FROM pipeline_runs p WHERE p.dag_id = d.dag_id ORDER BY end_time DESC LIMIT 1)
ORDER BY d.dag_id, file, d.rows_failed DESC;
-- ETL detail: every rejected row with its reason
SELECT file, count(*) AS rows_rejected FROM etl_quarantine
WHERE run_id = (SELECT max(run_id) FROM etl_quarantine) GROUP BY 1 ORDER BY 1;
-- ELT detail: every staging row marked FAIL
SELECT 'orders' AS file, failure_reason, count(*) FROM stg_orders WHERE dq_status = 'FAIL' GROUP BY ALL
UNION ALL SELECT 'payments', failure_reason, count(*) FROM stg_payments WHERE dq_status = 'FAIL' GROUP BY ALL
UNION ALL SELECT 'customers', failure_reason, count(*) FROM stg_customers WHERE dq_status = 'FAIL' GROUP BY ALL
UNION ALL SELECT 'products', failure_reason, count(*) FROM stg_products WHERE dq_status = 'FAIL' GROUP BY ALL
UNION ALL SELECT 'stores', failure_reason, count(*) FROM stg_stores WHERE dq_status = 'FAIL' GROUP BY ALL
ORDER BY 1, 3 DESC;

-- Scorecards used for the screenshots
SELECT * FROM pipeline_runs ORDER BY end_time DESC;
SELECT dag_id, dimension, severity, count(*) AS checks, sum(rows_failed) AS rows_failed
FROM dq_results GROUP BY ALL ORDER BY dag_id, dimension;
