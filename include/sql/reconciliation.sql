-- include/sql/reconciliation.sql
-- Order vs payment reconciliation, shared by both pipelines.
--   ETL: {orders} = etl_fact_orders,  {payments} = de-duplicated payments.csv rows
--   ELT: {orders} = stg_orders (passed rows), {payments} = stg_payments (first copies)
-- One output row per mismatch. amount_difference = what the gateway says minus what
-- the order system says (positive: we hold more money than the order explains).

CREATE OR REPLACE TABLE {target} AS
WITH o AS (
    SELECT order_id, order_status, order_amount, payment_mode FROM {orders}
),
p AS (
    SELECT payment_id, order_id, payment_mode, amount_paid, payment_status, payment_dq_status
    FROM {payments}
),
pairs AS (
    SELECT coalesce(o.order_id, p.order_id) AS order_id, p.payment_id,
           o.order_status, p.payment_status, o.order_amount, p.amount_paid,
           o.payment_mode AS order_payment_mode, p.payment_mode AS gateway_payment_mode,
           p.payment_dq_status,
           o.order_id IS NOT NULL AS has_order, p.payment_id IS NOT NULL AS has_payment
    FROM o FULL OUTER JOIN p ON o.order_id = p.order_id
),
mismatches AS (
    SELECT *, 'PAYMENT_WITHOUT_VALID_ORDER' AS mismatch_type, amount_paid AS amount_difference,
           'Payment has no order in the clean order table (order missing or rejected)' AS detail
    FROM pairs WHERE has_payment AND NOT has_order
    UNION ALL
    SELECT *, 'ORDER_WITHOUT_PAYMENT', -order_amount,
           'Delivered/returned order has no payment record'
    FROM pairs WHERE has_order AND NOT has_payment AND order_status IN ('DELIVERED', 'RETURNED')
    UNION ALL
    SELECT *, 'PAYMENT_FAILED_FOR_DELIVERED_ORDER', -order_amount,
           'Order delivered but the payment failed'
    FROM pairs WHERE order_status = 'DELIVERED' AND payment_status = 'FAILED'
    UNION ALL
    SELECT *, 'AMOUNT_MISMATCH', round(amount_paid - order_amount, 2),
           'SUCCESS payment amount differs from order_amount'
    FROM pairs WHERE has_order AND payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01
    UNION ALL
    SELECT *, 'RETURNED_NOT_REFUNDED', amount_paid,
           'Order returned but the payment is still SUCCESS (no refund)'
    FROM pairs WHERE order_status = 'RETURNED' AND payment_status = 'SUCCESS'
    UNION ALL
    SELECT *, 'REFUNDED_BUT_DELIVERED', -amount_paid,
           'Payment refunded but the order is still DELIVERED'
    FROM pairs WHERE order_status = 'DELIVERED' AND payment_status = 'REFUNDED'
    UNION ALL
    SELECT *, 'PAYMENT_ON_CANCELLED_ORDER', amount_paid,
           'Cancelled order has a payment'
    FROM pairs WHERE order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
    UNION ALL
    SELECT *, 'PAYMENT_MODE_MISMATCH', 0,
           'Order and gateway disagree on payment mode'
    FROM pairs WHERE has_order AND has_payment AND order_payment_mode <> gateway_payment_mode
)
SELECT order_id, payment_id, mismatch_type, order_status, payment_status,
       order_amount, amount_paid, amount_difference, payment_dq_status, detail,
       now()::TIMESTAMP AS reconciled_at
FROM mismatches
ORDER BY mismatch_type, order_id;
