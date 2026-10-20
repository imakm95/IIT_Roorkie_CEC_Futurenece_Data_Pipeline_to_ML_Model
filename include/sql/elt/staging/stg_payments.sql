-- include/sql/elt/staging/stg_payments.sql
-- Current payment gateway export, typed and checked against stg_orders. Rebuilt every run.
CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY std_text(payment_id) ORDER BY _row_num) AS dup_n
    FROM raw_payments
    WHERE _source_file = 'payments.csv' AND _run_id = current_run('payments.csv')
),
typed AS (
    SELECT
        std_text(payment_id)                                    AS payment_id,
        std_text(order_id)                                      AS order_id,
        try_strptime(std_text(payment_ts), '%Y-%m-%d %H:%M:%S') AS payment_ts,
        std_payment_mode(payment_mode)                          AS payment_mode,
        payment_mode                                            AS payment_mode_raw,
        try_cast(std_text(amount_paid) AS DOUBLE)               AS amount_paid,
        std_text(payment_status)                                AS payment_status,
        std_text(gateway_ref)                                   AS gateway_ref,
        CASE WHEN std_text(payment_id) IS NULL THEN 1 ELSE dup_n END AS _dup_rank,
        _source_file, _run_id, _row_num
    FROM src
),
first_order_copy AS (
    SELECT order_id, order_amount FROM stg_orders WHERE _dup_rank = 1 AND order_id IS NOT NULL
),
flagged AS (
    SELECT typed.*, list_filter([
        CASE WHEN payment_id IS NULL THEN 'pay_payment_id_not_null' END,
        CASE WHEN _dup_rank > 1 THEN 'pay_payment_id_unique' END,
        CASE WHEN payment_ts IS NULL THEN 'pay_ts_parses' END,
        CASE WHEN NOT coalesce(payment_mode IN (SELECT standard FROM ref_payment_mode_alias), false) THEN 'pay_mode_valid' END,
        CASE WHEN NOT coalesce(payment_status IN ('SUCCESS', 'FAILED', 'REFUNDED'), false) THEN 'pay_status_valid' END,
        CASE WHEN NOT coalesce(amount_paid >= 0, false) THEN 'pay_amount_non_negative' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM stg_orders x WHERE x.order_id = typed.order_id) THEN 'pay_order_fk' END,
        CASE WHEN payment_ts > getvariable('run_ts') THEN 'pay_ts_not_future' END,
        CASE WHEN payment_mode IN (SELECT standard FROM ref_payment_mode_alias) AND payment_mode_raw <> payment_mode THEN 'pay_mode_alias' END,
        CASE WHEN coalesce(payment_mode <> 'Cash on Delivery', true) AND gateway_ref IS NULL THEN 'pay_gateway_ref_missing' END,
        CASE WHEN payment_status = 'SUCCESS' AND abs(amount_paid - o.order_amount) > 0.01 THEN 'pay_amount_matches_order' END
    ], x -> x IS NOT NULL) AS dq_codes
    FROM typed LEFT JOIN first_order_copy o USING (order_id)
)
SELECT *,
       dq_reason(dq_codes, 'Critical') AS failure_reason,
       dq_reason(dq_codes, 'Warning')  AS warning_reason,
       dq_status_of(dq_codes)          AS dq_status
FROM flagged;
