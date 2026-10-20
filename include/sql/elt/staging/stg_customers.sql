-- include/sql/elt/staging/stg_customers.sql
-- Current customer master, typed, checked and PII-masked. The unmasked name, email and
-- phone stay only in raw_customers (restricted access). Rebuilt from raw every run.
CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY std_text(customer_id) ORDER BY _row_num) AS dup_n
    FROM raw_customers
    WHERE _source_file = 'customers.csv' AND _run_id = current_run('customers.csv')
),
typed AS (
    SELECT
        std_text(customer_id)                      AS customer_id,
        full_name, email, phone,
        std_text(city)                             AS city,
        std_text(area)                             AS area,
        std_text(pincode)                          AS pincode,
        try_strptime(std_text(signup_date), '%Y-%m-%d')::DATE AS signup_date,
        loyalty_tier                               AS loyalty_tier_raw,
        title_word(loyalty_tier)                   AS loyalty_tier,
        std_text(is_active) = 'Y'                  AS is_active,
        CASE WHEN std_text(customer_id) IS NULL THEN 1 ELSE dup_n END AS _dup_rank,
        _source_file, _run_id, _row_num
    FROM src
),
flagged AS (
    SELECT *, list_filter([
        CASE WHEN customer_id IS NULL THEN 'cus_customer_id_not_null' END,
        CASE WHEN _dup_rank > 1 THEN 'cus_customer_id_unique' END,
        CASE WHEN NOT regexp_full_match(coalesce(std_text(email), ''), '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') THEN 'cus_email_format' END,
        CASE WHEN NOT regexp_full_match(coalesce(std_text(phone), ''), '[6-9][0-9]{9}') THEN 'cus_phone_10_digits' END,
        CASE WHEN NOT regexp_full_match(coalesce(pincode, ''), '560[0-9]{3}') THEN 'cus_pincode_6_digits' END,
        CASE WHEN city IS NULL THEN 'cus_city_present' END,
        CASE WHEN loyalty_tier IS NOT NULL AND loyalty_tier_raw <> loyalty_tier THEN 'cus_tier_casing' END,
        CASE WHEN signup_date > getvariable('run_ts')::DATE THEN 'cus_signup_not_future' END
    ], x -> x IS NOT NULL) AS dq_codes
    FROM typed
)
SELECT
    customer_id,
    mask_name(full_name)  AS customer_name_masked,
    mask_email(email)     AS email_masked,
    mask_phone(phone)     AS phone_masked,
    city, area,
    CASE WHEN regexp_full_match(coalesce(pincode, ''), '560[0-9]{3}') THEN pincode END AS pincode,
    CASE WHEN signup_date <= getvariable('run_ts')::DATE THEN signup_date END AS signup_date,
    loyalty_tier, is_active,
    _dup_rank, _source_file, _run_id, _row_num, dq_codes,
    dq_reason(dq_codes, 'Critical') AS failure_reason,
    dq_reason(dq_codes, 'Warning')  AS warning_reason,
    dq_status_of(dq_codes)          AS dq_status
FROM flagged;
