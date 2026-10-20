-- include/sql/elt/staging/stg_products.sql
-- Current product catalogue, typed and checked. For a duplicated product_id the row
-- named "(old)" loses. Rebuilt from raw every run.
CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY std_text(product_id)
            ORDER BY CASE WHEN lower(coalesce(std_text(product_name), '')) LIKE '%(old)' THEN 1 ELSE 0 END, _row_num
        ) AS dup_n
    FROM raw_products
    WHERE _source_file = 'products.csv' AND _run_id = current_run('products.csv')
),
typed AS (
    SELECT
        std_text(product_id)                       AS product_id,
        std_text(product_name)                     AS product_name,
        std_text(category)                         AS category_raw,
        std_text(sub_category)                     AS sub_category,
        std_text(brand)                            AS brand,
        try_cast(std_text(mrp) AS DOUBLE)          AS mrp,
        try_cast(std_text(cost_price) AS DOUBLE)   AS cost_price,
        std_text(unit)                             AS unit,
        lower(std_text(is_perishable)) = 'true'    AS is_perishable,
        try_strptime(std_text(launch_date), '%Y-%m-%d')::DATE AS launch_date,
        CASE WHEN std_text(product_id) IS NULL THEN 1 ELSE dup_n END AS _dup_rank,
        _source_file, _run_id, _row_num
    FROM src
),
flagged AS (
    SELECT *, list_filter([
        CASE WHEN product_id IS NULL THEN 'prd_product_id_not_null' END,
        CASE WHEN _dup_rank > 1 THEN 'prd_product_id_unique' END,
        CASE WHEN category_raw IS NULL THEN 'prd_category_present' END,
        CASE WHEN NOT coalesce(mrp > 0, false) THEN 'prd_mrp_positive' END,
        CASE WHEN coalesce(cost_price > mrp, false) THEN 'prd_cost_le_mrp' END
    ], x -> x IS NOT NULL) AS dq_codes
    FROM typed
)
SELECT
    product_id, product_name, coalesce(category_raw, 'Unknown') AS category, sub_category, brand,
    mrp, cost_price, unit, is_perishable, launch_date,
    _dup_rank, _source_file, _run_id, _row_num, dq_codes,
    dq_reason(dq_codes, 'Critical') AS failure_reason,
    dq_reason(dq_codes, 'Warning')  AS warning_reason,
    dq_status_of(dq_codes)          AS dq_status
FROM flagged;
