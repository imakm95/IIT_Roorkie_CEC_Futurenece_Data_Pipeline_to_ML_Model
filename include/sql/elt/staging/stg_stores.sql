-- include/sql/elt/staging/stg_stores.sql
-- Current store master, typed and checked. Rebuilt from raw every run.
CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY std_text(store_id) ORDER BY _row_num) AS dup_n
    FROM raw_stores
    WHERE _source_file = 'stores.csv' AND _run_id = current_run('stores.csv')
),
typed AS (
    SELECT
        std_text(store_id)                         AS store_id,
        std_text(store_name)                       AS store_name,
        title_word(zone)                           AS zone_std,
        std_text(area)                             AS area,
        std_text(pincode)                          AS pincode,
        try_strptime(std_text(open_date), '%Y-%m-%d')::DATE AS open_date,
        std_text(manager_name)                     AS manager_name,
        try_cast(std_text(capacity_orders_per_day) AS DOUBLE) AS capacity_orders_per_day,
        CASE WHEN std_text(store_id) IS NULL THEN 1 ELSE dup_n END AS _dup_rank,
        _source_file, _run_id, _row_num
    FROM src
),
flagged AS (
    SELECT *, list_filter([
        CASE WHEN store_id IS NULL THEN 'sto_store_id_not_null' END,
        CASE WHEN _dup_rank > 1 THEN 'sto_store_id_unique' END,
        CASE WHEN NOT coalesce(zone_std IN ('North', 'South', 'East', 'West'), false) THEN 'sto_zone_valid' END,
        CASE WHEN NOT coalesce(capacity_orders_per_day > 0, false) THEN 'sto_capacity_positive' END
    ], x -> x IS NOT NULL) AS dq_codes
    FROM typed
)
SELECT
    store_id, store_name,
    CASE WHEN zone_std IN ('North', 'South', 'East', 'West') THEN zone_std ELSE 'Unknown' END AS zone,
    area, pincode, open_date, manager_name, capacity_orders_per_day::INTEGER AS capacity_orders_per_day,
    _dup_rank, _source_file, _run_id, _row_num, dq_codes,
    dq_reason(dq_codes, 'Critical') AS failure_reason,
    dq_reason(dq_codes, 'Warning')  AS warning_reason,
    dq_status_of(dq_codes)          AS dq_status
FROM flagged;
