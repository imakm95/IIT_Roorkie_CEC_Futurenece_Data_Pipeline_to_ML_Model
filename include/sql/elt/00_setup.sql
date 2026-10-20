-- include/sql/elt/00_setup.sql
-- Helper macros used by the staging SQL. Run once at the start of every ELT run.

-- Version of a source file this run should use: the rows held under the run that first
-- loaded the file's current content (raw_load_log points to it even when a re-run skipped
-- loading an identical file).
CREATE OR REPLACE MACRO current_run(f) AS (
    SELECT data_run_id FROM raw_load_log
    WHERE source_file = f ORDER BY logged_at DESC LIMIT 1
);

-- Turn a list of failed check codes into readable reasons, in catalog order.
CREATE OR REPLACE MACRO dq_reason(codes, sev) AS (
    SELECT string_agg(reason, '; ' ORDER BY ordinal)
    FROM dq_check_catalog
    WHERE severity = sev AND list_contains(codes, code)
);

CREATE OR REPLACE MACRO dq_status_of(codes) AS (
    CASE
        WHEN dq_reason(codes, 'Critical') IS NOT NULL THEN 'FAIL'
        WHEN dq_reason(codes, 'Warning') IS NOT NULL THEN 'WARN'
        ELSE 'PASS'
    END
);

-- Standardisation shared by orders and payments.
CREATE OR REPLACE MACRO std_text(x) AS nullif(trim(x), '');
CREATE OR REPLACE MACRO std_payment_mode(x) AS coalesce(
    (SELECT standard FROM ref_payment_mode_alias WHERE alias = lower(trim(x))),
    std_text(x)
);
CREATE OR REPLACE MACRO title_word(x) AS upper(left(std_text(x), 1)) || lower(substr(std_text(x), 2));

-- PII masking: 'priya.hegde@example.com' -> 'pr***@example.com', '9774349021' -> '97******21'
CREATE OR REPLACE MACRO mask_email(e) AS CASE
    WHEN regexp_full_match(std_text(e), '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
    THEN left(std_text(e), 2) || '***@' || split_part(std_text(e), '@', 2)
END;
CREATE OR REPLACE MACRO mask_phone(p) AS CASE
    WHEN std_text(p) IS NULL THEN NULL
    WHEN length(std_text(p)) <= 4 THEN repeat('*', length(std_text(p)))
    ELSE left(std_text(p), 2) || repeat('*', length(std_text(p)) - 4) || right(std_text(p), 2)
END;
CREATE OR REPLACE MACRO mask_name(n) AS CASE
    WHEN std_text(n) IS NULL THEN NULL
    WHEN len(string_split(std_text(n), ' ')) = 1 THEN std_text(n)
    ELSE split_part(std_text(n), ' ', 1) || ' ' || left(string_split(std_text(n), ' ')[-1], 1) || '.'
END;
