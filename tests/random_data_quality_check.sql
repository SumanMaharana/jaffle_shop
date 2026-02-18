{{ config(warn_if='>0', error_if='>3') }}

-- Singular test: Random data quality check on orders
-- Uses a SINGLE random() per run to guarantee one of three distinct outcomes:
--   r < 0.33            → 0 rows  → SUCCESS
--   0.33 <= r < 0.67    → 2 rows  → WARN   (>0, not >3)
--   r >= 0.67           → 8 rows  → FAIL   (>3)

with
random_val as (
    select random() as r
),

series as (
    select generate_series(1, 8) as n
),

result as (
    select
        n                                          as row_index,
        'Random data quality check failure'        as test_message,
        round((select r from random_val)::numeric, 4) as random_value
    from series
    cross join random_val
    where
        (r >= 0.33 and r < 0.67 and n <= 2)   -- WARN: 2 rows
        or (r >= 0.67)                          -- FAIL: 8 rows
        -- SUCCESS: r < 0.33 returns 0 rows
)

select * from result
