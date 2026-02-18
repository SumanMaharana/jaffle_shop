{{ config(warn_if='>0', error_if='>4') }}

-- Singular test: Random customer validation
-- Uses a SINGLE random() per run to guarantee one of three distinct outcomes:
--   r < 0.33            → 0 rows  → SUCCESS
--   0.33 <= r < 0.67    → 3 rows  → WARN   (>0, not >4)
--   r >= 0.67           → 10 rows → FAIL   (>4)

with
random_val as (
    select random() as r
),

series as (
    select generate_series(1, 10) as n
),

result as (
    select
        n                                              as row_index,
        'Randomly flagged customer record'             as test_message,
        case
            when (select r from random_val) >= 0.67 then 'FAIL'
            else 'WARN'
        end                                            as status,
        round((select r from random_val)::numeric, 4) as random_value
    from series
    cross join random_val
    where
        (r >= 0.33 and r < 0.67 and n <= 3)   -- WARN: 3 rows
        or (r >= 0.67)                          -- FAIL: 10 rows
        -- SUCCESS: r < 0.33 returns 0 rows
)

select * from result
