{{ config(warn_if='>0', error_if='>5') }}

-- Singular test: Random payment anomaly detection
-- Uses a SINGLE random() per run to guarantee one of three distinct outcomes:
--   r < 0.33            → 0 rows  → SUCCESS
--   0.33 <= r < 0.67    → 4 rows  → WARN   (>0, not >5)
--   r >= 0.67           → 15 rows → FAIL   (>5)

with
random_val as (
    select random() as r
),

series as (
    select generate_series(1, 15) as n
),

result as (
    select
        n                                              as row_index,
        'Payment anomaly detected'                     as test_message,
        round((select r from random_val)::numeric, 4) as anomaly_score,
        case
            when (select r from random_val) >= 0.67 then 'CRITICAL_ANOMALY'
            else 'SUSPECTED_ANOMALY'
        end                                            as severity_level
    from series
    cross join random_val
    where
        (r >= 0.33 and r < 0.67 and n <= 4)   -- WARN: 4 rows
        or (r >= 0.67)                          -- FAIL: 15 rows
        -- SUCCESS: r < 0.33 returns 0 rows
)

select * from result
