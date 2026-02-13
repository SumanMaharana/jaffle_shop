-- Singular test: Random customer validation
-- Simulates a flaky data quality check
-- Returns a random subset of customers as "invalid"

with random_seed as (
    select random() as seed_value
),

flagged_customers as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        random() as random_score,
        case
            when random() < 0.2 then 'FAIL'
            when random() < 0.5 then 'WARN'
            else 'PASS'
        end as test_status
    from {{ ref('customers') }} c
    cross join random_seed
)

select
    customer_id,
    first_name,
    last_name,
    random_score,
    test_status,
    'Randomly flagged customer record - test status: ' || test_status as message
from flagged_customers
where test_status in ('FAIL', 'WARN')
    and random() < 0.4  -- 40% chance of being included in failed results

-- This test randomly identifies "problematic" records
-- Each run produces different results!
