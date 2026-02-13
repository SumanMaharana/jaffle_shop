-- Singular test: Random data quality check
-- This test randomly returns 0-10 rows, causing random pass/fail/warn
-- Status depends on how many rows are returned:
--   0 rows = SUCCESS
--   1-5 rows = WARNING (if configured with warn_if)
--   >5 rows = FAILURE

-- Random number generator (0-1)
-- We use it to decide how many "bad" records to return

select
    order_id,
    amount,
    random() as random_value,
    'Random test - this may pass, warn, or fail' as test_message
from {{ ref('orders') }}
where random() < 0.3  -- 30% chance each row is selected
limit 10

-- Run this test multiple times and you'll see different results!
-- Configure in dbt_project.yml with:
-- tests:
--   jaffle_shop:
--     +severity: warn  # Makes failures into warnings
