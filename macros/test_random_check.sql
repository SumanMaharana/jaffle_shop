{% test random_check(model, column_name, warn_rows=3, fail_rows=12) %}
{#
    Returns exactly 0, warn_rows, or fail_rows rows — each with ~33% probability.
    Uses a SINGLE random() call per run so the outcome is consistent within a run
    but varies across runs.

    Configure in schema.yml:
      warn_if:  ">0"              (warn when any rows returned)
      error_if: ">{{ warn_rows }}" (fail when more than warn_rows returned)

    Outcomes:
      r < 0.33            → 0 rows         → SUCCESS
      0.33 <= r < 0.67    → warn_rows rows  → WARN
      r >= 0.67           → fail_rows rows  → FAIL
#}

with
-- Reference the model so dbt tracks lineage correctly
model_ref as (
    select {{ column_name }} from {{ model }} limit 1
),

-- Single random value determines outcome for the entire test run
random_val as (
    select random() as r
),

-- Generate enough rows to cover the fail case
series as (
    select generate_series(1, {{ fail_rows }}) as n
),

result as (
    select n
    from series
    cross join random_val
    where
        -- WARN zone: return warn_rows rows
        (r >= 0.33 and r < 0.67 and n <= {{ warn_rows }})
        -- FAIL zone: return fail_rows rows
        or (r >= 0.67)
        -- SUCCESS zone (r < 0.33): no WHERE condition matches → 0 rows
)

select * from result

{% endtest %}
