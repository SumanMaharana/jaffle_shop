{% test random_threshold(model, column_name, min_threshold=0, max_threshold=100) %}
{#
    Returns exactly 0, (max_threshold // 3), or max_threshold rows — each ~33% probability.
    Uses a SINGLE random() call per run so the outcome is consistent within a run
    but varies across runs.

    Configure in schema.yml:
      warn_if:  ">0"
      error_if: ">{{ (max_threshold / 3) | int }}"

    Example with max_threshold=25 (warn_rows=8, fail_rows=25):
      r < 0.33            → 0 rows  → SUCCESS
      0.33 <= r < 0.67    → 8 rows  → WARN   (>0 but not >8)
      r >= 0.67           → 25 rows → FAIL   (>8)
#}

{% set warn_rows = (max_threshold / 3) | int %}

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
    select generate_series(1, {{ max_threshold }}) as n
),

result as (
    select n
    from series
    cross join random_val
    where
        -- WARN zone: return warn_rows rows
        (r >= 0.33 and r < 0.67 and n <= {{ warn_rows }})
        -- FAIL zone: return max_threshold rows
        or (r >= 0.67)
        -- SUCCESS zone (r < 0.33): no WHERE condition matches → 0 rows
)

select * from result

{% endtest %}
