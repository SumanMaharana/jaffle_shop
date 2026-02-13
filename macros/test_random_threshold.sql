{% test random_threshold(model, column_name, min_threshold=0, max_threshold=100) %}
{#
    Test that randomly generates a count that may exceed warn/error thresholds
    Use with warn_if and error_if configurations in schema.yml
#}

with random_count as (
    select
        count(*) as record_count,
        floor(random() * {{ max_threshold }})::int as random_threshold_value
    from {{ model }}
),

validation as (
    select
        {{ column_name }},
        (select random_threshold_value from random_count) as threshold_value
    from {{ model }}
    limit (select random_threshold_value from random_count)
)

select *
from validation
-- Returns random number of rows (0 to max_threshold)
-- Configure warn_if: ">5" and error_if: ">10" for varied results

{% endtest %}
