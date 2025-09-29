{% macro test_recency_freshness(model, column_name, days_threshold=90) %}

with validation as (
    select
        {{ column_name }} as date_field,
        datediff('day', {{ column_name }}, current_date) as days_old

    from {{ model }}
    where {{ column_name }} is not null
),

validation_errors as (
    select
        date_field,
        days_old

    from validation
    where days_old > {{ days_threshold }}
)

select * from validation_errors

{% endmacro %}