{% macro test_percentage_range(model, column_name) %}

with validation as (
    select
        {{ column_name }} as percentage_field

    from {{ model }}
    where {{ column_name }} is not null
),

validation_errors as (
    select
        percentage_field

    from validation
    where percentage_field < 0 or percentage_field > 100
)

select * from validation_errors

{% endmacro %}