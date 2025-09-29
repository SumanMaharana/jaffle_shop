{% macro test_positive_value(model, column_name) %}

with validation as (
    select
        {{ column_name }} as value_field

    from {{ model }}
    where {{ column_name }} is not null
),

validation_errors as (
    select
        value_field

    from validation
    where value_field <= 0
)

select * from validation_errors

{% endmacro %}