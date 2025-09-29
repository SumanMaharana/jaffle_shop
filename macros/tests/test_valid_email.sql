{% macro test_valid_email(model, column_name) %}

with validation as (
    select
        {{ column_name }} as email_field

    from {{ model }}
    where {{ column_name }} is not null
),

validation_errors as (
    select
        email_field

    from validation
    where not regexp_like(email_field, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
)

select * from validation_errors

{% endmacro %}