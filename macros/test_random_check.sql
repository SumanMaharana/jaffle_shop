{% test random_check(model, column_name, failure_probability=0.33) %}
{#
    Custom test that randomly fails based on probability
    - failure_probability < 0.33: likely to pass
    - 0.33 <= failure_probability < 0.67: likely to warn (if configured)
    - failure_probability >= 0.67: likely to fail
#}

with random_validation as (
    select
        {{ column_name }},
        random() as random_value,
        {{ failure_probability }} as failure_threshold
    from {{ model }}
    where random() < {{ failure_probability }}
)

select *
from random_validation
-- Returns rows when random() < failure_probability (causing test to fail)
-- Returns no rows when random() >= failure_probability (test passes)

{% endtest %}
