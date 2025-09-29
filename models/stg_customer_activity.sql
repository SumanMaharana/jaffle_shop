{{
    config(
        materialized='view'
    )
}}

-- This model creates a unified view of customer activity for marketing analytics
with customers as (
    select * from {{ source('dbt_production', 'stg_customers') }}
),

orders as (
    select * from {{ source('dbt_production', 'stg_orders') }}
),

payments as (
    select * from {{ source('dbt_production', 'stg_payments') }}
),

customer_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        o.status,
        o.product_name,
        o.order_status_group,
        o.order_month,
        o.order_year,
        o.order_day_of_week,
        o.order_quarter,

        -- Customer lifecycle stage based on order history
        case
            when row_number() over (partition by c.customer_id order by o.order_date) = 1 then 'new'
            when count(o.order_id) over (partition by c.customer_id) > 3 then 'loyal'
            else 'active'
        end as customer_stage

    from customers c
    inner join orders o on c.customer_id = o.customer_id
),

final as (
    select
        co.*,
        p.payment_id,
        p.payment_method,
        p.amount,
        p.payment_category,
        p.is_promotional,

        -- Running totals for customer value tracking
        sum(p.amount) over (
            partition by co.customer_id
            order by co.order_date
            rows between unbounded preceding and current row
        ) as cumulative_customer_value

    from customer_orders co
    left join payments p on co.order_id = p.order_id
)

select * from final