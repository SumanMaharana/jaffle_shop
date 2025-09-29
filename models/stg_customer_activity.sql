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
    select
        order_id,
        customer_id,
        order_date,
        status,
        cycle_name,

        -- Compute date-based columns
        date_trunc('month', order_date) as order_month,
        extract(year from order_date) as order_year,
        extract(dow from order_date) as order_day_of_week,
        extract(quarter from order_date) as order_quarter,

        -- Group order status for analytics
        case
            when status in ('completed', 'shipped', 'placed') then 'successful'
            when status in ('returned', 'return_pending') then 'returned'
            else 'other'
        end as order_status_group

    from {{ source('dbt_production', 'stg_orders') }}
),

payments as (
    select
        payment_id,
        order_id,
        payment_method,
        amount,
        cycle_name,

        -- Compute payment categorization
        case
            when payment_method in ('credit_card', 'bank_transfer') then 'standard'
            when payment_method in ('coupon', 'gift_card') then 'promotional'
            else 'other'
        end as payment_category,

        -- Flag promotional payments
        case
            when payment_method in ('coupon', 'gift_card') then true
            else false
        end as is_promotional

    from {{ source('dbt_production', 'stg_payments') }}
),

customer_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        o.status,
        o.cycle_name,
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