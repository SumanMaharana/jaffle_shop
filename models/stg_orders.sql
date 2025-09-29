{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('dbt_production', 'orders') }}

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status,
        cycle_name as product_name,

        -- Marketing-specific fields
        case
            when status in ('completed', 'shipped') then 'successful'
            when status in ('returned', 'return_pending') then 'returned'
            else 'pending'
        end as order_status_group,

        -- Extract year and month for cohort analysis
        date_trunc('month', order_date) as order_month,
        date_trunc('year', order_date) as order_year,

        -- Day of week for behavioral analysis
        extract(dow from order_date) as order_day_of_week,

        -- Quarter for seasonal analysis
        extract(quarter from order_date) as order_quarter

    from source

)

select * from renamed