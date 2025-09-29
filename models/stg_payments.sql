{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('dbt_production', 'payments') }}

),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,
        cycle_name as product_name,

        -- Convert amount from cents to dollars
        amount / 100.0 as amount,

        -- Marketing-specific categorizations
        case
            when payment_method = 'credit_card' then 'card'
            when payment_method = 'bank_transfer' then 'bank'
            when payment_method in ('coupon', 'gift_card') then 'promotional'
            else 'other'
        end as payment_category,

        -- Flag for promotional payments
        case
            when payment_method in ('coupon', 'gift_card') then true
            else false
        end as is_promotional

    from source

)

select * from renamed