{{
    config(
        materialized='table',
        tags=['marketing', 'attribution']
    )
}}

-- Simplified Marketing Attribution Model for Campaign Performance Analysis

with customer_orders as (
    select
        customer_id,
        first_name,
        last_name,
        order_id,
        order_date,
        cycle_name,
        amount,
        payment_method,
        is_promotional,
        order_day_of_week,
        order_quarter,
        order_month,

        -- Create synthetic campaign attribution based on patterns
        case
            when is_promotional then 'Promotional Campaign'
            when order_day_of_week in (0, 6) then 'Social Media'
            when amount > 100 then 'Email Marketing'
            when order_quarter = 1 then 'New Year Campaign'
            else 'Organic'
        end as attribution_channel,

        -- Create synthetic campaign types
        case
            when is_promotional then 'Discount Campaign'
            when order_quarter = 4 then 'Holiday Campaign'
            when order_quarter = 2 then 'Spring Campaign'
            when order_quarter = 3 then 'Summer Campaign'
            else 'Evergreen Campaign'
        end as campaign_type

    from {{ ref('stg_customer_activity') }}
    where order_status_group = 'successful'
),

channel_performance as (
    select
        attribution_channel,

        -- Volume metrics
        count(distinct order_id) as total_conversions,
        count(distinct customer_id) as unique_customers,

        -- Revenue metrics
        sum(amount) as total_revenue,
        avg(amount) as avg_order_value,
        min(amount) as min_order_value,
        max(amount) as max_order_value,

        -- Efficiency metrics
        sum(amount) / nullif(count(distinct order_id), 0) as revenue_per_conversion,
        count(distinct customer_id) * 1.0 / nullif(count(distinct order_id), 0) as customer_conversion_rate,

        -- Product diversity
        count(distinct cycle_name) as product_diversity

    from customer_orders
    group by 1
),

campaign_summary as (
    select
        campaign_type,
        count(distinct order_id) as campaign_conversions,
        sum(amount) as campaign_revenue,
        count(distinct customer_id) as campaign_reach,
        avg(amount) as campaign_aov
    from customer_orders
    group by 1
),

final as (
    select
        cp.attribution_channel,

        -- Performance metrics
        cp.total_conversions,
        cp.unique_customers,
        cp.total_revenue,
        cp.avg_order_value,
        cp.revenue_per_conversion,
        cp.customer_conversion_rate,
        cp.product_diversity,

        -- Channel classification
        case
            when cp.total_revenue > 10000 then 'High Performer'
            when cp.unique_customers > 50 then 'Growth Driver'
            when cp.avg_order_value > 75 then 'Premium Channel'
            when cp.customer_conversion_rate > 0.8 then 'Efficient Converter'
            else 'Standard Channel'
        end as channel_classification,

        -- Marketing recommendations
        case
            when cp.attribution_channel = 'Promotional Campaign' and cp.revenue_per_conversion < 50 then 'Review promotion depth, may be over-discounting'
            when cp.attribution_channel = 'Social Media' and cp.product_diversity < 3 then 'Expand product promotion variety'
            when cp.attribution_channel = 'Email Marketing' and cp.unique_customers < 5 then 'Focus on acquisition campaigns'
            when cp.attribution_channel = 'Organic' and cp.total_conversions > 20 then 'High organic performance, invest in SEO'
            else 'Optimize channel mix for better ROI'
        end as optimization_recommendation,

        -- Metadata
        current_timestamp as analysis_timestamp

    from channel_performance cp
)

select * from final
order by total_revenue desc