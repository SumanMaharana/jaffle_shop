{{
    config(
        materialized='table',
        tags=['marketing', 'attribution']
    )
}}

-- Marketing Attribution Model for Campaign Performance Analysis
-- Since we don't have explicit campaign data, we'll create synthetic attribution based on patterns

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
            -- Promotional payments indicate campaign response
            when is_promotional then 'Promotional Campaign'
            -- Weekend orders might be from social media
            when order_day_of_week in (0, 6) then 'Social Media'
            -- High-value orders might be from email campaigns
            when amount > 100 then 'Email Marketing'
            -- First orders in Q1 might be from New Year campaigns
            when order_quarter = 1 and row_number() over (partition by customer_id order by order_date) = 1 then 'New Year Campaign'
            -- Default to organic
            else 'Organic'
        end as attribution_channel,

        -- Create synthetic campaign types
        case
            when is_promotional and payment_method = 'coupon' then 'Discount Campaign'
            when is_promotional and payment_method = 'gift_card' then 'Gift Card Promotion'
            when order_quarter = 4 then 'Holiday Campaign'
            when order_quarter = 2 then 'Spring Campaign'
            when order_quarter = 3 then 'Summer Campaign'
            else 'Evergreen Campaign'
        end as campaign_type,

        -- Customer acquisition flag
        row_number() over (partition by customer_id order by order_date) = 1 as is_new_customer

    from {{ ref('stg_customer_activity') }}
    where order_status_group = 'successful'
),

channel_performance as (
    select
        attribution_channel,

        -- Volume metrics
        count(distinct order_id) as total_conversions,
        count(distinct customer_id) as unique_customers,
        sum(case when is_new_customer then 1 else 0 end) as new_customer_acquisitions,

        -- Revenue metrics
        sum(amount) as total_revenue,
        avg(amount) as avg_order_value,
        min(amount) as min_order_value,
        max(amount) as max_order_value,

        -- Efficiency metrics
        sum(amount) / nullif(count(distinct order_id), 0) as revenue_per_conversion,
        count(distinct customer_id) * 1.0 / nullif(count(distinct order_id), 0) as customer_conversion_rate,

        -- Product mix
        mode() within group (order by cycle_name) as top_product,
        count(distinct cycle_name) as product_diversity

    from customer_orders
    group by 1
),

campaign_performance as (
    select
        campaign_type,
        attribution_channel,

        -- Campaign metrics
        count(distinct order_id) as campaign_conversions,
        sum(amount) as campaign_revenue,
        count(distinct customer_id) as campaign_reach,
        avg(amount) as campaign_aov,

        -- ROI proxy (assuming cost structure)
        case
            when campaign_type like '%Discount%' then sum(amount) * 0.7  -- 30% cost
            when campaign_type like '%Gift%' then sum(amount) * 0.6      -- 40% cost
            when campaign_type like '%Holiday%' then sum(amount) * 0.75  -- 25% cost
            else sum(amount) * 0.85                                       -- 15% cost
        end as estimated_profit

    from customer_orders
    group by 1, 2
),

customer_journey as (
    select
        customer_id,
        first_name,
        last_name,

        -- First touch attribution
        first_value(attribution_channel) over (
            partition by customer_id
            order by order_date
            rows between unbounded preceding and unbounded following
        ) as first_touch_channel,

        -- Last touch attribution
        last_value(attribution_channel) over (
            partition by customer_id
            order by order_date
            rows between unbounded preceding and unbounded following
        ) as last_touch_channel,

        -- Multi-touch insights
        count(distinct attribution_channel) over (partition by customer_id) as channels_engaged,
        sum(amount) over (partition by customer_id) as customer_total_value

    from customer_orders
),

attribution_summary as (
    select
        'First Touch' as attribution_model,
        first_touch_channel as channel,
        count(distinct customer_id) as attributed_customers,
        sum(customer_total_value) / count(distinct customer_id) as attributed_value

    from (
        select distinct
            customer_id,
            first_touch_channel,
            customer_total_value
        from customer_journey
    ) first_touch
    group by 1, 2

    union all

    select
        'Last Touch' as attribution_model,
        last_touch_channel as channel,
        count(distinct customer_id) as attributed_customers,
        sum(customer_total_value) / count(distinct customer_id) as attributed_value

    from (
        select distinct
            customer_id,
            last_touch_channel,
            customer_total_value
        from customer_journey
    ) last_touch
    group by 1, 2
),

channel_trends as (
    select
        attribution_channel,
        order_month,
        count(distinct order_id) as monthly_conversions,
        sum(amount) as monthly_revenue,

        -- Calculate month-over-month growth
        (sum(amount) - lag(sum(amount)) over (partition by attribution_channel order by order_month)) /
            nullif(lag(sum(amount)) over (partition by attribution_channel order by order_month), 0) as mom_growth_rate

    from customer_orders
    group by 1, 2
),

final as (
    select
        cp.attribution_channel,

        -- Performance metrics
        cp.total_conversions,
        cp.unique_customers,
        cp.new_customer_acquisitions,
        cp.total_revenue,
        cp.avg_order_value,
        cp.revenue_per_conversion,
        cp.customer_conversion_rate,

        -- Product insights
        cp.top_product,
        cp.product_diversity,

        -- Channel efficiency score (composite metric)
        (
            cp.revenue_per_conversion / nullif((select avg(revenue_per_conversion) from channel_performance), 0) * 0.4 +
            cp.customer_conversion_rate / nullif((select avg(customer_conversion_rate) from channel_performance), 0) * 0.3 +
            cp.new_customer_acquisitions * 1.0 / nullif((select sum(new_customer_acquisitions) from channel_performance), 0) * 0.3
        ) * 100 as channel_efficiency_score,

        -- Channel classification
        case
            when cp.total_revenue > (select avg(total_revenue) * 1.5 from channel_performance) then 'High Performer'
            when cp.new_customer_acquisitions > (select avg(new_customer_acquisitions) * 1.2 from channel_performance) then 'Growth Driver'
            when cp.avg_order_value > (select avg(avg_order_value) * 1.2 from channel_performance) then 'Premium Channel'
            when cp.customer_conversion_rate > (select avg(customer_conversion_rate) * 1.1 from channel_performance) then 'Efficient Converter'
            else 'Standard Channel'
        end as channel_classification,

        -- Marketing recommendations
        case
            when cp.attribution_channel = 'Promotional Campaign' and cp.revenue_per_conversion < 50 then 'Review promotion depth, may be over-discounting'
            when cp.attribution_channel = 'Social Media' and cp.product_diversity < 3 then 'Expand product promotion variety'
            when cp.attribution_channel = 'Email Marketing' and cp.new_customer_acquisitions < 5 then 'Focus on acquisition campaigns'
            when cp.attribution_channel = 'Organic' and cp.total_conversions > 20 then 'High organic performance, invest in SEO'
            else 'Optimize channel mix for better ROI'
        end as optimization_recommendation,

        -- Metadata
        current_timestamp as analysis_timestamp

    from channel_performance cp
)

select * from final
order by total_revenue desc