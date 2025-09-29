{{
    config(
        materialized='table',
        tags=['marketing', 'customer_analytics']
    )
}}

-- RFM (Recency, Frequency, Monetary) Analysis for Customer Segmentation
with customer_product_prefs as (
    select
        customer_id,
        cycle_name,
        count(*) as product_count,
        row_number() over (partition by customer_id order by count(*) desc) as product_rank
    from {{ ref('stg_customer_activity') }}
    where order_status_group = 'successful'
    group by 1, 2
),

customer_payment_prefs as (
    select
        customer_id,
        payment_method,
        count(*) as payment_count,
        row_number() over (partition by customer_id order by count(*) desc) as payment_rank
    from {{ ref('stg_customer_activity') }}
    where order_status_group = 'successful'
    group by 1, 2
),

customer_metrics as (
    select
        ca.customer_id,
        ca.first_name,
        ca.last_name,

        -- Recency: Days since last order
        datediff('day', max(ca.order_date), current_date) as days_since_last_order,

        -- Frequency: Number of orders
        count(distinct ca.order_id) as total_orders,

        -- Monetary: Total spend
        sum(ca.amount) as total_revenue,

        -- Additional metrics
        avg(ca.amount) as avg_order_value,
        min(ca.order_date) as first_order_date,
        max(ca.order_date) as last_order_date,
        datediff('day', min(ca.order_date), max(ca.order_date)) as customer_lifespan_days,

        -- Product preferences
        max(case when cpp.product_rank = 1 then cpp.cycle_name end) as favorite_product,
        count(distinct ca.cycle_name) as unique_products_purchased,

        -- Payment preferences
        max(case when cpm.payment_rank = 1 then cpm.payment_method end) as preferred_payment_method,
        sum(case when ca.is_promotional then ca.amount else 0 end) as promotional_spend,
        sum(case when ca.is_promotional then ca.amount else 0 end) / nullif(sum(ca.amount), 0) as promotional_spend_ratio

    from {{ ref('stg_customer_activity') }} ca
    left join customer_product_prefs cpp on ca.customer_id = cpp.customer_id
    left join customer_payment_prefs cpm on ca.customer_id = cpm.customer_id
    where ca.order_status_group = 'successful'
    group by 1, 2, 3
),

rfm_scores as (
    select
        *,

        -- Calculate RFM scores (1-5, where 5 is best)
        ntile(5) over (order by days_since_last_order desc) as recency_score,
        ntile(5) over (order by total_orders) as frequency_score,
        ntile(5) over (order by total_revenue) as monetary_score

    from customer_metrics
),

rfm_segments as (
    select
        *,

        -- Combine scores for segmentation
        recency_score * 100 + frequency_score * 10 + monetary_score as rfm_combined_score,

        -- Create customer segments based on RFM scores
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 4 then 'Loyal Customers'
            when recency_score >= 3 and frequency_score >= 1 and monetary_score >= 3 then 'Potential Loyalists'
            when recency_score >= 4 and frequency_score <= 2 then 'New Customers'
            when recency_score >= 3 and frequency_score <= 2 then 'Promising'
            when recency_score >= 2 and frequency_score >= 3 then 'Need Attention'
            when recency_score <= 2 and frequency_score >= 4 then 'At Risk'
            when recency_score <= 1 and monetary_score >= 4 then 'Cant Lose Them'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2 then 'Hibernating'
            else 'Lost'
        end as customer_segment,

        -- Value tier
        case
            when monetary_score >= 4 then 'High Value'
            when monetary_score = 3 then 'Medium Value'
            else 'Low Value'
        end as value_tier,

        -- Engagement level
        case
            when recency_score >= 4 and frequency_score >= 3 then 'Highly Engaged'
            when recency_score >= 3 or frequency_score >= 3 then 'Moderately Engaged'
            else 'Low Engagement'
        end as engagement_level

    from rfm_scores
),

final as (
    select
        -- Customer identifiers
        customer_id,
        first_name,
        last_name,

        -- RFM metrics
        days_since_last_order,
        total_orders,
        total_revenue,
        avg_order_value,

        -- RFM scores
        recency_score,
        frequency_score,
        monetary_score,
        rfm_combined_score,

        -- Segmentation
        customer_segment,
        value_tier,
        engagement_level,

        -- Customer lifecycle
        first_order_date,
        last_order_date,
        customer_lifespan_days,

        -- Preferences
        favorite_product,
        unique_products_purchased,
        preferred_payment_method,
        promotional_spend,
        promotional_spend_ratio,

        -- Marketing action recommendations
        case
            when customer_segment = 'Champions' then 'Reward loyalty, early access to new products'
            when customer_segment = 'Loyal Customers' then 'Upsell higher value products'
            when customer_segment = 'Potential Loyalists' then 'Membership offers, loyalty programs'
            when customer_segment = 'New Customers' then 'Welcome series, onboarding'
            when customer_segment = 'Promising' then 'Create brand awareness, free trials'
            when customer_segment = 'Need Attention' then 'Reactivation campaigns, limited time offers'
            when customer_segment = 'At Risk' then 'Win-back campaigns, feedback surveys'
            when customer_segment = 'Cant Lose Them' then 'Win-back with special offers'
            when customer_segment = 'Hibernating' then 'Recreation campaigns, new product announcements'
            else 'Consider removing from active campaigns'
        end as recommended_action,

        -- Metadata
        current_timestamp as analysis_timestamp

    from rfm_segments
)

select * from final