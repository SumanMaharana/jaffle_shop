{{
    config(
        materialized='table',
        tags=['marketing', 'cohort_analysis']
    )
}}

-- Customer Cohort Analysis for Retention and LTV tracking
with customer_cohorts as (
    select
        customer_id,
        first_name,
        last_name,
        date_trunc('month', min(order_date)) as cohort_month,
        min(order_date) as first_order_date

    from {{ ref('stg_customer_activity') }}
    where order_status_group = 'successful'
    group by 1, 2, 3
),

cohort_orders as (
    select
        c.customer_id,
        c.cohort_month,
        c.first_order_date,
        ca.order_id,
        ca.order_date,
        ca.amount,
        ca.cycle_name,
        date_trunc('month', ca.order_date) as order_month,

        -- Calculate months since first purchase
        datediff('month', c.cohort_month, date_trunc('month', ca.order_date)) as months_since_first_purchase

    from customer_cohorts c
    inner join {{ ref('stg_customer_activity') }} ca on c.customer_id = ca.customer_id
    where ca.order_status_group = 'successful'
),

cohort_retention as (
    select
        cohort_month,
        months_since_first_purchase,

        -- Customer counts
        count(distinct customer_id) as customers_active,
        sum(amount) as cohort_revenue,
        avg(amount) as avg_order_value,
        count(distinct order_id) as total_orders

    from cohort_orders
    group by 1, 2
),

cohort_size as (
    select
        cohort_month,
        count(distinct customer_id) as cohort_size,
        sum(amount) as first_month_revenue

    from cohort_orders
    where months_since_first_purchase = 0
    group by 1
),

retention_matrix as (
    select
        cr.cohort_month,
        cr.months_since_first_purchase,
        cs.cohort_size,
        cr.customers_active,
        cr.cohort_revenue,
        cr.avg_order_value,
        cr.total_orders,

        -- Retention rate
        cr.customers_active * 100.0 / nullif(cs.cohort_size, 0) as retention_rate,

        -- Revenue metrics
        cr.cohort_revenue / nullif(cs.cohort_size, 0) as revenue_per_original_customer,
        cr.cohort_revenue / nullif(cr.customers_active, 0) as revenue_per_active_customer,

        -- Cumulative metrics
        sum(cr.cohort_revenue) over (
            partition by cr.cohort_month
            order by cr.months_since_first_purchase
            rows between unbounded preceding and current row
        ) as cumulative_revenue,

        sum(cr.customers_active) over (
            partition by cr.cohort_month
            order by cr.months_since_first_purchase
            rows between unbounded preceding and current row
        ) as cumulative_customers_retained

    from cohort_retention cr
    left join cohort_size cs on cr.cohort_month = cs.cohort_month
),

cohort_ltv as (
    select
        cohort_month,
        max(months_since_first_purchase) as cohort_age_months,
        max(cumulative_revenue) / nullif(max(cohort_size), 0) as cohort_ltv,
        max(cumulative_revenue) as total_cohort_revenue,

        -- Calculate average retention over first 6 months
        avg(case when months_since_first_purchase between 1 and 6 then retention_rate end) as avg_6m_retention,

        -- Calculate average retention over first 12 months
        avg(case when months_since_first_purchase between 1 and 12 then retention_rate end) as avg_12m_retention

    from retention_matrix
    group by 1
),

cohort_product_mix as (
    select
        cohort_month,
        cycle_name,
        count(distinct customer_id) as customers,
        sum(amount) as product_revenue,
        row_number() over (partition by cohort_month order by sum(amount) desc) as product_rank

    from cohort_orders
    group by 1, 2
),

top_products_by_cohort as (
    select
        cohort_month,
        string_agg(
            case when product_rank <= 3 then cycle_name end,
            ', '
            order by product_rank
        ) as top_3_products

    from cohort_product_mix
    where product_rank <= 3
    group by 1
),

final as (
    select
        rm.cohort_month,
        rm.months_since_first_purchase,
        rm.cohort_size,
        rm.customers_active,
        rm.retention_rate,
        rm.cohort_revenue,
        rm.avg_order_value,
        rm.total_orders,
        rm.revenue_per_original_customer,
        rm.revenue_per_active_customer,
        rm.cumulative_revenue,
        cl.cohort_ltv,
        cl.cohort_age_months,
        cl.avg_6m_retention,
        cl.avg_12m_retention,
        tp.top_3_products,

        -- Cohort quality score (based on retention and LTV)
        case
            when cl.avg_6m_retention > 40 and cl.cohort_ltv > 500 then 'Premium'
            when cl.avg_6m_retention > 30 and cl.cohort_ltv > 300 then 'High Value'
            when cl.avg_6m_retention > 20 and cl.cohort_ltv > 200 then 'Standard'
            when cl.avg_6m_retention > 10 then 'Below Average'
            else 'At Risk'
        end as cohort_quality,

        -- Retention stage classification
        case
            when rm.months_since_first_purchase = 0 then 'Acquisition'
            when rm.months_since_first_purchase <= 3 then 'Activation'
            when rm.months_since_first_purchase <= 6 then 'Retention'
            when rm.months_since_first_purchase <= 12 then 'Revenue'
            else 'Referral'
        end as retention_stage,

        -- Marketing insights
        case
            when rm.months_since_first_purchase = 0 then 'Focus on onboarding experience'
            when rm.months_since_first_purchase = 1 and rm.retention_rate < 50 then 'Improve first month experience'
            when rm.months_since_first_purchase between 2 and 3 and rm.retention_rate < 30 then 'Launch retention campaigns'
            when rm.months_since_first_purchase between 4 and 6 and rm.retention_rate < 20 then 'Implement win-back strategies'
            when rm.months_since_first_purchase > 12 and rm.retention_rate > 10 then 'Focus on loyalty programs'
            else 'Monitor performance'
        end as marketing_action,

        -- Metadata
        current_timestamp as analysis_timestamp

    from retention_matrix rm
    left join cohort_ltv cl on rm.cohort_month = cl.cohort_month
    left join top_products_by_cohort tp on rm.cohort_month = tp.cohort_month
)

select * from final
order by cohort_month desc, months_since_first_purchase