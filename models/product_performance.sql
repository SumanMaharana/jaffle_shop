{{
    config(
        materialized='table',
        tags=['marketing', 'product_analytics']
    )
}}

-- Product Performance Analytics for Marketing Insights
with product_sales as (
    select
        cycle_name,
        order_id,
        customer_id,
        order_date,
        order_month,
        order_quarter,
        order_year,
        order_status_group,
        amount,
        payment_method,
        is_promotional

    from {{ ref('stg_customer_activity') }}
),

product_metrics as (
    select
        cycle_name,

        -- Sales metrics
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(amount) as total_revenue,
        avg(amount) as avg_order_value,
        stddev(amount) as revenue_stddev,
        min(amount) as min_order_value,
        max(amount) as max_order_value,

        -- Customer metrics
        count(distinct customer_id) * 1.0 / nullif(
            (select count(distinct customer_id) from {{ source('dbt_production', 'stg_customers') }}), 0
        ) as market_penetration,

        -- Return metrics
        sum(case when order_status_group = 'returned' then 1 else 0 end) as return_count,
        sum(case when order_status_group = 'returned' then 1 else 0 end) * 1.0 /
            nullif(count(distinct order_id), 0) as return_rate,

        -- Promotional impact
        sum(case when is_promotional then amount else 0 end) as promotional_revenue,
        sum(case when is_promotional then 1 else 0 end) as promotional_orders,
        sum(case when is_promotional then amount else 0 end) * 1.0 /
            nullif(sum(amount), 0) as promotional_revenue_ratio,

        -- Time-based metrics
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        count(distinct order_month) as active_months,
        datediff('day', min(order_date), max(order_date)) as product_lifecycle_days,

        -- Payment method distribution
        count(case when payment_method = 'credit_card' then 1 end) as credit_card_orders,
        count(case when payment_method = 'coupon' then 1 end) as coupon_orders,
        count(case when payment_method = 'bank_transfer' then 1 end) as bank_transfer_orders,
        count(case when payment_method = 'gift_card' then 1 end) as gift_card_orders

    from product_sales
    where order_status_group in ('successful', 'returned')
    group by 1
),

product_trends as (
    select
        cycle_name,
        order_month,
        count(distinct order_id) as monthly_orders,
        sum(amount) as monthly_revenue,
        count(distinct customer_id) as monthly_customers,

        -- Calculate month-over-month growth
        lag(sum(amount)) over (partition by cycle_name order by order_month) as prev_month_revenue,
        (sum(amount) - lag(sum(amount)) over (partition by cycle_name order by order_month)) /
            nullif(lag(sum(amount)) over (partition by cycle_name order by order_month), 0) as mom_growth

    from product_sales
    where order_status_group = 'successful'
    group by 1, 2
),

product_customer_analysis as (
    select
        cycle_name,

        -- Repeat purchase metrics
        avg(customer_order_count) as avg_orders_per_customer,
        sum(case when customer_order_count > 1 then 1 else 0 end) * 1.0 /
            nullif(count(distinct customer_id), 0) as repeat_purchase_rate

    from (
        select
            cycle_name,
            customer_id,
            count(distinct order_id) as customer_order_count
        from product_sales
        where order_status_group = 'successful'
        group by 1, 2
    ) customer_product_orders
    group by 1
),

product_ranking as (
    select
        cycle_name,
        total_revenue,
        total_orders,

        -- Rankings
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_orders desc) as volume_rank,
        row_number() over (order by unique_customers desc) as popularity_rank,

        -- Percentiles
        percent_rank() over (order by total_revenue) as revenue_percentile,
        percent_rank() over (order by total_orders) as volume_percentile,

        -- ABC Analysis
        case
            when sum(total_revenue) over (order by total_revenue desc) <=
                 sum(total_revenue) over () * 0.8 then 'A'
            when sum(total_revenue) over (order by total_revenue desc) <=
                 sum(total_revenue) over () * 0.95 then 'B'
            else 'C'
        end as abc_category

    from product_metrics
),

final as (
    select
        pm.cycle_name,

        -- Core metrics
        pm.total_orders,
        pm.unique_customers,
        pm.total_revenue,
        pm.avg_order_value,
        pm.revenue_stddev,
        pm.min_order_value,
        pm.max_order_value,

        -- Market metrics
        pm.market_penetration,
        pca.repeat_purchase_rate,
        pca.avg_orders_per_customer,

        -- Return analysis
        pm.return_count,
        pm.return_rate,

        -- Promotional analysis
        pm.promotional_revenue,
        pm.promotional_orders,
        pm.promotional_revenue_ratio,

        -- Lifecycle metrics
        pm.first_sale_date,
        pm.last_sale_date,
        pm.active_months,
        pm.product_lifecycle_days,

        -- Rankings and categorization
        pr.revenue_rank,
        pr.volume_rank,
        pr.popularity_rank,
        pr.revenue_percentile,
        pr.volume_percentile,
        pr.abc_category,

        -- Payment preferences
        pm.credit_card_orders,
        pm.coupon_orders,
        pm.bank_transfer_orders,
        pm.gift_card_orders,

        -- Performance indicators
        case
            when pr.revenue_rank <= 5 then 'Star'
            when pm.return_rate > 0.2 then 'Problem'
            when pr.revenue_percentile > 0.5 and pca.repeat_purchase_rate > 0.3 then 'Cash Cow'
            when pm.product_lifecycle_days < 30 then 'New'
            when pm.last_sale_date < dateadd('day', -60, current_date) then 'Declining'
            else 'Standard'
        end as product_status,

        -- Marketing recommendations
        case
            when pr.abc_category = 'A' then 'Focus marketing spend, premium positioning'
            when pm.return_rate > 0.15 then 'Investigate quality issues, improve product descriptions'
            when pca.repeat_purchase_rate < 0.1 then 'Develop retention strategies, bundle offers'
            when pm.promotional_revenue_ratio > 0.5 then 'Reduce dependency on promotions'
            when pm.market_penetration < 0.05 then 'Increase awareness campaigns'
            else 'Maintain current strategy'
        end as marketing_recommendation,

        -- Metadata
        current_timestamp as analysis_timestamp

    from product_metrics pm
    left join product_customer_analysis pca on pm.cycle_name = pca.cycle_name
    left join product_ranking pr on pm.cycle_name = pr.cycle_name
)

select * from final
order by revenue_rank