# Marketing Analytics dbt Project Documentation

## Executive Summary

This dbt project provides a comprehensive marketing analytics platform built on top of e-commerce transaction data. It delivers actionable insights across customer segmentation, product performance, retention analysis, and marketing attribution to enable data-driven marketing decisions.

## Project Architecture

```
Raw Data (dbt_production)
    ├── customers
    ├── orders
    └── payments
         ↓
Staging Layer (Views)
    ├── stg_customers
    ├── stg_orders
    ├── stg_payments
    └── stg_customer_activity
         ↓
Marketing Analytics Layer (Tables)
    ├── customer_segmentation (RFM Analysis)
    ├── product_performance (Product Analytics)
    ├── customer_cohorts (Retention Analysis)
    └── marketing_attribution (Channel Performance)
         ↓
Core Business Models
    ├── customers (360° View)
    └── orders (Transactional Analytics)
```

## Data Models Overview

### 1. Staging Layer

#### stg_customers
- **Purpose**: Standardizes customer data
- **Key Fields**: customer_id, first_name, last_name
- **Tests**: Uniqueness, not null constraints

#### stg_orders
- **Purpose**: Enriches order data with marketing dimensions
- **Key Features**:
  - Order status grouping (successful/returned/pending)
  - Temporal analysis fields (day of week, quarter, month)
  - Product name preservation
- **Tests**: Foreign key relationships, accepted values

#### stg_payments
- **Purpose**: Categorizes payment methods for analysis
- **Key Features**:
  - Amount conversion (cents to dollars)
  - Payment categorization (card/bank/promotional)
  - Promotional flag identification
- **Tests**: Positive value checks, referential integrity

#### stg_customer_activity
- **Purpose**: Unified activity view joining customers, orders, and payments
- **Key Features**:
  - Customer lifecycle stage tracking
  - Cumulative value calculations
  - Complete transaction context

### 2. Marketing Analytics Models

#### customer_segmentation
- **Purpose**: RFM-based customer segmentation for targeted marketing
- **Methodology**:
  - **Recency**: Days since last order
  - **Frequency**: Total number of orders
  - **Monetary**: Total customer spend
- **Segments**:
  - Champions (R≥4, F≥4, M≥4)
  - Loyal Customers
  - Potential Loyalists
  - New Customers
  - At Risk
  - Can't Lose Them
  - Hibernating
- **Marketing Actions**: Automated recommendations per segment
- **Tests**: RFM score validation, segment classification

#### product_performance
- **Purpose**: Comprehensive product analytics
- **Key Metrics**:
  - Sales volume and revenue
  - Market penetration
  - Return rates
  - Customer acquisition per product
- **Classifications**:
  - ABC Analysis (Pareto principle)
  - Product status (Star/Problem/Cash Cow/Declining)
- **Features**:
  - Payment method preferences by product
  - Promotional dependency analysis
  - Cross-sell opportunity identification
- **Tests**: Positive values, ABC category validation

#### customer_cohorts
- **Purpose**: Cohort-based retention and LTV analysis
- **Methodology**:
  - Monthly cohort grouping
  - Retention rate tracking over time
  - Revenue per cohort member
- **Key Insights**:
  - Month-over-month retention curves
  - Cohort quality scoring
  - LTV progression
  - Top products by cohort
- **Retention Stages**:
  - Acquisition (Month 0)
  - Activation (Months 1-3)
  - Retention (Months 4-6)
  - Revenue (Months 7-12)
  - Referral (12+ months)
- **Tests**: Cohort uniqueness, retention rate bounds

#### marketing_attribution
- **Purpose**: Channel performance and attribution analysis
- **Attribution Models**:
  - First-touch attribution
  - Last-touch attribution
  - Channel efficiency scoring
- **Synthetic Channels**:
  - Promotional Campaign (coupon/gift card users)
  - Social Media (weekend purchases)
  - Email Marketing (high-value orders)
  - Organic (default)
- **Performance Metrics**:
  - Conversion rates
  - Customer acquisition cost proxy
  - Channel ROI estimation
- **Tests**: Channel validation, efficiency score checks

### 3. Core Business Models

#### customers
- **Purpose**: 360-degree customer view
- **Key Metrics**:
  - Customer lifetime value
  - Order frequency
  - First/last order dates
  - Total spend
- **Tests**: Uniqueness, positive CLV

#### orders
- **Purpose**: Transactional analytics
- **Features**:
  - Payment method breakdown
  - Order status tracking
  - Product association
- **Tests**: Referential integrity, amount validation

## Key Marketing Metrics

### Customer Metrics
- **Customer Lifetime Value (CLV)**: Total revenue per customer
- **Average Order Value (AOV)**: Revenue / Orders
- **Purchase Frequency**: Orders per customer
- **Retention Rate**: Active customers / Total customers by cohort
- **Churn Risk Score**: Based on recency and frequency decline

### Product Metrics
- **Market Penetration**: Customers purchased / Total customers
- **Return Rate**: Returned orders / Total orders
- **Revenue Concentration**: ABC analysis
- **Product Lifecycle Stage**: New/Growth/Mature/Declining

### Campaign Metrics
- **Channel Efficiency Score**: Composite of conversion, AOV, and acquisition
- **Attribution Value**: Revenue credited to each channel
- **Promotional Dependency**: Promotional revenue / Total revenue

## Testing Strategy

### Data Quality Tests
1. **Uniqueness Tests**: Primary keys in all models
2. **Not Null Tests**: Critical fields validation
3. **Referential Integrity**: Foreign key relationships
4. **Accepted Values**: Categorical field validation
5. **Custom Tests**:
   - `positive_value`: Revenue and count fields
   - `percentage_range`: Rates between 0-100
   - `recency_freshness`: Data freshness checks

### Test Coverage
- **Source Tests**: 15+ tests on raw data
- **Staging Tests**: 25+ tests on transformations
- **Model Tests**: 50+ tests on analytics models
- **Total Tests**: 90+ comprehensive tests

## Usage Examples

### Customer Segmentation Query
```sql
SELECT
    customer_segment,
    COUNT(*) as customer_count,
    AVG(total_revenue) as avg_revenue,
    AVG(total_orders) as avg_orders
FROM marketing.customer_segmentation
GROUP BY customer_segment
ORDER BY avg_revenue DESC;
```

### Product Performance Dashboard
```sql
SELECT
    product_name,
    revenue_rank,
    total_revenue,
    return_rate,
    abc_category,
    product_status,
    marketing_recommendation
FROM marketing.product_performance
WHERE abc_category = 'A'
ORDER BY revenue_rank;
```

### Cohort Retention Analysis
```sql
SELECT
    cohort_month,
    months_since_first_purchase,
    retention_rate,
    cohort_revenue,
    cohort_quality
FROM marketing.customer_cohorts
WHERE cohort_month >= '2024-01-01'
ORDER BY cohort_month, months_since_first_purchase;
```

### Channel Attribution Report
```sql
SELECT
    attribution_channel,
    total_conversions,
    total_revenue,
    channel_efficiency_score,
    channel_classification,
    optimization_recommendation
FROM marketing.marketing_attribution
ORDER BY total_revenue DESC;
```

## Implementation Guide

### Prerequisites
1. dbt version >= 1.0.0
2. Redshift database access
3. Source tables in `dbt_production` schema

### Setup Steps
```bash
# 1. Clone the repository
git clone <repository>
cd jaffle_shop

# 2. Install dependencies
dbt deps

# 3. Test connection
dbt debug

# 4. Run models
dbt run

# 5. Test data quality
dbt test

# 6. Generate documentation
dbt docs generate
dbt docs serve
```

### Maintenance

#### Daily Tasks
- Monitor test results
- Review data freshness
- Check for anomalies in key metrics

#### Weekly Tasks
- Review customer segment migrations
- Analyze product performance changes
- Update marketing recommendations

#### Monthly Tasks
- Cohort analysis review
- Attribution model validation
- Performance optimization

## Business Impact

### Expected Outcomes
1. **Improved Targeting**: 20-30% increase in campaign ROI through segmentation
2. **Reduced Churn**: 15% reduction through at-risk identification
3. **Product Optimization**: 10% revenue increase from ABC analysis
4. **Channel Efficiency**: 25% improvement in acquisition costs

### Key Stakeholders
- **Marketing Team**: Primary users for campaign planning
- **Product Team**: Product performance insights
- **Customer Success**: Retention and churn prevention
- **Executive Team**: Strategic decision support

## Technical Specifications

### Materialization Strategy
- **Staging Models**: Views for real-time updates
- **Analytics Models**: Tables for performance
- **Incremental Options**: Available for high-volume models

### Performance Considerations
- Indexes on join keys
- Partitioning by date for large tables
- Regular VACUUM and ANALYZE operations

### Data Governance
- PII handling in customer models
- Access control via database roles
- Audit logging for compliance

## Future Enhancements

### Planned Features
1. **Predictive Analytics**
   - Churn prediction models
   - CLV forecasting
   - Next best action recommendations

2. **Advanced Attribution**
   - Multi-touch attribution
   - Time-decay models
   - Data-driven attribution

3. **Real-time Analytics**
   - Streaming data integration
   - Near real-time dashboards
   - Alert systems

4. **External Data Integration**
   - Marketing campaign data
   - Website analytics
   - Social media metrics

## Support and Resources

### Documentation
- dbt Documentation: `dbt docs serve`
- Model Lineage: Available in dbt docs
- Column Descriptions: Comprehensive in schema.yml

### Troubleshooting
1. **Test Failures**: Check source data quality
2. **Performance Issues**: Review model complexity
3. **Data Discrepancies**: Validate business logic

### Contact
- **Technical Support**: Data Engineering Team
- **Business Questions**: Marketing Analytics Team
- **Feature Requests**: Submit via project repository

---

*Last Updated: 2024*
*Version: 1.0.0*
*Maintained by: Marketing Analytics Team*