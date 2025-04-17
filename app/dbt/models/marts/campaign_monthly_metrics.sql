{{ config(materialized='table') }}

/*
Model: campaign_monthly_metrics
Time Context: Monthly (aggregated by month)

Description:
This model aggregates campaign performance metrics by month to provide
time-based analysis of campaign effectiveness throughout the year.

Key Features:
1. Aggregates key performance metrics by month
2. Enables month-over-month comparison
3. Supports seasonal trend analysis
4. Provides a foundation for time series visualizations

Dashboard Usage:
- Monthly Performance Trends charts
- Seasonal Analysis visualizations
*/

SELECT 
    EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY month
ORDER BY month
