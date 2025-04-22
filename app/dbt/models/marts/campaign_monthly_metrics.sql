{{ config(materialized='table') }}

/*
Model: campaign_monthly_metrics
Time Context: Monthly (aggregated by company and month)

Description:
This model aggregates campaign performance metrics by company and month to provide
company-specific time-based analysis of campaign effectiveness throughout the year.

Key Features:
1. Aggregates key performance metrics by company and month
2. Calculates company-specific month-over-month changes
3. Provides spend and revenue calculations
4. Enables company performance tracking over time
5. Supports seasonal trend analysis by company
6. Serves as foundation for company-specific time series visualizations

Dashboard Usage:
- Company Monthly Performance Trends charts
- Company-specific Seasonal Analysis visualizations
- Month-over-Month Performance Comparison
- Company Revenue and Spend Tracking
*/

WITH date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month
    FROM {{ ref('stg_campaigns') }}
),

monthly_data AS (
    SELECT 
        Company,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
        {{ agg_metrics() }},
        -- Sum of acquisition costs for accurate spend calculation
        SUM(Acquisition_Cost) as total_acquisition_cost,
        -- Calculate company-specific month-over-month changes
        LAG(avg_roi) OVER (PARTITION BY Company ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_roi,
        LAG(avg_conversion_rate) OVER (PARTITION BY Company ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_conversion_rate,
        LAG(avg_acquisition_cost) OVER (PARTITION BY Company ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_acquisition_cost,
        LAG(overall_ctr) OVER (PARTITION BY Company ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_ctr
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Company, EXTRACT(MONTH FROM CAST(Date AS DATE))
)

SELECT
    Company,
    month,
    campaign_count,
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    overall_ctr as avg_ctr,
    total_clicks,
    total_impressions,
    -- Calculate company-specific spend and revenue
    total_acquisition_cost as total_spend,
    total_acquisition_cost * avg_roi as total_revenue,
    -- Calculate company-specific month-over-month percentage changes
    CASE 
        WHEN prev_month_roi IS NULL THEN NULL
        WHEN prev_month_roi = 0 THEN NULL
        ELSE (avg_roi - prev_month_roi) / prev_month_roi 
    END as roi_vs_prev_month,
    CASE 
        WHEN prev_month_conversion_rate IS NULL THEN NULL
        WHEN prev_month_conversion_rate = 0 THEN NULL
        ELSE (avg_conversion_rate - prev_month_conversion_rate) / prev_month_conversion_rate 
    END as conversion_rate_vs_prev_month,
    CASE 
        WHEN prev_month_acquisition_cost IS NULL THEN NULL
        WHEN prev_month_acquisition_cost = 0 THEN NULL
        ELSE (prev_month_acquisition_cost - avg_acquisition_cost) / prev_month_acquisition_cost -- Note: for cost, lower is better
    END as acquisition_cost_vs_prev_month,
    CASE 
        WHEN prev_month_ctr IS NULL THEN NULL
        WHEN prev_month_ctr = 0 THEN NULL
        ELSE (avg_ctr - prev_month_ctr) / prev_month_ctr 
    END as ctr_vs_prev_month
FROM monthly_data
ORDER BY Company, month
