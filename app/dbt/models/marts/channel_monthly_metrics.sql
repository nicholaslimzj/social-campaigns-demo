{{ config(materialized='table') }}

/*
Model: channel_monthly_metrics
Time Context: Monthly (aggregated by company, channel and month)

Description:
This model aggregates campaign performance metrics by company, channel and month to provide
channel-specific time-based analysis of effectiveness throughout the year, following the hierarchical
dimension structure with Company as the primary dimension.

Key Features:
1. Company as the primary dimension
2. Channel_Used as the secondary dimension
3. Monthly time grain for granular trend analysis
4. Aggregates key performance metrics by company, channel and month
5. Calculates channel-specific month-over-month changes
6. Provides spend and revenue calculations
7. Enables channel performance tracking over time by company
8. Supports seasonal trend analysis by channel within each company

Dashboard Usage:
- Channel Conversion Trends line chart
- Channel Monthly Performance Trends charts
- Company-specific Channel Analysis visualizations
- Month-over-Month Performance Comparison
*/

WITH monthly_data AS (
    SELECT 
        Company,
        Channel_Used,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as monthly_ctr,
        SUM(Acquisition_Cost) as total_spend,
        SUM(Acquisition_Cost * (1 + ROI)) as total_revenue,
        -- Calculate company-channel-specific month-over-month changes
        LAG(AVG(ROI)) OVER (PARTITION BY Company, Channel_Used ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_roi,
        LAG(AVG(Conversion_Rate)) OVER (PARTITION BY Company, Channel_Used ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_conversion_rate,
        LAG(AVG(Acquisition_Cost)) OVER (PARTITION BY Company, Channel_Used ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_acquisition_cost,
        LAG(CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0)) OVER (PARTITION BY Company, Channel_Used ORDER BY EXTRACT(MONTH FROM CAST(Date AS DATE))) as prev_month_ctr
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Company, Channel_Used, EXTRACT(MONTH FROM CAST(Date AS DATE))
)

SELECT
    Company,
    Channel_Used,
    month,
    campaign_count,
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    monthly_ctr,
    total_clicks,
    total_impressions,
    total_spend,
    total_revenue,
    -- Calculate channel-specific month-over-month percentage changes
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
        ELSE (monthly_ctr - prev_month_ctr) / prev_month_ctr 
    END as ctr_vs_prev_month,
    -- Add channel count within company using a subquery
    (
        SELECT COUNT(DISTINCT Channel_Used)
        FROM {{ ref('stg_campaigns') }} sc
        WHERE sc.Company = monthly_data.Company
        AND EXTRACT(MONTH FROM CAST(sc.Date AS DATE)) = monthly_data.month
    ) as channel_count,
    -- Add channel share calculation within company
    (
        SELECT 
            CAST(SUM(CASE WHEN sc.Channel_Used = monthly_data.Channel_Used THEN sc.Clicks ELSE 0 END) AS FLOAT) / 
            NULLIF(SUM(sc.Clicks), 0)
        FROM {{ ref('stg_campaigns') }} sc
        WHERE sc.Company = monthly_data.Company
        AND EXTRACT(MONTH FROM CAST(sc.Date AS DATE)) = monthly_data.month
    ) as channel_share_clicks,
    -- Add channel efficiency metric (ROI per dollar spent)
    CASE
        WHEN total_spend > 0 THEN total_revenue / total_spend
        ELSE NULL
    END as efficiency_ratio
FROM monthly_data
ORDER BY Company, Channel_Used, month
