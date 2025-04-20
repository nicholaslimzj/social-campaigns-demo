{{ config(materialized='table') }}

/*
Model: metrics_monthly_anomalies
Time Context: Monthly (rolling window analysis)

Description:
This model identifies anomalies in monthly performance metrics by analyzing deviations from expected
values based on recent trends and statistical thresholds. It focuses on month-to-month changes
and flags significant deviations that may require attention.

Key Features:
1. Calculates rolling statistics (mean, standard deviation) for key metrics using recent months
2. Identifies outliers based on z-score methodology comparing current month to previous months
3. Flags significant monthly deviations that may require attention
4. Provides context for each anomaly to aid investigation

Dashboard Usage:
- Monthly Anomaly Detection alerts
- Performance Monitoring section
- Risk Management visualizations
*/

-- First, we need to get company metrics by month
WITH company_monthly_metrics AS (
    SELECT 
        Company,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as monthly_ctr,
        SUM(Clicks * Acquisition_Cost) as total_spend,
        SUM(Clicks * Acquisition_Cost * ROI) as total_revenue
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Company, month
    ORDER BY Company, month
),

-- Calculate rolling statistics for each metric
rolling_stats AS (
    SELECT
        Company,
        month,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        total_clicks,
        total_impressions,
        monthly_ctr,
        total_spend,
        total_revenue,
        
        -- Rolling averages (3-month window excluding current month)
        AVG(avg_conversion_rate) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS conversion_rate_mean,
        
        AVG(avg_roi) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS roi_mean,
        
        AVG(avg_acquisition_cost) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS acquisition_cost_mean,
        
        AVG(total_clicks) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS clicks_mean,
        
        AVG(total_impressions) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS impressions_mean,
        
        AVG(monthly_ctr) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS ctr_mean,
        
        AVG(total_spend) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS spend_mean,
        
        AVG(total_revenue) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS revenue_mean,
        
        -- Rolling standard deviations
        STDDEV_POP(avg_conversion_rate) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS conversion_rate_std,
        
        STDDEV_POP(avg_roi) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS roi_std,
        
        STDDEV_POP(avg_acquisition_cost) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS acquisition_cost_std,
        
        STDDEV_POP(total_clicks) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS clicks_std,
        
        STDDEV_POP(total_impressions) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS impressions_std,
        
        STDDEV_POP(monthly_ctr) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS ctr_std,
        
        STDDEV_POP(total_spend) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS spend_std,
        
        STDDEV_POP(total_revenue) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS revenue_std
    FROM company_monthly_metrics
)

-- Calculate z-scores and flag anomalies
SELECT
    Company,
    month,
    avg_conversion_rate,
    conversion_rate_mean,
    conversion_rate_std,
    CASE
        WHEN conversion_rate_std IS NULL OR conversion_rate_std = 0 THEN NULL
        ELSE (avg_conversion_rate - conversion_rate_mean) / conversion_rate_std
    END AS conversion_rate_z,
    CASE
        WHEN conversion_rate_std IS NULL OR conversion_rate_std = 0 THEN 'normal'
        WHEN ABS((avg_conversion_rate - conversion_rate_mean) / conversion_rate_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS conversion_rate_anomaly,
    
    avg_roi,
    roi_mean,
    roi_std,
    CASE
        WHEN roi_std IS NULL OR roi_std = 0 THEN NULL
        ELSE (avg_roi - roi_mean) / roi_std
    END AS roi_z,
    CASE
        WHEN roi_std IS NULL OR roi_std = 0 THEN 'normal'
        WHEN ABS((avg_roi - roi_mean) / roi_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS roi_anomaly,
    
    avg_acquisition_cost,
    acquisition_cost_mean,
    acquisition_cost_std,
    CASE
        WHEN acquisition_cost_std IS NULL OR acquisition_cost_std = 0 THEN NULL
        ELSE (avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std
    END AS acquisition_cost_z,
    CASE
        WHEN acquisition_cost_std IS NULL OR acquisition_cost_std = 0 THEN 'normal'
        WHEN ABS((avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS acquisition_cost_anomaly,
    
    total_clicks,
    clicks_mean,
    clicks_std,
    CASE
        WHEN clicks_std IS NULL OR clicks_std = 0 THEN NULL
        ELSE (total_clicks - clicks_mean) / clicks_std
    END AS clicks_z,
    CASE
        WHEN clicks_std IS NULL OR clicks_std = 0 THEN 'normal'
        WHEN ABS((total_clicks - clicks_mean) / clicks_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS clicks_anomaly,
    
    total_impressions,
    impressions_mean,
    impressions_std,
    CASE
        WHEN impressions_std IS NULL OR impressions_std = 0 THEN NULL
        ELSE (total_impressions - impressions_mean) / impressions_std
    END AS impressions_z,
    CASE
        WHEN impressions_std IS NULL OR impressions_std = 0 THEN 'normal'
        WHEN ABS((total_impressions - impressions_mean) / impressions_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS impressions_anomaly,
    
    monthly_ctr,
    ctr_mean,
    ctr_std,
    CASE
        WHEN ctr_std IS NULL OR ctr_std = 0 THEN NULL
        ELSE (monthly_ctr - ctr_mean) / ctr_std
    END AS ctr_z,
    CASE
        WHEN ctr_std IS NULL OR ctr_std = 0 THEN 'normal'
        WHEN ABS((monthly_ctr - ctr_mean) / ctr_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS ctr_anomaly,
    
    -- Spend metrics
    total_spend,
    spend_mean,
    spend_std,
    CASE
        WHEN spend_std IS NULL OR spend_std = 0 THEN NULL
        ELSE (total_spend - spend_mean) / spend_std
    END AS spend_z,
    CASE
        WHEN spend_std IS NULL OR spend_std = 0 THEN 'normal'
        WHEN ABS((total_spend - spend_mean) / spend_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS spend_anomaly,
    
    -- Revenue metrics
    total_revenue,
    revenue_mean,
    revenue_std,
    CASE
        WHEN revenue_std IS NULL OR revenue_std = 0 THEN NULL
        ELSE (total_revenue - revenue_mean) / revenue_std
    END AS revenue_z,
    CASE
        WHEN revenue_std IS NULL OR revenue_std = 0 THEN 'normal'
        WHEN ABS((total_revenue - revenue_mean) / revenue_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS revenue_anomaly,
    
    -- Create a summary field for easy filtering
    CASE
        WHEN (
            (conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND ABS((avg_conversion_rate - conversion_rate_mean) / conversion_rate_std) > 2) OR
            (roi_std IS NOT NULL AND roi_std > 0 AND ABS((avg_roi - roi_mean) / roi_std) > 2) OR
            (acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND ABS((avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std) > 2) OR
            (clicks_std IS NOT NULL AND clicks_std > 0 AND ABS((total_clicks - clicks_mean) / clicks_std) > 2) OR
            (impressions_std IS NOT NULL AND impressions_std > 0 AND ABS((total_impressions - impressions_mean) / impressions_std) > 2) OR
            (ctr_std IS NOT NULL AND ctr_std > 0 AND ABS((monthly_ctr - ctr_mean) / ctr_std) > 2) OR
            (spend_std IS NOT NULL AND spend_std > 0 AND ABS((total_spend - spend_mean) / spend_std) > 2) OR
            (revenue_std IS NOT NULL AND revenue_std > 0 AND ABS((total_revenue - revenue_mean) / revenue_std) > 2)
        ) THEN TRUE
        ELSE FALSE
    END AS has_anomaly
FROM rolling_stats
ORDER BY Company, month
