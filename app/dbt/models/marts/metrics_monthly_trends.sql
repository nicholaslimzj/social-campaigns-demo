{{ config(materialized='table') }}

/*
Model: metrics_monthly_trends
Time Context: Monthly (trend analysis)

Description:
This model analyzes monthly performance trends to identify significant changes and patterns
in key metrics over time, enabling trend-based decision making.

Key Features:
1. Calculates moving averages to identify underlying trends
2. Detects significant changes in performance metrics
3. Classifies trend directions (improving, declining, stable)
4. Provides month-over-month change analysis

Dashboard Usage:
- Trend Analysis section
- Performance Change Alerts
- Month-over-Month Comparison charts
*/

WITH monthly_metrics AS (
    -- Get metrics by month for each company
    SELECT
        Company,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as monthly_ctr
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Company, EXTRACT(MONTH FROM CAST(Date AS DATE))
    ORDER BY Company, month
),

-- Calculate moving averages for trend detection
moving_avgs AS (
    SELECT
        Company,
        month,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        monthly_ctr,
        
        -- 3-month moving average (current month and 2 previous)
        AVG(avg_conversion_rate) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS conversion_rate_ma,
        
        -- Same for other metrics
        AVG(avg_roi) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS roi_ma,
        
        AVG(avg_acquisition_cost) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS acquisition_cost_ma,
        
        AVG(monthly_ctr) OVER (
            PARTITION BY Company
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS ctr_ma
    FROM monthly_metrics
),

-- Add lagged moving averages in a separate CTE to avoid nested window functions
lagged_avgs AS (
    SELECT
        Company,
        month,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        monthly_ctr,
        conversion_rate_ma,
        roi_ma,
        acquisition_cost_ma,
        ctr_ma,
        
        -- Get the moving average from 3 months ago
        LAG(conversion_rate_ma, 3) OVER (PARTITION BY Company ORDER BY month) AS prev_conversion_rate_ma,
        LAG(roi_ma, 3) OVER (PARTITION BY Company ORDER BY month) AS prev_roi_ma,
        LAG(acquisition_cost_ma, 3) OVER (PARTITION BY Company ORDER BY month) AS prev_acquisition_cost_ma,
        LAG(ctr_ma, 3) OVER (PARTITION BY Company ORDER BY month) AS prev_ctr_ma,
        
        -- Also get previous month's values to detect direction changes
        LAG(conversion_rate_ma, 1) OVER (PARTITION BY Company ORDER BY month) AS prev_month_conversion_rate_ma,
        LAG(roi_ma, 1) OVER (PARTITION BY Company ORDER BY month) AS prev_month_roi_ma,
        LAG(acquisition_cost_ma, 1) OVER (PARTITION BY Company ORDER BY month) AS prev_month_acquisition_cost_ma,
        LAG(ctr_ma, 1) OVER (PARTITION BY Company ORDER BY month) AS prev_month_ctr_ma
    FROM moving_avgs
)

-- Detect trend changes
SELECT
    Company,
    month,
    avg_conversion_rate,
    conversion_rate_ma,
    prev_conversion_rate_ma,
    
    -- Detect trend changes (1 = upward trend change, -1 = downward trend change, 0 = no change)
    CASE
        WHEN prev_conversion_rate_ma IS NULL THEN 0 -- Not enough history
        WHEN conversion_rate_ma > prev_conversion_rate_ma AND 
             prev_month_conversion_rate_ma <= prev_conversion_rate_ma
            THEN 1 -- Upward trend change
        WHEN conversion_rate_ma < prev_conversion_rate_ma AND 
             prev_month_conversion_rate_ma >= prev_conversion_rate_ma
            THEN -1 -- Downward trend change
        ELSE 0 -- No trend change
    END AS conversion_rate_trend_change,
    
    -- ROI trend change
    CASE
        WHEN prev_roi_ma IS NULL THEN 0 -- Not enough history
        WHEN roi_ma > prev_roi_ma AND 
             prev_month_roi_ma <= prev_roi_ma
            THEN 1 -- Upward trend change
        WHEN roi_ma < prev_roi_ma AND 
             prev_month_roi_ma >= prev_roi_ma
            THEN -1 -- Downward trend change
        ELSE 0 -- No trend change
    END AS roi_trend_change,
    
    -- Acquisition cost trend change (note: for cost, lower is better)
    CASE
        WHEN prev_acquisition_cost_ma IS NULL THEN 0 -- Not enough history
        WHEN acquisition_cost_ma < prev_acquisition_cost_ma AND 
             prev_month_acquisition_cost_ma >= prev_acquisition_cost_ma
            THEN 1 -- Downward (positive) trend change
        WHEN acquisition_cost_ma > prev_acquisition_cost_ma AND 
             prev_month_acquisition_cost_ma <= prev_acquisition_cost_ma
            THEN -1 -- Upward (negative) trend change
        ELSE 0 -- No trend change
    END AS acquisition_cost_trend_change,
    
    -- CTR trend change
    CASE
        WHEN prev_ctr_ma IS NULL THEN 0 -- Not enough history
        WHEN ctr_ma > prev_ctr_ma AND 
             prev_month_ctr_ma <= prev_ctr_ma
            THEN 1 -- Upward trend change
        WHEN ctr_ma < prev_ctr_ma AND 
             prev_month_ctr_ma >= prev_ctr_ma
            THEN -1 -- Downward trend change
        ELSE 0 -- No trend change
    END AS ctr_trend_change,
    
    -- Summary field for any trend change
    CASE
        WHEN prev_conversion_rate_ma IS NULL THEN FALSE -- Not enough history
        WHEN (conversion_rate_ma > prev_conversion_rate_ma AND 
              prev_month_conversion_rate_ma <= prev_conversion_rate_ma)
             OR
             (conversion_rate_ma < prev_conversion_rate_ma AND 
              prev_month_conversion_rate_ma >= prev_conversion_rate_ma)
             OR
             (roi_ma > prev_roi_ma AND 
              prev_month_roi_ma <= prev_roi_ma)
             OR
             (roi_ma < prev_roi_ma AND 
              prev_month_roi_ma >= prev_roi_ma)
             OR
             (acquisition_cost_ma < prev_acquisition_cost_ma AND 
              prev_month_acquisition_cost_ma >= prev_acquisition_cost_ma)
             OR
             (acquisition_cost_ma > prev_acquisition_cost_ma AND 
              prev_month_acquisition_cost_ma <= prev_acquisition_cost_ma)
             OR
             (ctr_ma > prev_ctr_ma AND 
              prev_month_ctr_ma <= prev_ctr_ma)
             OR
             (ctr_ma < prev_ctr_ma AND 
              prev_month_ctr_ma >= prev_ctr_ma)
            THEN TRUE -- Any trend change
        ELSE FALSE -- No trend change
    END AS has_trend_change
FROM lagged_avgs
WHERE month >= 4  -- Only include months with enough history for trend detection
ORDER BY Company, month