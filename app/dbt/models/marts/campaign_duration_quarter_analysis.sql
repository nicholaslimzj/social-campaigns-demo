{{ config(materialized='table') }}

/*
Model: campaign_duration_quarter_analysis
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model analyzes the relationship between campaign duration and performance metrics across
the current quarter to identify optimal campaign lengths for different dimensions (goals, channels).

Key Features:
1. Identifies optimal campaign durations across different dimensions
2. Analyzes performance trends across different duration ranges
3. Provides specific duration recommendations based on statistical analysis
4. Calculates the ROI impact of optimal vs. suboptimal durations

Dashboard Usage:
- Campaign Duration Impact visualization
- Campaign Length Optimization recommendations

Note: This model is part of the industry-standard models that provide cross-company insights.
*/

WITH 
-- First, determine the date range for the current quarter
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month
    FROM {{ ref('stg_campaigns') }}
),

-- First, bucket campaign durations into ranges for analysis
campaign_with_duration_buckets AS (
    SELECT
        *,
        CASE
            WHEN Duration <= 7 THEN '1-7 days'
            WHEN Duration <= 14 THEN '8-14 days'
            WHEN Duration <= 21 THEN '15-21 days'
            WHEN Duration <= 30 THEN '22-30 days'
            WHEN Duration <= 45 THEN '31-45 days'
            WHEN Duration <= 60 THEN '46-60 days'
            ELSE '60+ days'
        END AS duration_bucket,
        -- Also create a numeric bucket for analysis
        CASE
            WHEN Duration <= 7 THEN 1
            WHEN Duration <= 14 THEN 2
            WHEN Duration <= 21 THEN 3
            WHEN Duration <= 30 THEN 4
            WHEN Duration <= 45 THEN 5
            WHEN Duration <= 60 THEN 6
            ELSE 7
        END AS duration_bucket_num
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
),

-- Calculate performance metrics by duration bucket for each company (overall)
duration_performance_by_company AS (
    SELECT
        Company,
        duration_bucket,
        duration_bucket_num,
        MIN(Duration) AS min_duration,
        MAX(Duration) AS max_duration,
        AVG(Duration) AS avg_duration,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        -- Calculate ROI per day to normalize for duration
        AVG(ROI / NULLIF(Duration, 0)) AS avg_roi_per_day
    FROM campaign_with_duration_buckets
    GROUP BY Company, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 2  -- Only consider buckets with enough data
    ORDER BY Company, duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each company-goal combination
duration_performance_by_company_goal AS (
    SELECT
        Company,
        Campaign_Goal AS goal,
        duration_bucket,
        duration_bucket_num,
        MIN(Duration) AS min_duration,
        MAX(Duration) AS max_duration,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        AVG(ROI / NULLIF(Duration, 0)) AS avg_roi_per_day
    FROM campaign_with_duration_buckets
    GROUP BY Company, Campaign_Goal, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 2  -- Only consider buckets with enough data
    ORDER BY Company, Campaign_Goal, duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each company-channel combination
duration_performance_by_company_channel AS (
    SELECT
        Company,
        Channel_Used AS channel,
        duration_bucket,
        duration_bucket_num,
        MIN(Duration) AS min_duration,
        MAX(Duration) AS max_duration,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        AVG(ROI / NULLIF(Duration, 0)) AS avg_roi_per_day
    FROM campaign_with_duration_buckets
    GROUP BY Company, Channel_Used, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 2  -- Only consider buckets with enough data
    ORDER BY Company, Channel_Used, duration_bucket_num
),

-- Find optimal duration bucket for each company based on ROI
optimal_durations_by_company AS (
    SELECT
        Company,
        'Company' AS dimension,
        Company AS category,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY Company
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY Company
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY Company
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY Company
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY Company
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY Company
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_company
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Company ORDER BY avg_roi DESC) = 1
),

-- Find optimal duration bucket for each company-goal combination
optimal_durations_by_company_goal AS (
    SELECT
        Company,
        'Goal' AS dimension,
        goal AS category,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY Company, goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY Company, goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY Company, goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY Company, goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY Company, goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY Company, goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_company_goal
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Company, goal ORDER BY avg_roi DESC) = 1
),

-- Find optimal duration bucket for each company-channel combination
optimal_durations_by_company_channel AS (
    SELECT
        Company,
        'Channel' AS dimension,
        channel AS category,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY Company, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY Company, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY Company, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY Company, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY Company, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY Company, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_company_channel
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Company, channel ORDER BY avg_roi DESC) = 1
),

-- Combine all optimal durations
all_optimal_durations AS (
    SELECT Company, dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_company
    
    UNION ALL
    
    SELECT Company, dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_company_goal
    
    UNION ALL
    
    SELECT Company, dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_company_channel
),

-- Calculate performance by exact duration days (for scatter plot)
exact_duration_performance AS (
    SELECT
        Company,
        Duration,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        AVG(ROI / NULLIF(Duration, 0)) AS avg_roi_per_day
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
    GROUP BY Company, Duration
    HAVING COUNT(*) >= 2  -- Only consider durations with enough data
    ORDER BY Company, Duration
),

-- Prepare the raw duration performance data for heatmap visualization
company_duration_heatmap AS (
    SELECT
        'company_duration_heatmap' AS analysis_type,
        Company,
        'Company' AS dimension,
        Company AS category,
        duration_bucket,
        duration_bucket_num,
        min_duration,
        max_duration,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        avg_ctr,
        avg_roi_per_day,
        -- Calculate ROI impact compared to company average
        avg_roi - (SELECT AVG(avg_roi) FROM duration_performance_by_company c2 WHERE c2.Company = duration_performance_by_company.Company) AS roi_impact,
        -- Calculate ROI impact percentage
        CASE 
            WHEN (SELECT AVG(avg_roi) FROM duration_performance_by_company c2 WHERE c2.Company = duration_performance_by_company.Company) = 0 THEN 0
            ELSE (avg_roi / (SELECT AVG(avg_roi) FROM duration_performance_by_company c2 WHERE c2.Company = duration_performance_by_company.Company) - 1) * 100 
        END AS roi_impact_pct
    FROM duration_performance_by_company
),

goal_duration_heatmap AS (
    SELECT
        'goal_duration_heatmap' AS analysis_type,
        Company,
        'Goal' AS dimension,
        goal AS category,
        duration_bucket,
        duration_bucket_num,
        min_duration,
        max_duration,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        avg_ctr,
        avg_roi_per_day,
        -- Calculate ROI impact compared to goal average
        avg_roi - (SELECT AVG(avg_roi) FROM duration_performance_by_company_goal g2 
                   WHERE g2.Company = duration_performance_by_company_goal.Company 
                   AND g2.goal = duration_performance_by_company_goal.goal) AS roi_impact,
        -- Calculate ROI impact percentage
        CASE 
            WHEN (SELECT AVG(avg_roi) FROM duration_performance_by_company_goal g2 
                  WHERE g2.Company = duration_performance_by_company_goal.Company 
                  AND g2.goal = duration_performance_by_company_goal.goal) = 0 THEN 0
            ELSE (avg_roi / (SELECT AVG(avg_roi) FROM duration_performance_by_company_goal g2 
                            WHERE g2.Company = duration_performance_by_company_goal.Company 
                            AND g2.goal = duration_performance_by_company_goal.goal) - 1) * 100 
        END AS roi_impact_pct
    FROM duration_performance_by_company_goal
),

channel_duration_heatmap AS (
    SELECT
        'channel_duration_heatmap' AS analysis_type,
        Company,
        'Channel' AS dimension,
        channel AS category,
        duration_bucket,
        duration_bucket_num,
        min_duration,
        max_duration,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        avg_ctr,
        avg_roi_per_day,
        -- Calculate ROI impact compared to channel average
        avg_roi - (SELECT AVG(avg_roi) FROM duration_performance_by_company_channel c2 
                   WHERE c2.Company = duration_performance_by_company_channel.Company 
                   AND c2.channel = duration_performance_by_company_channel.channel) AS roi_impact,
        -- Calculate ROI impact percentage
        CASE 
            WHEN (SELECT AVG(avg_roi) FROM duration_performance_by_company_channel c2 
                  WHERE c2.Company = duration_performance_by_company_channel.Company 
                  AND c2.channel = duration_performance_by_company_channel.channel) = 0 THEN 0
            ELSE (avg_roi / (SELECT AVG(avg_roi) FROM duration_performance_by_company_channel c2 
                            WHERE c2.Company = duration_performance_by_company_channel.Company 
                            AND c2.channel = duration_performance_by_company_channel.channel) - 1) * 100 
        END AS roi_impact_pct
    FROM duration_performance_by_company_channel
)

-- Final output combining all analyses
SELECT
    'optimal_durations' AS analysis_type,
    Company,
    dimension,
    category,
    optimal_duration_bucket,
    optimal_min_duration,
    optimal_max_duration,
    optimal_roi,
    optimal_conversion_rate,
    optimal_roi_per_day,
    -- Create a formatted optimal duration range string
    optimal_min_duration || '-' || optimal_max_duration || ' days' AS optimal_duration_range,
    NULL AS Duration,
    NULL AS campaign_count,
    NULL AS avg_conversion_rate,
    NULL AS avg_roi,
    NULL AS avg_acquisition_cost,
    NULL AS avg_ctr,
    NULL AS avg_roi_per_day,
    NULL AS roi_impact,
    NULL AS roi_impact_pct,
    NULL AS duration_bucket_num
FROM all_optimal_durations

UNION ALL

-- Add company duration heatmap data
SELECT
    analysis_type,
    Company,
    dimension,
    category,
    NULL AS optimal_duration_bucket,
    min_duration AS optimal_min_duration,
    max_duration AS optimal_max_duration,
    NULL AS optimal_roi,
    NULL AS optimal_conversion_rate,
    NULL AS optimal_roi_per_day,
    NULL AS optimal_duration_range,
    NULL AS Duration,
    campaign_count,
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    avg_ctr,
    avg_roi_per_day,
    roi_impact,
    roi_impact_pct,
    duration_bucket_num
FROM company_duration_heatmap

UNION ALL

-- Add goal duration heatmap data
SELECT
    analysis_type,
    Company,
    dimension,
    category,
    NULL AS optimal_duration_bucket,
    min_duration AS optimal_min_duration,
    max_duration AS optimal_max_duration,
    NULL AS optimal_roi,
    NULL AS optimal_conversion_rate,
    NULL AS optimal_roi_per_day,
    NULL AS optimal_duration_range,
    NULL AS Duration,
    campaign_count,
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    avg_ctr,
    avg_roi_per_day,
    roi_impact,
    roi_impact_pct,
    duration_bucket_num
FROM goal_duration_heatmap

UNION ALL

-- Add channel duration heatmap data
SELECT
    analysis_type,
    Company,
    dimension,
    category,
    NULL AS optimal_duration_bucket,
    min_duration AS optimal_min_duration,
    max_duration AS optimal_max_duration,
    NULL AS optimal_roi,
    NULL AS optimal_conversion_rate,
    NULL AS optimal_roi_per_day,
    NULL AS optimal_duration_range,
    NULL AS Duration,
    campaign_count,
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    avg_ctr,
    avg_roi_per_day,
    roi_impact,
    roi_impact_pct,
    duration_bucket_num
FROM channel_duration_heatmap

UNION ALL

-- Add exact duration performance data
SELECT
    'exact_duration_performance' AS analysis_type,
    Company,
    NULL AS dimension,
    NULL AS category,
    NULL AS optimal_duration_bucket,
    NULL AS optimal_min_duration,
    NULL AS optimal_max_duration,
    NULL AS optimal_roi,
    NULL AS optimal_conversion_rate,
    NULL AS optimal_roi_per_day,
    NULL AS optimal_duration_range,
    Duration,
    campaign_count,
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    avg_ctr,
    avg_roi_per_day,
    NULL AS roi_impact,
    NULL AS roi_impact_pct,
    NULL AS duration_bucket_num
FROM exact_duration_performance
