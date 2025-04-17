{{ config(materialized='table') }}

/*
Model: campaign_duration_historical_analysis
Time Context: Historical (all available data)

Description:
This model analyzes the relationship between campaign duration and performance metrics across
all historical data to identify optimal campaign lengths for different combinations.

Key Features:
1. Identifies optimal campaign durations for different combinations of goals, segments, and channels
2. Analyzes performance trends across different duration ranges
3. Provides specific duration recommendations based on statistical analysis
4. Calculates the ROI impact of optimal vs. suboptimal durations

Dashboard Usage:
- Campaign Duration Impact visualization
- Campaign Length Optimization recommendations

Note: The approach uses statistical analysis techniques that can be implemented in SQL
without requiring external ML libraries.
*/

WITH 
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
),

-- Calculate performance metrics by duration bucket (overall)
duration_performance_overall AS (
    SELECT
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
    GROUP BY duration_bucket, duration_bucket_num
    ORDER BY duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each goal
duration_performance_by_goal AS (
    SELECT
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
    GROUP BY Campaign_Goal, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 3  -- Only consider buckets with enough data
    ORDER BY Campaign_Goal, duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each segment
duration_performance_by_segment AS (
    SELECT
        Customer_Segment AS segment,
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
    GROUP BY Customer_Segment, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 3  -- Only consider buckets with enough data
    ORDER BY Customer_Segment, duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each channel
duration_performance_by_channel AS (
    SELECT
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
    GROUP BY Channel_Used, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 3  -- Only consider buckets with enough data
    ORDER BY Channel_Used, duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each goal-segment combination
duration_performance_by_goal_segment AS (
    SELECT
        Campaign_Goal AS goal,
        Customer_Segment AS segment,
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
    GROUP BY Campaign_Goal, Customer_Segment, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 3  -- Only consider buckets with enough data
    ORDER BY Campaign_Goal, Customer_Segment, duration_bucket_num
),

-- Calculate performance metrics by duration bucket for each goal-channel combination
duration_performance_by_goal_channel AS (
    SELECT
        Campaign_Goal AS goal,
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
    GROUP BY Campaign_Goal, Channel_Used, duration_bucket, duration_bucket_num
    HAVING COUNT(*) >= 3  -- Only consider buckets with enough data
    ORDER BY Campaign_Goal, Channel_Used, duration_bucket_num
),

-- Find optimal duration bucket for each dimension based on ROI
optimal_durations_overall AS (
    SELECT
        'Overall' AS dimension,
        'All' AS category,
        FIRST_VALUE(duration_bucket) OVER (
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_overall
    LIMIT 1
),

-- Find optimal duration bucket for each goal
optimal_durations_by_goal AS (
    SELECT
        'Goal' AS dimension,
        goal AS category,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY goal
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_goal
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_roi DESC) = 1
),

-- Find optimal duration bucket for each segment
optimal_durations_by_segment AS (
    SELECT
        'Segment' AS dimension,
        segment AS category,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_segment
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_roi DESC) = 1
),

-- Find optimal duration bucket for each channel
optimal_durations_by_channel AS (
    SELECT
        'Channel' AS dimension,
        channel AS category,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_channel
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) = 1
),

-- Find optimal duration bucket for each goal-segment combination
optimal_durations_by_goal_segment AS (
    SELECT
        'Goal-Segment' AS dimension,
        goal || ' - ' || segment AS category,
        goal,
        segment,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY goal, segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY goal, segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY goal, segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY goal, segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY goal, segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY goal, segment
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_goal_segment
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goal, segment ORDER BY avg_roi DESC) = 1
),

-- Find optimal duration bucket for each goal-channel combination
optimal_durations_by_goal_channel AS (
    SELECT
        'Goal-Channel' AS dimension,
        goal || ' - ' || channel AS category,
        goal,
        channel,
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY goal, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY goal, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY goal, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration,
        FIRST_VALUE(avg_roi) OVER (
            PARTITION BY goal, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi,
        FIRST_VALUE(avg_conversion_rate) OVER (
            PARTITION BY goal, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_conversion_rate,
        FIRST_VALUE(avg_roi_per_day) OVER (
            PARTITION BY goal, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_roi_per_day
    FROM duration_performance_by_goal_channel
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goal, channel ORDER BY avg_roi DESC) = 1
),

-- Combine all optimal durations
all_optimal_durations AS (
    SELECT * FROM optimal_durations_overall
    UNION ALL
    SELECT dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_goal
    UNION ALL
    SELECT dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_segment
    UNION ALL
    SELECT dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_channel
    UNION ALL
    SELECT dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_goal_segment
    UNION ALL
    SELECT dimension, category, optimal_duration_bucket, optimal_min_duration, optimal_max_duration, 
           optimal_roi, optimal_conversion_rate, optimal_roi_per_day
    FROM optimal_durations_by_goal_channel
),

-- Calculate performance by exact duration days (for scatter plot)
exact_duration_performance AS (
    SELECT
        Duration,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        AVG(ROI / NULLIF(Duration, 0)) AS avg_roi_per_day
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Duration
    HAVING COUNT(*) >= 3  -- Only consider durations with enough data
    ORDER BY Duration
),

-- Calculate the ROI impact of optimal vs. suboptimal durations
roi_impact_analysis AS (
    SELECT
        cwdb.Campaign_Goal AS goal,
        cwdb.Customer_Segment AS segment,
        cwdb.Channel_Used AS channel,
        cwdb.duration_bucket,
        odgs.optimal_duration_bucket AS optimal_bucket_for_goal_segment,
        odgc.optimal_duration_bucket AS optimal_bucket_for_goal_channel,
        AVG(cwdb.ROI) AS actual_roi,
        odgs.optimal_roi AS optimal_roi_for_goal_segment,
        odgc.optimal_roi AS optimal_roi_for_goal_channel,
        -- Calculate potential ROI improvement
        odgs.optimal_roi - AVG(cwdb.ROI) AS potential_roi_improvement_goal_segment,
        odgc.optimal_roi - AVG(cwdb.ROI) AS potential_roi_improvement_goal_channel,
        -- Calculate percentage improvement
        (odgs.optimal_roi / NULLIF(AVG(cwdb.ROI), 0) - 1) * 100 AS pct_improvement_goal_segment,
        (odgc.optimal_roi / NULLIF(AVG(cwdb.ROI), 0) - 1) * 100 AS pct_improvement_goal_channel,
        -- Flag if using optimal duration
        CASE 
            WHEN cwdb.duration_bucket = odgs.optimal_duration_bucket THEN TRUE
            ELSE FALSE
        END AS is_using_optimal_duration_goal_segment,
        CASE 
            WHEN cwdb.duration_bucket = odgc.optimal_duration_bucket THEN TRUE
            ELSE FALSE
        END AS is_using_optimal_duration_goal_channel,
        COUNT(*) AS campaign_count
    FROM campaign_with_duration_buckets cwdb
    LEFT JOIN optimal_durations_by_goal_segment odgs 
        ON cwdb.Campaign_Goal = odgs.goal AND cwdb.Customer_Segment = odgs.segment
    LEFT JOIN optimal_durations_by_goal_channel odgc
        ON cwdb.Campaign_Goal = odgc.goal AND cwdb.Channel_Used = odgc.channel
    GROUP BY 
        cwdb.Campaign_Goal,
        cwdb.Customer_Segment,
        cwdb.Channel_Used,
        cwdb.duration_bucket,
        odgs.optimal_duration_bucket,
        odgc.optimal_duration_bucket,
        odgs.optimal_roi,
        odgc.optimal_roi
)

-- Final output combining all analyses
SELECT
    'optimal_durations' AS analysis_type,
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
    NULL AS goal,
    NULL AS segment,
    NULL AS channel,
    NULL AS duration_bucket,
    NULL AS optimal_bucket_for_goal_segment,
    NULL AS optimal_bucket_for_goal_channel,
    NULL AS actual_roi,
    NULL AS optimal_roi_for_goal_segment,
    NULL AS optimal_roi_for_goal_channel,
    NULL AS potential_roi_improvement_goal_segment,
    NULL AS potential_roi_improvement_goal_channel,
    NULL AS pct_improvement_goal_segment,
    NULL AS pct_improvement_goal_channel,
    NULL AS is_using_optimal_duration_goal_segment,
    NULL AS is_using_optimal_duration_goal_channel
FROM all_optimal_durations

UNION ALL

SELECT
    'exact_duration_performance' AS analysis_type,
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
    NULL AS goal,
    NULL AS segment,
    NULL AS channel,
    NULL AS duration_bucket,
    NULL AS optimal_bucket_for_goal_segment,
    NULL AS optimal_bucket_for_goal_channel,
    NULL AS actual_roi,
    NULL AS optimal_roi_for_goal_segment,
    NULL AS optimal_roi_for_goal_channel,
    NULL AS potential_roi_improvement_goal_segment,
    NULL AS potential_roi_improvement_goal_channel,
    NULL AS pct_improvement_goal_segment,
    NULL AS pct_improvement_goal_channel,
    NULL AS is_using_optimal_duration_goal_segment,
    NULL AS is_using_optimal_duration_goal_channel
FROM exact_duration_performance

UNION ALL

SELECT
    'roi_impact_analysis' AS analysis_type,
    NULL AS dimension,
    NULL AS category,
    NULL AS optimal_duration_bucket,
    NULL AS optimal_min_duration,
    NULL AS optimal_max_duration,
    NULL AS optimal_roi,
    NULL AS optimal_conversion_rate,
    NULL AS optimal_roi_per_day,
    NULL AS optimal_duration_range,
    NULL AS Duration,
    campaign_count,
    NULL AS avg_conversion_rate,
    NULL AS avg_roi,
    NULL AS avg_acquisition_cost,
    NULL AS avg_ctr,
    NULL AS avg_roi_per_day,
    goal,
    segment,
    channel,
    duration_bucket,
    optimal_bucket_for_goal_segment,
    optimal_bucket_for_goal_channel,
    actual_roi,
    optimal_roi_for_goal_segment,
    optimal_roi_for_goal_channel,
    potential_roi_improvement_goal_segment,
    potential_roi_improvement_goal_channel,
    pct_improvement_goal_segment,
    pct_improvement_goal_channel,
    is_using_optimal_duration_goal_segment,
    is_using_optimal_duration_goal_channel
FROM roi_impact_analysis
