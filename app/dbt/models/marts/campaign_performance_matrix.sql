{{ config(materialized='table') }}

/*
This model creates a performance matrix that cross-tabulates campaign goals against customer segments.
It provides a comprehensive view of which goal-segment combinations perform best across different metrics:

1. Conversion Rate: How effectively campaigns convert for each goal-segment combination
2. ROI: Return on investment for each goal-segment combination
3. Acquisition Cost: Cost efficiency for each goal-segment combination
4. CTR: Engagement levels for each goal-segment combination

The matrix helps identify the most effective targeting strategies and highlights
opportunities for optimization across the campaign portfolio.
*/

WITH 
-- First, get a list of all distinct campaign goals and segments
distinct_goals AS (
    SELECT DISTINCT Campaign_Goal AS goal
    FROM {{ ref('stg_campaigns') }}
),

distinct_segments AS (
    SELECT DISTINCT Customer_Segment AS segment
    FROM {{ ref('stg_campaigns') }}
),

-- Calculate performance metrics for each goal-segment combination
goal_segment_performance AS (
    SELECT
        Campaign_Goal AS goal,
        Customer_Segment AS segment,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Campaign_Goal, Customer_Segment
),

-- Calculate global averages for comparison
global_metrics AS (
    SELECT
        AVG(Conversion_Rate) AS global_avg_conversion_rate,
        AVG(ROI) AS global_avg_roi,
        AVG(Acquisition_Cost) AS global_avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS global_avg_ctr
    FROM {{ ref('stg_campaigns') }}
),

-- Calculate goal-specific averages
goal_metrics AS (
    SELECT
        Campaign_Goal AS goal,
        AVG(Conversion_Rate) AS goal_avg_conversion_rate,
        AVG(ROI) AS goal_avg_roi,
        AVG(Acquisition_Cost) AS goal_avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS goal_avg_ctr
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Campaign_Goal
),

-- Calculate segment-specific averages
segment_metrics AS (
    SELECT
        Customer_Segment AS segment,
        AVG(Conversion_Rate) AS segment_avg_conversion_rate,
        AVG(ROI) AS segment_avg_roi,
        AVG(Acquisition_Cost) AS segment_avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS segment_avg_ctr
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Customer_Segment
),

-- Create a complete matrix with all possible goal-segment combinations
complete_matrix AS (
    SELECT
        dg.goal,
        ds.segment
    FROM distinct_goals dg
    CROSS JOIN distinct_segments ds
),

-- Join performance data to the complete matrix
matrix_with_performance AS (
    SELECT
        cm.goal,
        cm.segment,
        COALESCE(gsp.campaign_count, 0) AS campaign_count,
        gsp.avg_conversion_rate,
        gsp.avg_roi,
        gsp.avg_acquisition_cost,
        gsp.avg_ctr,
        gsp.total_clicks,
        gsp.total_impressions,
        
        -- Join goal and segment averages
        gm.goal_avg_conversion_rate,
        gm.goal_avg_roi,
        gm.goal_avg_acquisition_cost,
        gm.goal_avg_ctr,
        
        sm.segment_avg_conversion_rate,
        sm.segment_avg_roi,
        sm.segment_avg_acquisition_cost,
        sm.segment_avg_ctr,
        
        -- Global averages
        g_metrics.global_avg_conversion_rate,
        g_metrics.global_avg_roi,
        g_metrics.global_avg_acquisition_cost,
        g_metrics.global_avg_ctr
    FROM complete_matrix cm
    LEFT JOIN goal_segment_performance gsp ON cm.goal = gsp.goal AND cm.segment = gsp.segment
    LEFT JOIN goal_metrics gm ON cm.goal = gm.goal
    LEFT JOIN segment_metrics sm ON cm.segment = sm.segment
    CROSS JOIN global_metrics AS g_metrics
),

-- Calculate relative performance metrics
matrix_with_relative_performance AS (
    SELECT
        *,
        -- Calculate performance relative to goal average
        CASE 
            WHEN avg_conversion_rate IS NOT NULL THEN 
                avg_conversion_rate / NULLIF(goal_avg_conversion_rate, 0) - 1
            ELSE NULL
        END AS conversion_rate_vs_goal_avg,
        
        CASE 
            WHEN avg_roi IS NOT NULL THEN 
                avg_roi / NULLIF(goal_avg_roi, 0) - 1
            ELSE NULL
        END AS roi_vs_goal_avg,
        
        CASE 
            WHEN avg_acquisition_cost IS NOT NULL THEN 
                goal_avg_acquisition_cost / NULLIF(avg_acquisition_cost, 0) - 1
            ELSE NULL
        END AS acquisition_cost_vs_goal_avg,
        
        CASE 
            WHEN avg_ctr IS NOT NULL THEN 
                avg_ctr / NULLIF(goal_avg_ctr, 0) - 1
            ELSE NULL
        END AS ctr_vs_goal_avg,
        
        -- Calculate performance relative to segment average
        CASE 
            WHEN avg_conversion_rate IS NOT NULL THEN 
                avg_conversion_rate / NULLIF(segment_avg_conversion_rate, 0) - 1
            ELSE NULL
        END AS conversion_rate_vs_segment_avg,
        
        CASE 
            WHEN avg_roi IS NOT NULL THEN 
                avg_roi / NULLIF(segment_avg_roi, 0) - 1
            ELSE NULL
        END AS roi_vs_segment_avg,
        
        CASE 
            WHEN avg_acquisition_cost IS NOT NULL THEN 
                segment_avg_acquisition_cost / NULLIF(avg_acquisition_cost, 0) - 1
            ELSE NULL
        END AS acquisition_cost_vs_segment_avg,
        
        CASE 
            WHEN avg_ctr IS NOT NULL THEN 
                avg_ctr / NULLIF(segment_avg_ctr, 0) - 1
            ELSE NULL
        END AS ctr_vs_segment_avg,
        
        -- Calculate performance relative to global average
        CASE 
            WHEN avg_conversion_rate IS NOT NULL THEN 
                avg_conversion_rate / NULLIF(global_avg_conversion_rate, 0) - 1
            ELSE NULL
        END AS conversion_rate_vs_global_avg,
        
        CASE 
            WHEN avg_roi IS NOT NULL THEN 
                avg_roi / NULLIF(global_avg_roi, 0) - 1
            ELSE NULL
        END AS roi_vs_global_avg,
        
        CASE 
            WHEN avg_acquisition_cost IS NOT NULL THEN 
                global_avg_acquisition_cost / NULLIF(avg_acquisition_cost, 0) - 1
            ELSE NULL
        END AS acquisition_cost_vs_global_avg,
        
        CASE 
            WHEN avg_ctr IS NOT NULL THEN 
                avg_ctr / NULLIF(global_avg_ctr, 0) - 1
            ELSE NULL
        END AS ctr_vs_global_avg
    FROM matrix_with_performance
),

-- Rank combinations by different metrics
ranked_matrix AS (
    SELECT
        *,
        -- Rank by conversion rate (higher is better)
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate DESC NULLS LAST) AS conversion_rate_rank,
        ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_conversion_rate DESC NULLS LAST) AS conversion_rate_rank_within_goal,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_conversion_rate DESC NULLS LAST) AS conversion_rate_rank_within_segment,
        
        -- Rank by ROI (higher is better)
        ROW_NUMBER() OVER (ORDER BY avg_roi DESC NULLS LAST) AS roi_rank,
        ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_roi DESC NULLS LAST) AS roi_rank_within_goal,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_roi DESC NULLS LAST) AS roi_rank_within_segment,
        
        -- Rank by acquisition cost (lower is better)
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost ASC NULLS LAST) AS acquisition_cost_rank,
        ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_acquisition_cost ASC NULLS LAST) AS acquisition_cost_rank_within_goal,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_acquisition_cost ASC NULLS LAST) AS acquisition_cost_rank_within_segment,
        
        -- Rank by CTR (higher is better)
        ROW_NUMBER() OVER (ORDER BY avg_ctr DESC NULLS LAST) AS ctr_rank,
        ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_ctr DESC NULLS LAST) AS ctr_rank_within_goal,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_ctr DESC NULLS LAST) AS ctr_rank_within_segment,
        
        -- Count total combinations for percentile calculations
        COUNT(*) OVER () AS total_combinations,
        COUNT(*) OVER (PARTITION BY goal) AS total_combinations_per_goal,
        COUNT(*) OVER (PARTITION BY segment) AS total_combinations_per_segment
    FROM matrix_with_relative_performance
),

-- Calculate a composite score for each combination
matrix_with_composite_score AS (
    SELECT
        *,
        -- Create normalized scores (0-1 scale, higher is better)
        -- For conversion rate: higher is better
        CASE 
            WHEN avg_conversion_rate IS NOT NULL THEN 
                1 - ((conversion_rate_rank - 1) / NULLIF(total_combinations - 1, 1))
            ELSE 0
        END AS conversion_rate_score,
        
        -- For ROI: higher is better
        CASE 
            WHEN avg_roi IS NOT NULL THEN 
                1 - ((roi_rank - 1) / NULLIF(total_combinations - 1, 1))
            ELSE 0
        END AS roi_score,
        
        -- For acquisition cost: lower is better
        CASE 
            WHEN avg_acquisition_cost IS NOT NULL THEN 
                1 - ((acquisition_cost_rank - 1) / NULLIF(total_combinations - 1, 1))
            ELSE 0
        END AS acquisition_cost_score,
        
        -- For CTR: higher is better
        CASE 
            WHEN avg_ctr IS NOT NULL THEN 
                1 - ((ctr_rank - 1) / NULLIF(total_combinations - 1, 1))
            ELSE 0
        END AS ctr_score
    FROM ranked_matrix
),

-- Calculate final composite score with weights
final_matrix AS (
    SELECT
        *,
        -- Weighted composite score (customize weights as needed)
        (conversion_rate_score * 0.3) + 
        (roi_score * 0.4) + 
        (acquisition_cost_score * 0.2) + 
        (ctr_score * 0.1) AS composite_score,
        
        -- Flag high-performing combinations
        CASE 
            WHEN avg_conversion_rate IS NOT NULL AND conversion_rate_vs_global_avg > 0.1 THEN TRUE
            ELSE FALSE
        END AS is_high_conversion,
        
        CASE 
            WHEN avg_roi IS NOT NULL AND roi_vs_global_avg > 0.1 THEN TRUE
            ELSE FALSE
        END AS is_high_roi,
        
        CASE 
            WHEN avg_acquisition_cost IS NOT NULL AND acquisition_cost_vs_global_avg > 0.1 THEN TRUE
            ELSE FALSE
        END AS is_cost_efficient,
        
        CASE 
            WHEN avg_ctr IS NOT NULL AND ctr_vs_global_avg > 0.1 THEN TRUE
            ELSE FALSE
        END AS is_high_engagement
    FROM matrix_with_composite_score
)

-- Final output
SELECT
    goal,
    segment,
    campaign_count,
    
    -- Raw performance metrics
    avg_conversion_rate,
    avg_roi,
    avg_acquisition_cost,
    avg_ctr,
    
    -- Relative performance vs. goal average
    conversion_rate_vs_goal_avg,
    roi_vs_goal_avg,
    acquisition_cost_vs_goal_avg,
    ctr_vs_goal_avg,
    
    -- Relative performance vs. segment average
    conversion_rate_vs_segment_avg,
    roi_vs_segment_avg,
    acquisition_cost_vs_segment_avg,
    ctr_vs_segment_avg,
    
    -- Relative performance vs. global average
    conversion_rate_vs_global_avg,
    roi_vs_global_avg,
    acquisition_cost_vs_global_avg,
    ctr_vs_global_avg,
    
    -- Rankings
    conversion_rate_rank,
    conversion_rate_rank_within_goal,
    conversion_rate_rank_within_segment,
    roi_rank,
    roi_rank_within_goal,
    roi_rank_within_segment,
    acquisition_cost_rank,
    acquisition_cost_rank_within_goal,
    acquisition_cost_rank_within_segment,
    ctr_rank,
    ctr_rank_within_goal,
    ctr_rank_within_segment,
    
    -- Scores
    conversion_rate_score,
    roi_score,
    acquisition_cost_score,
    ctr_score,
    composite_score,
    
    -- Performance flags
    is_high_conversion,
    is_high_roi,
    is_cost_efficient,
    is_high_engagement,
    
    -- Flag top performers (top 10% by composite score)
    CASE 
        WHEN ROW_NUMBER() OVER (ORDER BY composite_score DESC) <= CEIL(total_combinations * 0.1) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_top_performer,
    
    -- Flag combinations with no data
    CASE 
        WHEN campaign_count = 0 THEN TRUE
        ELSE FALSE
    END AS is_untested_combination
FROM final_matrix
ORDER BY 
    CASE WHEN campaign_count = 0 THEN 1 ELSE 0 END,  -- Put combinations with no data last
    composite_score DESC
