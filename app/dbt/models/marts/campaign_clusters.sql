{{ config(materialized='table') }}

/*
This model identifies high-performing campaign combinations by:
1. Grouping campaigns by combinations of Goal, Segment, Channel, and Duration buckets
2. Calculating performance metrics for each combination
3. Ranking combinations based on multiple metrics
4. Identifying optimal duration ranges for each combination
5. Flagging the most effective combinations as "winning combinations"

The approach simulates a clustering analysis without requiring advanced ML techniques,
making it compatible with DuckDB's SQL capabilities.
*/

WITH 
-- First, bucket campaign durations into ranges for more meaningful grouping
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

-- Group by the key dimensions to find combinations
campaign_combinations AS (
    SELECT
        Company,
        Campaign_Goal AS goal,
        Customer_Segment AS segment,
        Channel_Used AS channel,
        duration_bucket,
        -- Calculate performance metrics for each combination
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        COUNT(*) AS campaign_count,
        -- Calculate the most common duration bucket
        MODE() WITHIN GROUP (ORDER BY duration_bucket_num) AS most_common_duration_bucket,
        -- Calculate the min and max durations to find the optimal range
        MIN(Duration) AS min_duration,
        MAX(Duration) AS max_duration,
        -- Calculate the average duration
        AVG(Duration) AS avg_duration
    FROM campaign_with_duration_buckets
    GROUP BY 
        Company,
        Campaign_Goal,
        Customer_Segment,
        Channel_Used,
        duration_bucket
    HAVING COUNT(*) >= 3  -- Only consider combinations with at least 3 campaigns for statistical significance
),

-- Calculate global metrics for comparison
global_metrics AS (
    SELECT
        AVG(avg_conversion_rate) AS global_avg_conversion_rate,
        AVG(avg_roi) AS global_avg_roi,
        AVG(avg_acquisition_cost) AS global_avg_acquisition_cost,
        AVG(avg_ctr) AS global_avg_ctr
    FROM campaign_combinations
),

-- Calculate company-specific metrics for comparison
company_metrics AS (
    SELECT
        Company,
        AVG(avg_conversion_rate) AS company_avg_conversion_rate,
        AVG(avg_roi) AS company_avg_roi,
        AVG(avg_acquisition_cost) AS company_avg_acquisition_cost,
        AVG(avg_ctr) AS company_avg_ctr
    FROM campaign_combinations
    GROUP BY Company
),

-- Rank combinations within each company
ranked_combinations AS (
    SELECT
        cc.*,
        -- Compare to company averages
        cc.avg_conversion_rate / NULLIF(cm.company_avg_conversion_rate, 0) AS conversion_rate_vs_company,
        cc.avg_roi / NULLIF(cm.company_avg_roi, 0) AS roi_vs_company,
        cm.company_avg_acquisition_cost / NULLIF(cc.avg_acquisition_cost, 0) AS acquisition_cost_vs_company,
        cc.avg_ctr / NULLIF(cm.company_avg_ctr, 0) AS ctr_vs_company,
        
        -- Compare to global averages
        cc.avg_conversion_rate / NULLIF(gm.global_avg_conversion_rate, 0) AS conversion_rate_vs_global,
        cc.avg_roi / NULLIF(gm.global_avg_roi, 0) AS roi_vs_global,
        gm.global_avg_acquisition_cost / NULLIF(cc.avg_acquisition_cost, 0) AS acquisition_cost_vs_global,
        cc.avg_ctr / NULLIF(gm.global_avg_ctr, 0) AS ctr_vs_global,
        
        -- Rank combinations within each company by different metrics
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_conversion_rate DESC
        ) AS conversion_rate_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_roi DESC
        ) AS roi_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_acquisition_cost ASC
        ) AS acquisition_cost_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_ctr DESC
        ) AS ctr_rank,
        
        -- Rankings will be used to calculate a composite score later
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_roi DESC
        ) AS roi_rank_for_composite,
        
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_conversion_rate DESC
        ) AS conversion_rank_for_composite,
        
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_acquisition_cost ASC
        ) AS acquisition_rank_for_composite,
        
        ROW_NUMBER() OVER (
            PARTITION BY cc.Company 
            ORDER BY cc.avg_ctr DESC
        ) AS ctr_rank_for_composite,
        
        -- Count total combinations per company for percentile calculations
        COUNT(*) OVER (PARTITION BY cc.Company) AS total_combinations_per_company
    FROM campaign_combinations cc
    JOIN company_metrics cm ON cc.Company = cm.Company
    CROSS JOIN global_metrics gm
),

-- Find the optimal duration for each goal-segment-channel combination
optimal_durations AS (
    SELECT
        Company,
        goal,
        segment,
        channel,
        -- Use the duration bucket from the highest performing combination by ROI
        FIRST_VALUE(duration_bucket) OVER (
            PARTITION BY Company, goal, segment, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_duration_bucket,
        -- Get the min and max duration from that bucket to create a range
        FIRST_VALUE(min_duration) OVER (
            PARTITION BY Company, goal, segment, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_min_duration,
        FIRST_VALUE(max_duration) OVER (
            PARTITION BY Company, goal, segment, channel
            ORDER BY avg_roi DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS optimal_max_duration
    FROM ranked_combinations
),

-- Deduplicate the optimal durations
distinct_optimal_durations AS (
    SELECT DISTINCT
        Company,
        goal,
        segment,
        channel,
        optimal_duration_bucket,
        optimal_min_duration,
        optimal_max_duration
    FROM optimal_durations
),

-- Calculate composite score using the individual ranks
composite_scored AS (
    SELECT
        *,
        -- Calculate weighted composite score (lower is better)
        -- This weights ROI and conversion rate more heavily
        (roi_rank_for_composite * 0.4) +
        (conversion_rank_for_composite * 0.3) +
        (acquisition_rank_for_composite * 0.2) +
        (ctr_rank_for_composite * 0.1) AS composite_score,
        -- Rank based on the composite score
        ROW_NUMBER() OVER (
            PARTITION BY Company
            ORDER BY (roi_rank_for_composite * 0.4) +
                     (conversion_rank_for_composite * 0.3) +
                     (acquisition_rank_for_composite * 0.2) +
                     (ctr_rank_for_composite * 0.1) ASC
        ) AS composite_rank
    FROM ranked_combinations
)

-- Final output
SELECT
    cs.Company,
    cs.goal,
    cs.segment,
    cs.channel,
    cs.duration_bucket,
    cs.avg_conversion_rate,
    cs.avg_roi,
    cs.avg_acquisition_cost,
    cs.avg_ctr,
    cs.campaign_count,
    cs.min_duration,
    cs.max_duration,
    cs.avg_duration,
    
    -- Comparison to company averages
    cs.conversion_rate_vs_company,
    cs.roi_vs_company,
    cs.acquisition_cost_vs_company,
    cs.ctr_vs_company,
    
    -- Comparison to global averages
    cs.conversion_rate_vs_global,
    cs.roi_vs_global,
    cs.acquisition_cost_vs_global,
    cs.ctr_vs_global,
    
    -- Rankings
    cs.conversion_rate_rank,
    cs.roi_rank,
    cs.acquisition_cost_rank,
    cs.ctr_rank,
    cs.composite_rank,
    cs.composite_score,
    
    -- Optimal duration information
    dod.optimal_duration_bucket,
    dod.optimal_min_duration,
    dod.optimal_max_duration,
    
    -- Flag winning combinations (top 10% by composite rank)
    CASE 
        WHEN cs.composite_rank <= GREATEST(CAST(cs.total_combinations_per_company * 0.1 AS INTEGER), 1) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_winning_combination,
    
    -- Flag if this combination is using the optimal duration
    CASE 
        WHEN cs.duration_bucket = dod.optimal_duration_bucket 
        THEN TRUE 
        ELSE FALSE 
    END AS is_optimal_duration,
    
    -- Create a formatted optimal duration range string
    dod.optimal_min_duration || '-' || dod.optimal_max_duration || ' days' AS optimal_duration_range
FROM composite_scored cs
JOIN distinct_optimal_durations dod ON 
    cs.Company = dod.Company AND
    cs.goal = dod.goal AND
    cs.segment = dod.segment AND
    cs.channel = dod.channel
ORDER BY 
    cs.Company,
    cs.composite_rank
