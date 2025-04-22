{{ config(materialized='table') }}

/*
Model: channel_quarter_performance_matrix
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model analyzes channel performance metrics with Channel as the primary dimension and Company
as the secondary dimension, followed by other dimensions (segment, campaign goal).

Key Features:
1. Channel as the primary dimension for analysis
2. Company as the secondary dimension within each Channel
3. Additional dimensions (segment, goal) as tertiary dimensions
4. Calculates performance metrics for each combination (conversion rate, ROI, acquisition cost, CTR)
5. Provides multi-dimensional performance comparisons:
   - Performance relative to channel average
   - Performance relative to company average within channel
   - Performance relative to global average
6. Implements a weighted composite score for ranking combinations

Dashboard Usage:
- Channel ROI vs Acquisition Cost bubble chart
- Channel Mix Analysis pie chart
- Channel Performance Metrics table
- Channel by Campaign Goal heatmap
- Channel by Target Audience heatmap
*/

WITH 
-- First, determine the date range for the current quarter
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month
    FROM {{ ref('stg_campaigns') }}
),

-- Get current quarter data
current_quarter_data AS (
    SELECT *
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
),

-- Get distinct channels
distinct_channels AS (
    SELECT DISTINCT Channel_Used
    FROM current_quarter_data
),

-- Get distinct companies per channel
channel_company_combinations AS (
    SELECT DISTINCT
        Channel_Used,
        Company
    FROM current_quarter_data
),

-- Get other dimensions for each channel-company combination
channel_company_audience_combinations AS (
    SELECT DISTINCT
        cc.Channel_Used,
        cc.Company,
        cq.Target_Audience AS dimension_value,
        'target_audience' AS dimension_type
    FROM channel_company_combinations cc
    JOIN current_quarter_data cq ON cc.Channel_Used = cq.Channel_Used AND cc.Company = cq.Company
),

channel_company_goal_combinations AS (
    SELECT DISTINCT
        cc.Channel_Used,
        cc.Company,
        cq.Campaign_Goal AS dimension_value,
        'goal' AS dimension_type
    FROM channel_company_combinations cc
    JOIN current_quarter_data cq ON cc.Channel_Used = cq.Channel_Used AND cc.Company = cq.Company
),

-- Combine all dimension combinations
all_combinations AS (
    SELECT 
        Channel_Used,
        Company,
        dimension_value,
        dimension_type
    FROM channel_company_audience_combinations
    
    UNION ALL
    
    SELECT 
        Channel_Used,
        Company,
        dimension_value,
        dimension_type
    FROM channel_company_goal_combinations
),

-- Calculate metrics for each channel-company-dimension combination
combination_metrics AS (
    SELECT
        c.Channel_Used,
        c.Company,
        c.dimension_value,
        c.dimension_type,
        COUNT(s.Campaign_ID) AS campaign_count,
        AVG(s.Conversion_Rate) AS avg_conversion_rate,
        AVG(s.ROI) AS avg_roi,
        AVG(s.Acquisition_Cost) AS avg_acquisition_cost,
        SUM(s.Clicks) AS total_clicks,
        SUM(s.Impressions) AS total_impressions,
        CAST(SUM(s.Clicks) AS FLOAT) / NULLIF(SUM(s.Impressions), 0) AS avg_ctr,
        SUM(s.Clicks * s.Conversion_Rate * s.Acquisition_Cost) AS total_spend,
        SUM(s.Clicks * s.Conversion_Rate * s.Acquisition_Cost * (1 + s.ROI)) AS total_revenue
    FROM all_combinations c
    LEFT JOIN current_quarter_data s
        ON c.Channel_Used = s.Channel_Used
        AND c.Company = s.Company
        AND (
            (c.dimension_type = 'target_audience' AND c.dimension_value = s.Target_Audience) OR
            (c.dimension_type = 'goal' AND c.dimension_value = s.Campaign_Goal)
        )
    GROUP BY c.Channel_Used, c.Company, c.dimension_value, c.dimension_type
),

-- Calculate global averages for normalization
global_metrics AS (
    SELECT
        AVG(avg_conversion_rate) AS global_avg_conversion_rate,
        AVG(avg_roi) AS global_avg_roi,
        AVG(avg_acquisition_cost) AS global_avg_acquisition_cost,
        AVG(avg_ctr) AS global_avg_ctr
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
),

-- Calculate channel averages
channel_metrics AS (
    SELECT
        Channel_Used,
        AVG(avg_conversion_rate) AS channel_avg_conversion_rate,
        AVG(avg_roi) AS channel_avg_roi,
        AVG(avg_acquisition_cost) AS channel_avg_acquisition_cost,
        AVG(avg_ctr) AS channel_avg_ctr,
        SUM(total_spend) AS channel_total_spend,
        SUM(total_revenue) AS channel_total_revenue
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
    GROUP BY Channel_Used
),

-- Calculate channel-company averages
channel_company_metrics AS (
    SELECT
        Channel_Used,
        Company,
        AVG(avg_conversion_rate) AS company_avg_conversion_rate,
        AVG(avg_roi) AS company_avg_roi,
        AVG(avg_acquisition_cost) AS company_avg_acquisition_cost,
        AVG(avg_ctr) AS company_avg_ctr,
        SUM(total_spend) AS company_total_spend,
        SUM(total_revenue) AS company_total_revenue
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
    GROUP BY Channel_Used, Company
),

-- Calculate dimension averages within channel
dimension_metrics AS (
    SELECT
        Channel_Used,
        dimension_value,
        dimension_type,
        AVG(avg_conversion_rate) AS dimension_avg_conversion_rate,
        AVG(avg_roi) AS dimension_avg_roi,
        AVG(avg_acquisition_cost) AS dimension_avg_acquisition_cost,
        AVG(avg_ctr) AS dimension_avg_ctr
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
    GROUP BY Channel_Used, dimension_value, dimension_type
)

-- Final output with normalized metrics and composite score
SELECT
    cm.Channel_Used,
    cm.Company,
    cm.dimension_value,
    cm.dimension_type,
    cm.campaign_count,
    cm.avg_conversion_rate,
    cm.avg_roi,
    cm.avg_acquisition_cost,
    cm.avg_ctr,
    cm.total_clicks,
    cm.total_impressions,
    cm.total_spend,
    cm.total_revenue,
    
    -- Channel averages for context
    ch.channel_avg_conversion_rate,
    ch.channel_avg_roi,
    ch.channel_avg_acquisition_cost,
    ch.channel_avg_ctr,
    ch.channel_total_spend,
    ch.channel_total_revenue,
    
    -- Channel-Company averages for context
    cc.company_avg_conversion_rate,
    cc.company_avg_roi,
    cc.company_avg_acquisition_cost,
    cc.company_avg_ctr,
    cc.company_total_spend,
    cc.company_total_revenue,
    
    -- Dimension averages for context
    dm.dimension_avg_conversion_rate,
    dm.dimension_avg_roi,
    dm.dimension_avg_acquisition_cost,
    dm.dimension_avg_ctr,
    
    -- Global averages for context
    (SELECT global_avg_conversion_rate FROM global_metrics) AS global_avg_conversion_rate,
    (SELECT global_avg_roi FROM global_metrics) AS global_avg_roi,
    (SELECT global_avg_acquisition_cost FROM global_metrics) AS global_avg_acquisition_cost,
    (SELECT global_avg_ctr FROM global_metrics) AS global_avg_ctr,
    
    -- Performance vs channel average
    CASE 
        WHEN ch.channel_avg_conversion_rate > 0 
        THEN cm.avg_conversion_rate / ch.channel_avg_conversion_rate - 1
        ELSE NULL
    END AS conversion_rate_vs_channel_avg,
    
    CASE 
        WHEN ch.channel_avg_roi > 0 
        THEN cm.avg_roi / ch.channel_avg_roi - 1
        ELSE NULL
    END AS roi_vs_channel_avg,
    
    CASE 
        WHEN cm.avg_acquisition_cost > 0 
        THEN ch.channel_avg_acquisition_cost / cm.avg_acquisition_cost - 1
        ELSE NULL
    END AS acquisition_cost_vs_channel_avg,
    
    CASE 
        WHEN ch.channel_avg_ctr > 0 
        THEN cm.avg_ctr / ch.channel_avg_ctr - 1
        ELSE NULL
    END AS ctr_vs_channel_avg,
    
    -- Performance vs company average within channel
    CASE 
        WHEN cc.company_avg_conversion_rate > 0 
        THEN cm.avg_conversion_rate / cc.company_avg_conversion_rate - 1
        ELSE NULL
    END AS conversion_rate_vs_company_avg,
    
    CASE 
        WHEN cc.company_avg_roi > 0 
        THEN cm.avg_roi / cc.company_avg_roi - 1
        ELSE NULL
    END AS roi_vs_company_avg,
    
    CASE 
        WHEN cm.avg_acquisition_cost > 0 
        THEN cc.company_avg_acquisition_cost / cm.avg_acquisition_cost - 1
        ELSE NULL
    END AS acquisition_cost_vs_company_avg,
    
    CASE 
        WHEN cc.company_avg_ctr > 0 
        THEN cm.avg_ctr / cc.company_avg_ctr - 1
        ELSE NULL
    END AS ctr_vs_company_avg,
    
    -- Performance vs dimension average
    CASE 
        WHEN dm.dimension_avg_conversion_rate > 0 
        THEN cm.avg_conversion_rate / dm.dimension_avg_conversion_rate - 1
        ELSE NULL
    END AS conversion_rate_vs_dimension_avg,
    
    CASE 
        WHEN dm.dimension_avg_roi > 0 
        THEN cm.avg_roi / dm.dimension_avg_roi - 1
        ELSE NULL
    END AS roi_vs_dimension_avg,
    
    CASE 
        WHEN cm.avg_acquisition_cost > 0 
        THEN dm.dimension_avg_acquisition_cost / cm.avg_acquisition_cost - 1
        ELSE NULL
    END AS acquisition_cost_vs_dimension_avg,
    
    CASE 
        WHEN dm.dimension_avg_ctr > 0 
        THEN cm.avg_ctr / dm.dimension_avg_ctr - 1
        ELSE NULL
    END AS ctr_vs_dimension_avg,
    
    -- Performance vs global average
    CASE 
        WHEN (SELECT global_avg_conversion_rate FROM global_metrics) > 0 
        THEN cm.avg_conversion_rate / (SELECT global_avg_conversion_rate FROM global_metrics) - 1
        ELSE NULL
    END AS conversion_rate_vs_global_avg,
    
    CASE 
        WHEN (SELECT global_avg_roi FROM global_metrics) > 0 
        THEN cm.avg_roi / (SELECT global_avg_roi FROM global_metrics) - 1
        ELSE NULL
    END AS roi_vs_global_avg,
    
    CASE 
        WHEN cm.avg_acquisition_cost > 0 
        THEN (SELECT global_avg_acquisition_cost FROM global_metrics) / cm.avg_acquisition_cost - 1
        ELSE NULL
    END AS acquisition_cost_vs_global_avg,
    
    CASE 
        WHEN (SELECT global_avg_ctr FROM global_metrics) > 0 
        THEN cm.avg_ctr / (SELECT global_avg_ctr FROM global_metrics) - 1
        ELSE NULL
    END AS ctr_vs_global_avg,
    

    
    -- Spend allocation percentages
    CASE 
        WHEN ch.channel_total_spend > 0 
        THEN cm.total_spend / ch.channel_total_spend
        ELSE NULL
    END AS spend_pct_of_channel,
    
    -- Revenue contribution percentages
    CASE 
        WHEN ch.channel_total_revenue > 0 
        THEN cm.total_revenue / ch.channel_total_revenue
        ELSE NULL
    END AS revenue_pct_of_channel,
    
    -- Weighted composite score (higher is better)
    -- Weights can be adjusted based on business priorities
    (
        CASE 
            WHEN (SELECT global_avg_conversion_rate FROM global_metrics) > 0 
            THEN (cm.avg_conversion_rate / (SELECT global_avg_conversion_rate FROM global_metrics)) * 0.25
            ELSE 0
        END
        +
        CASE 
            WHEN (SELECT global_avg_roi FROM global_metrics) > 0 
            THEN (cm.avg_roi / (SELECT global_avg_roi FROM global_metrics)) * 0.4
            ELSE 0
        END
        +
        CASE 
            WHEN cm.avg_acquisition_cost > 0 AND (SELECT global_avg_acquisition_cost FROM global_metrics) > 0
            THEN ((SELECT global_avg_acquisition_cost FROM global_metrics) / cm.avg_acquisition_cost) * 0.25
            ELSE 0
        END
        +
        CASE 
            WHEN (SELECT global_avg_ctr FROM global_metrics) > 0 
            THEN (cm.avg_ctr / (SELECT global_avg_ctr FROM global_metrics)) * 0.1
            ELSE 0
        END
    ) AS composite_score,
    
    -- Performance classification
    CASE
        WHEN (
            CASE 
                WHEN (SELECT global_avg_conversion_rate FROM global_metrics) > 0 
                THEN (cm.avg_conversion_rate / (SELECT global_avg_conversion_rate FROM global_metrics)) * 0.25
                ELSE 0
            END
            +
            CASE 
                WHEN (SELECT global_avg_roi FROM global_metrics) > 0 
                THEN (cm.avg_roi / (SELECT global_avg_roi FROM global_metrics)) * 0.4
                ELSE 0
            END
            +
            CASE 
                WHEN cm.avg_acquisition_cost > 0 AND (SELECT global_avg_acquisition_cost FROM global_metrics) > 0
                THEN ((SELECT global_avg_acquisition_cost FROM global_metrics) / cm.avg_acquisition_cost) * 0.25
                ELSE 0
            END
            +
            CASE 
                WHEN (SELECT global_avg_ctr FROM global_metrics) > 0 
                THEN (cm.avg_ctr / (SELECT global_avg_ctr FROM global_metrics)) * 0.1
                ELSE 0
            END
        ) >= 1.2 THEN 'high_performer'
        WHEN (
            CASE 
                WHEN (SELECT global_avg_conversion_rate FROM global_metrics) > 0 
                THEN (cm.avg_conversion_rate / (SELECT global_avg_conversion_rate FROM global_metrics)) * 0.25
                ELSE 0
            END
            +
            CASE 
                WHEN (SELECT global_avg_roi FROM global_metrics) > 0 
                THEN (cm.avg_roi / (SELECT global_avg_roi FROM global_metrics)) * 0.4
                ELSE 0
            END
            +
            CASE 
                WHEN cm.avg_acquisition_cost > 0 AND (SELECT global_avg_acquisition_cost FROM global_metrics) > 0
                THEN ((SELECT global_avg_acquisition_cost FROM global_metrics) / cm.avg_acquisition_cost) * 0.25
                ELSE 0
            END
            +
            CASE 
                WHEN (SELECT global_avg_ctr FROM global_metrics) > 0 
                THEN (cm.avg_ctr / (SELECT global_avg_ctr FROM global_metrics)) * 0.1
                ELSE 0
            END
        ) <= 0.8 THEN 'low_performer'
        ELSE 'average_performer'
    END AS performance_tier,
    
    -- Flag top performers (in top 10% by composite score)
    CASE
        WHEN PERCENT_RANK() OVER (
            PARTITION BY cm.dimension_type
            ORDER BY (
                CASE 
                    WHEN (SELECT global_avg_conversion_rate FROM global_metrics) > 0 
                    THEN (cm.avg_conversion_rate / (SELECT global_avg_conversion_rate FROM global_metrics)) * 0.25
                    ELSE 0
                END
                +
                CASE 
                    WHEN (SELECT global_avg_roi FROM global_metrics) > 0 
                    THEN (cm.avg_roi / (SELECT global_avg_roi FROM global_metrics)) * 0.4
                    ELSE 0
                END
                +
                CASE 
                    WHEN cm.avg_acquisition_cost > 0 AND (SELECT global_avg_acquisition_cost FROM global_metrics) > 0
                    THEN ((SELECT global_avg_acquisition_cost FROM global_metrics) / cm.avg_acquisition_cost) * 0.25
                    ELSE 0
                END
                +
                CASE 
                    WHEN (SELECT global_avg_ctr FROM global_metrics) > 0 
                    THEN (cm.avg_ctr / (SELECT global_avg_ctr FROM global_metrics)) * 0.1
                    ELSE 0
                END
            ) DESC
        ) >= 0.9 THEN TRUE
        ELSE FALSE
    END AS is_top_performer
FROM combination_metrics cm
LEFT JOIN channel_metrics ch ON cm.Channel_Used = ch.Channel_Used
LEFT JOIN channel_company_metrics cc ON cm.Channel_Used = cc.Channel_Used AND cm.Company = cc.Company
LEFT JOIN dimension_metrics dm ON cm.Channel_Used = dm.Channel_Used AND cm.dimension_value = dm.dimension_value AND cm.dimension_type = dm.dimension_type
ORDER BY 
    cm.Channel_Used,
    cm.Company,
    cm.dimension_type,
    cm.dimension_value
