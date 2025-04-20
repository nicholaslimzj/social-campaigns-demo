{{ config(materialized='table') }}

/*
Model: audience_quarter_performance_matrix
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model analyzes audience performance metrics with Company as the primary dimension and Target_Audience
as the secondary dimension, followed by other dimensions (location, channel, campaign goal).

Key Features:
1. Hierarchical structure with Company as the primary dimension
2. Target_Audience as the secondary dimension within each Company
3. Additional dimensions (location, channel, goal) as tertiary dimensions
4. Calculates performance metrics for each combination (conversion rate, ROI, acquisition cost, CTR)
5. Provides multi-dimensional performance comparisons:
   - Performance relative to company average
   - Performance relative to audience average within company
   - Performance relative to global average
6. Implements a weighted composite score for ranking combinations

Dashboard Usage:
- Company-Audience Performance Matrix heatmap
- Company-specific Audience Performance visualization
- Audience-Language Performance visualization by company
- Audience-Goal Response visualization by company
- High-ROI Company-Audience Combinations identification
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

-- Get distinct companies
distinct_companies AS (
    SELECT DISTINCT Company
    FROM current_quarter_data
),

-- Get distinct target audiences per company
company_audience_combinations AS (
    SELECT DISTINCT
        Company,
        Target_Audience
    FROM current_quarter_data
),

-- Get other dimensions for each company-audience combination
company_audience_location_combinations AS (
    SELECT DISTINCT
        ca.Company,
        ca.Target_Audience,
        cq.Location AS dimension_value,
        'location' AS dimension_type
    FROM company_audience_combinations ca
    JOIN current_quarter_data cq ON ca.Company = cq.Company AND ca.Target_Audience = cq.Target_Audience
),

company_audience_language_combinations AS (
    SELECT DISTINCT
        ca.Company,
        ca.Target_Audience,
        cq.Language AS dimension_value,
        'language' AS dimension_type
    FROM company_audience_combinations ca
    JOIN current_quarter_data cq ON ca.Company = cq.Company AND ca.Target_Audience = cq.Target_Audience
),

company_audience_goal_combinations AS (
    SELECT DISTINCT
        ca.Company,
        ca.Target_Audience,
        cq.Campaign_Goal AS dimension_value,
        'goal' AS dimension_type
    FROM company_audience_combinations ca
    JOIN current_quarter_data cq ON ca.Company = cq.Company AND ca.Target_Audience = cq.Target_Audience
),

-- Combine all dimension combinations
all_combinations AS (
    SELECT 
        Company,
        Target_Audience,
        dimension_value,
        dimension_type
    FROM company_audience_location_combinations
    
    UNION ALL
    
    SELECT 
        Company,
        Target_Audience,
        dimension_value,
        dimension_type
    FROM company_audience_language_combinations
    
    UNION ALL
    
    SELECT 
        Company,
        Target_Audience,
        dimension_value,
        dimension_type
    FROM company_audience_goal_combinations
),

-- Calculate metrics for each company-audience-dimension combination
combination_metrics AS (
    SELECT
        c.Company,
        c.Target_Audience,
        c.dimension_value,
        c.dimension_type,
        COUNT(s.Campaign_ID) AS campaign_count,
        AVG(s.Conversion_Rate) AS avg_conversion_rate,
        AVG(s.ROI) AS avg_roi,
        AVG(s.Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(s.Clicks) AS FLOAT) / NULLIF(SUM(s.Impressions), 0) AS avg_ctr
    FROM all_combinations c
    LEFT JOIN current_quarter_data s
        ON c.Company = s.Company
        AND c.Target_Audience = s.Target_Audience
        AND (
            (c.dimension_type = 'location' AND c.dimension_value = s.Location) OR
            (c.dimension_type = 'language' AND c.dimension_value = s.Language) OR
            (c.dimension_type = 'goal' AND c.dimension_value = s.Campaign_Goal)
        )
    GROUP BY c.Company, c.Target_Audience, c.dimension_value, c.dimension_type
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

-- Calculate company averages
company_metrics AS (
    SELECT
        Company,
        AVG(avg_conversion_rate) AS company_avg_conversion_rate,
        AVG(avg_roi) AS company_avg_roi,
        AVG(avg_acquisition_cost) AS company_avg_acquisition_cost,
        AVG(avg_ctr) AS company_avg_ctr
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
    GROUP BY Company
),

-- Calculate company-audience averages
company_audience_metrics AS (
    SELECT
        Company,
        Target_Audience,
        AVG(avg_conversion_rate) AS audience_avg_conversion_rate,
        AVG(avg_roi) AS audience_avg_roi,
        AVG(avg_acquisition_cost) AS audience_avg_acquisition_cost,
        AVG(avg_ctr) AS audience_avg_ctr
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
    GROUP BY Company, Target_Audience
),

-- Calculate dimension averages within company
dimension_metrics AS (
    SELECT
        Company,
        dimension_value,
        dimension_type,
        AVG(avg_conversion_rate) AS dimension_avg_conversion_rate,
        AVG(avg_roi) AS dimension_avg_roi,
        AVG(avg_acquisition_cost) AS dimension_avg_acquisition_cost,
        AVG(avg_ctr) AS dimension_avg_ctr
    FROM combination_metrics
    WHERE campaign_count > 0  -- Only include combinations that have campaigns
    GROUP BY Company, dimension_value, dimension_type
)

-- Final output with normalized metrics and composite score
SELECT
    cm.Company,
    cm.Target_Audience,
    cm.dimension_value,
    cm.dimension_type,
    cm.campaign_count,
    
    -- Raw metrics
    cm.avg_conversion_rate,
    cm.avg_roi,
    cm.avg_acquisition_cost,
    cm.avg_ctr,
    
    -- Company averages for context
    comp.company_avg_conversion_rate,
    comp.company_avg_roi,
    comp.company_avg_acquisition_cost,
    comp.company_avg_ctr,
    
    -- Company-Audience averages for context
    ca.audience_avg_conversion_rate,
    ca.audience_avg_roi,
    ca.audience_avg_acquisition_cost,
    ca.audience_avg_ctr,
    
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
    
    -- Performance vs company average
    CASE 
        WHEN comp.company_avg_conversion_rate > 0 
        THEN cm.avg_conversion_rate / comp.company_avg_conversion_rate - 1
        ELSE NULL
    END AS conversion_rate_vs_company_avg,
    
    CASE 
        WHEN comp.company_avg_roi > 0 
        THEN cm.avg_roi / comp.company_avg_roi - 1
        ELSE NULL
    END AS roi_vs_company_avg,
    
    CASE 
        WHEN cm.avg_acquisition_cost > 0 
        THEN comp.company_avg_acquisition_cost / cm.avg_acquisition_cost - 1
        ELSE NULL
    END AS acquisition_cost_vs_company_avg,
    
    CASE 
        WHEN comp.company_avg_ctr > 0 
        THEN cm.avg_ctr / comp.company_avg_ctr - 1
        ELSE NULL
    END AS ctr_vs_company_avg,
    
    -- Performance vs company-audience average
    CASE 
        WHEN ca.audience_avg_conversion_rate > 0 
        THEN cm.avg_conversion_rate / ca.audience_avg_conversion_rate - 1
        ELSE NULL
    END AS conversion_rate_vs_audience_avg,
    
    CASE 
        WHEN ca.audience_avg_roi > 0 
        THEN cm.avg_roi / ca.audience_avg_roi - 1
        ELSE NULL
    END AS roi_vs_audience_avg,
    
    CASE 
        WHEN cm.avg_acquisition_cost > 0 
        THEN ca.audience_avg_acquisition_cost / cm.avg_acquisition_cost - 1
        ELSE NULL
    END AS acquisition_cost_vs_audience_avg,
    
    CASE 
        WHEN ca.audience_avg_ctr > 0 
        THEN cm.avg_ctr / ca.audience_avg_ctr - 1
        ELSE NULL
    END AS ctr_vs_audience_avg,
    
    -- Performance relative to dimension average
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
    
    -- Performance relative to global average
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
    
    -- Normalized metrics (0-1 scale)
    CASE 
        WHEN (SELECT MAX(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0) = 
             (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0)
        THEN 0.5
        ELSE (cm.avg_conversion_rate - (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0)) / 
             NULLIF((SELECT MAX(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0) - 
                   (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0), 0)
    END AS normalized_conversion_rate,
    
    CASE 
        WHEN (SELECT MAX(avg_roi) FROM combination_metrics WHERE campaign_count > 0) = 
             (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0)
        THEN 0.5
        ELSE (cm.avg_roi - (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0)) / 
             NULLIF((SELECT MAX(avg_roi) FROM combination_metrics WHERE campaign_count > 0) - 
                   (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0), 0)
    END AS normalized_roi,
    
    CASE 
        WHEN (SELECT MAX(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0) = 
             (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0)
        THEN 0.5
        ELSE 1 - (cm.avg_acquisition_cost - (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0)) / 
                 NULLIF((SELECT MAX(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0) - 
                       (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0), 0)
    END AS normalized_acquisition_cost,
    
    CASE 
        WHEN (SELECT MAX(avg_ctr) FROM combination_metrics WHERE campaign_count > 0) = 
             (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0)
        THEN 0.5
        ELSE (cm.avg_ctr - (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0)) / 
             NULLIF((SELECT MAX(avg_ctr) FROM combination_metrics WHERE campaign_count > 0) - 
                   (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0), 0)
    END AS normalized_ctr,
    
    -- Weighted composite score (customizable weights)
    CASE 
        WHEN cm.campaign_count = 0 THEN NULL
        ELSE (
            CASE 
                WHEN (SELECT MAX(avg_roi) FROM combination_metrics WHERE campaign_count > 0) = 
                     (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0)
                THEN 0.5
                ELSE (cm.avg_roi - (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0)) / 
                     NULLIF((SELECT MAX(avg_roi) FROM combination_metrics WHERE campaign_count > 0) - 
                           (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0), 0)
            END * 0.4 -- ROI weight: 40%
            +
            CASE 
                WHEN (SELECT MAX(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0) = 
                     (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0)
                THEN 0.5
                ELSE (cm.avg_conversion_rate - (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0)) / 
                     NULLIF((SELECT MAX(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0) - 
                           (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0), 0)
            END * 0.3 -- Conversion rate weight: 30%
            +
            CASE 
                WHEN (SELECT MAX(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0) = 
                     (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0)
                THEN 0.5
                ELSE 1 - (cm.avg_acquisition_cost - (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0)) / 
                         NULLIF((SELECT MAX(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0) - 
                               (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0), 0)
            END * 0.2 -- Acquisition cost weight: 20%
            +
            CASE 
                WHEN (SELECT MAX(avg_ctr) FROM combination_metrics WHERE campaign_count > 0) = 
                     (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0)
                THEN 0.5
                ELSE (cm.avg_ctr - (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0)) / 
                     NULLIF((SELECT MAX(avg_ctr) FROM combination_metrics WHERE campaign_count > 0) - 
                           (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0), 0)
            END * 0.1 -- CTR weight: 10%
        )
    END AS composite_score,
    
    -- Flag top performers (top 10% by composite score within each dimension type)
    CASE 
        WHEN cm.campaign_count = 0 THEN FALSE
        ELSE PERCENT_RANK() OVER (
            PARTITION BY cm.dimension_type
            ORDER BY 
                CASE 
                    WHEN cm.campaign_count = 0 THEN NULL
                    ELSE (
                        CASE 
                            WHEN (SELECT MAX(avg_roi) FROM combination_metrics WHERE campaign_count > 0) = 
                                 (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0)
                            THEN 0.5
                            ELSE (cm.avg_roi - (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0)) / 
                                 NULLIF((SELECT MAX(avg_roi) FROM combination_metrics WHERE campaign_count > 0) - 
                                       (SELECT MIN(avg_roi) FROM combination_metrics WHERE campaign_count > 0), 0)
                        END * 0.4 -- ROI weight: 40%
                        +
                        CASE 
                            WHEN (SELECT MAX(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0) = 
                                 (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0)
                            THEN 0.5
                            ELSE (cm.avg_conversion_rate - (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0)) / 
                                 NULLIF((SELECT MAX(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0) - 
                                       (SELECT MIN(avg_conversion_rate) FROM combination_metrics WHERE campaign_count > 0), 0)
                        END * 0.3 -- Conversion rate weight: 30%
                        +
                        CASE 
                            WHEN (SELECT MAX(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0) = 
                                 (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0)
                            THEN 0.5
                            ELSE 1 - (cm.avg_acquisition_cost - (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0)) / 
                                     NULLIF((SELECT MAX(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0) - 
                                           (SELECT MIN(avg_acquisition_cost) FROM combination_metrics WHERE campaign_count > 0), 0)
                        END * 0.2 -- Acquisition cost weight: 20%
                        +
                        CASE 
                            WHEN (SELECT MAX(avg_ctr) FROM combination_metrics WHERE campaign_count > 0) = 
                                 (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0)
                            THEN 0.5
                            ELSE (cm.avg_ctr - (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0)) / 
                                 NULLIF((SELECT MAX(avg_ctr) FROM combination_metrics WHERE campaign_count > 0) - 
                                       (SELECT MIN(avg_ctr) FROM combination_metrics WHERE campaign_count > 0), 0)
                        END * 0.1 -- CTR weight: 10%
                    )
                END DESC
        ) >= 0.9
    END AS is_top_performer
FROM combination_metrics cm
LEFT JOIN company_metrics comp ON cm.Company = comp.Company
LEFT JOIN company_audience_metrics ca ON cm.Company = ca.Company AND cm.Target_Audience = ca.Target_Audience
LEFT JOIN dimension_metrics dm ON cm.Company = dm.Company AND cm.dimension_value = dm.dimension_value AND cm.dimension_type = dm.dimension_type
ORDER BY 
    cm.Company,
    cm.Target_Audience,
    cm.dimension_type,
    cm.dimension_value
