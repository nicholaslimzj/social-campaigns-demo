{{ config(materialized='table') }}

/*
Model: audience_quarter_clusters
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model identifies high-performing target audience clusters for the current quarter by
analyzing combinations of Company, Target_Audience, and other dimensions to find optimal groupings.
It follows the hierarchical dimension structure with Company as the primary dimension and
Target_Audience as the secondary dimension.

Key Features:
1. Company as the primary dimension
2. Target_Audience as the secondary dimension
3. Clusters target audiences based on performance across channels, locations, and goals
4. Identifies high-ROI combinations
5. Calculates performance metrics for each cluster
6. Ranks clusters within each company

Dashboard Usage:
- High-ROI Target Audience Clusters visualization in the Cohort Analysis tab
*/

WITH 
-- First, determine the date range for the current quarter
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month
    FROM {{ ref('stg_campaigns') }}
),

-- Get data for the current quarter
current_quarter_data AS (
    SELECT 
        *,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) / 3 + EXTRACT(YEAR FROM CAST(Date AS DATE)) * 4 AS quarter
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
),

-- Group by Company, Target_Audience, and other dimensions to find clusters
audience_clusters AS (
    SELECT
        Company,
        Target_Audience,
        Location,
        Channel_Used AS channel,
        Campaign_Goal AS goal,
        
        -- Calculate performance metrics for each cluster
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr,
        SUM(Acquisition_Cost) AS total_spend,
        SUM(Acquisition_Cost * (1 + ROI)) AS total_revenue
    FROM current_quarter_data
    GROUP BY 
        Company,
        Target_Audience,
        Location,
        Channel_Used,
        Campaign_Goal
    HAVING COUNT(*) >= 3  -- Only consider combinations with at least 3 campaigns for statistical significance
),

-- Calculate company-level metrics for normalization
company_metrics AS (
    SELECT
        Company,
        AVG(avg_conversion_rate) AS avg_company_conversion_rate,
        STDDEV(avg_conversion_rate) AS stddev_company_conversion_rate,
        AVG(avg_roi) AS avg_company_roi,
        STDDEV(avg_roi) AS stddev_company_roi,
        AVG(avg_ctr) AS avg_company_ctr,
        STDDEV(avg_ctr) AS stddev_company_ctr,
        AVG(avg_acquisition_cost) AS avg_company_acquisition_cost,
        STDDEV(avg_acquisition_cost) AS stddev_company_acquisition_cost
    FROM audience_clusters
    GROUP BY Company
),

-- Calculate audience-level metrics within each company
audience_metrics AS (
    SELECT
        Company,
        Target_Audience,
        AVG(avg_conversion_rate) AS avg_audience_conversion_rate,
        AVG(avg_roi) AS avg_audience_roi,
        AVG(avg_ctr) AS avg_audience_ctr,
        AVG(avg_acquisition_cost) AS avg_audience_acquisition_cost,
        COUNT(*) AS cluster_count
    FROM audience_clusters
    GROUP BY Company, Target_Audience
),

-- Calculate normalized scores and rankings
scored_clusters AS (
    SELECT
        ac.*,
        am.avg_audience_conversion_rate,
        am.avg_audience_roi,
        am.avg_audience_ctr,
        am.avg_audience_acquisition_cost,
        am.cluster_count,
        
        -- Calculate normalized scores (z-scores) for key metrics
        CASE 
            WHEN cm.stddev_company_conversion_rate > 0 
            THEN (ac.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
            ELSE 0
        END AS conversion_rate_z_score,
        
        CASE 
            WHEN cm.stddev_company_roi > 0 
            THEN (ac.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
            ELSE 0
        END AS roi_z_score,
        
        CASE 
            WHEN cm.stddev_company_ctr > 0 
            THEN (ac.avg_ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
            ELSE 0
        END AS ctr_z_score,
        
        CASE 
            WHEN cm.stddev_company_acquisition_cost > 0 
            THEN -1 * (ac.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
            ELSE 0
        END AS cost_efficiency_z_score,
        
        -- Calculate composite performance score (weighted average of z-scores)
        (CASE 
            WHEN cm.stddev_company_conversion_rate > 0 
            THEN (ac.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
            ELSE 0
        END * 0.35 + 
        CASE 
            WHEN cm.stddev_company_roi > 0 
            THEN (ac.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
            ELSE 0
        END * 0.35 + 
        CASE 
            WHEN cm.stddev_company_ctr > 0 
            THEN (ac.avg_ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
            ELSE 0
        END * 0.15 + 
        CASE 
            WHEN cm.stddev_company_acquisition_cost > 0 
            THEN -1 * (ac.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
            ELSE 0
        END * 0.15) AS composite_performance_score,
        
        -- Add rankings within company
        ROW_NUMBER() OVER (PARTITION BY ac.Company ORDER BY ac.avg_roi DESC) AS roi_rank,
        ROW_NUMBER() OVER (PARTITION BY ac.Company ORDER BY ac.avg_conversion_rate DESC) AS conversion_rate_rank,
        ROW_NUMBER() OVER (PARTITION BY ac.Company ORDER BY ac.avg_acquisition_cost ASC) AS acquisition_cost_rank,
        
        -- Add rankings within company and target audience
        ROW_NUMBER() OVER (PARTITION BY ac.Company, ac.Target_Audience ORDER BY ac.avg_roi DESC) AS audience_roi_rank,
        ROW_NUMBER() OVER (PARTITION BY ac.Company, ac.Target_Audience ORDER BY ac.avg_conversion_rate DESC) AS audience_conversion_rate_rank
        
    FROM audience_clusters ac
    JOIN company_metrics cm ON ac.Company = cm.Company
    JOIN audience_metrics am ON ac.Company = am.Company AND ac.Target_Audience = am.Target_Audience
),

-- Calculate the total number of clusters per company for percentile calculations
company_cluster_counts AS (
    SELECT
        Company,
        COUNT(*) AS total_clusters
    FROM scored_clusters
    GROUP BY Company
)

-- Final output with performance tiers
SELECT
    sc.Company,
    sc.Target_Audience,
    sc.Location,
    sc.channel,
    sc.goal,
    sc.campaign_count,
    sc.avg_conversion_rate,
    sc.avg_roi,
    sc.avg_acquisition_cost,
    sc.avg_ctr,
    sc.total_spend,
    sc.total_revenue,
    
    -- Audience-level metrics
    sc.avg_audience_conversion_rate,
    sc.avg_audience_roi,
    sc.avg_audience_ctr,
    sc.avg_audience_acquisition_cost,
    sc.cluster_count,
    
    -- Z-scores
    sc.conversion_rate_z_score,
    sc.roi_z_score,
    sc.ctr_z_score,
    sc.cost_efficiency_z_score,
    sc.composite_performance_score,
    
    -- Rankings
    sc.roi_rank,
    sc.conversion_rate_rank,
    sc.acquisition_cost_rank,
    sc.audience_roi_rank,
    sc.audience_conversion_rate_rank,
    
    -- Calculate percentile rank within company (lower is better)
    CAST(sc.roi_rank AS FLOAT) / ccc.total_clusters AS roi_percentile,
    
    -- Flag high-performing clusters (top 10% by composite score within company)
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY sc.Company ORDER BY sc.composite_performance_score DESC) <= GREATEST(CAST(ccc.total_clusters * 0.1 AS INTEGER), 1)
        THEN TRUE 
        ELSE FALSE 
    END AS is_high_performing_cluster,
    
    -- Add performance tier based on composite score
    CASE
        WHEN sc.composite_performance_score >= 1.5 THEN 'Exceptional'
        WHEN sc.composite_performance_score >= 0.5 THEN 'High Performing'
        WHEN sc.composite_performance_score >= -0.5 THEN 'Average'
        WHEN sc.composite_performance_score >= -1.5 THEN 'Underperforming'
        ELSE 'Poor'
    END AS performance_tier,
    
    -- Recommended action based on performance
    CASE
        WHEN sc.composite_performance_score >= 1.5 THEN 'Increase investment'
        WHEN sc.composite_performance_score >= 0.5 THEN 'Maintain or increase'
        WHEN sc.composite_performance_score >= -0.5 THEN 'Monitor and optimize'
        WHEN sc.composite_performance_score >= -1.5 THEN 'Reduce investment'
        ELSE 'Consider discontinuing'
    END AS recommended_action
    
FROM scored_clusters sc
JOIN company_cluster_counts ccc ON sc.Company = ccc.Company
ORDER BY 
    sc.Company,
    sc.Target_Audience,
    sc.composite_performance_score DESC
