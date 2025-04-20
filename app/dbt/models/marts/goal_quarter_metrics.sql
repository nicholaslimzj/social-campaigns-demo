{{ config(materialized='table') }}

/*
Model: goal_quarter_metrics
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model aggregates campaign performance metrics by company and campaign goal for the current quarter,
following the hierarchical dimension structure with Company as the primary dimension.

Key Features:
1. Company as the primary dimension
2. Campaign_Goal as the secondary dimension
3. Quarterly time grain for strategic analysis
4. Aggregates key performance metrics by company and goal
5. Calculates goal-specific quarter-over-quarter changes
6. Provides spend and revenue calculations

Dashboard Usage:
- Campaign Goals Analysis bar chart
- Goal Performance Comparison
- Goal-specific ROI Analysis
*/

WITH 
-- First, determine the date range for the current quarter
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month
    FROM {{ ref('stg_campaigns') }}
),

-- Calculate the quarter for each date
with_quarters AS (
    SELECT 
        *,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) / 3 + EXTRACT(YEAR FROM CAST(Date AS DATE)) * 4 AS quarter
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
),

-- Get goal metrics by company for the current quarter only
goal_quarterly_metrics AS (
    SELECT 
        Company,
        Campaign_Goal as goal,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr,
        SUM(Clicks * Acquisition_Cost) as total_spend,
        SUM(Clicks * Acquisition_Cost * ROI) as total_revenue
    FROM with_quarters
    WHERE quarter = (SELECT MAX(quarter) FROM with_quarters) -- Only use the current quarter
    GROUP BY Company, Campaign_Goal
),

-- Get previous quarter metrics for comparison
previous_quarter_metrics AS (
    SELECT 
        Company,
        Campaign_Goal as goal,
        AVG(Conversion_Rate) as prev_conversion_rate,
        AVG(ROI) as prev_roi,
        AVG(Acquisition_Cost) as prev_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as prev_ctr
    FROM with_quarters
    WHERE quarter = (SELECT MAX(quarter) - 1 FROM with_quarters) -- Previous quarter
    GROUP BY Company, Campaign_Goal
),

-- Calculate company-level metrics for normalization
company_metrics AS (
    SELECT
        Company,
        AVG(avg_conversion_rate) AS avg_company_conversion_rate,
        STDDEV(avg_conversion_rate) AS stddev_company_conversion_rate,
        AVG(avg_roi) AS avg_company_roi,
        STDDEV(avg_roi) AS stddev_company_roi,
        AVG(ctr) AS avg_company_ctr,
        STDDEV(ctr) AS stddev_company_ctr,
        AVG(avg_acquisition_cost) AS avg_company_acquisition_cost,
        STDDEV(avg_acquisition_cost) AS stddev_company_acquisition_cost
    FROM goal_quarterly_metrics
    GROUP BY Company
)

SELECT
    gqm.Company,
    gqm.goal,
    gqm.campaign_count,
    gqm.avg_conversion_rate,
    gqm.avg_roi,
    gqm.avg_acquisition_cost,
    gqm.ctr,
    gqm.total_clicks,
    gqm.total_impressions,
    gqm.total_spend,
    gqm.total_revenue,
    
    -- Calculate goal-specific quarter-over-quarter percentage changes
    CASE 
        WHEN pqm.prev_roi IS NULL OR pqm.prev_roi = 0 THEN NULL
        ELSE (gqm.avg_roi - pqm.prev_roi) / pqm.prev_roi 
    END as roi_vs_prev_quarter,
    
    CASE 
        WHEN pqm.prev_conversion_rate IS NULL OR pqm.prev_conversion_rate = 0 THEN NULL
        ELSE (gqm.avg_conversion_rate - pqm.prev_conversion_rate) / pqm.prev_conversion_rate 
    END as conversion_rate_vs_prev_quarter,
    
    CASE 
        WHEN pqm.prev_acquisition_cost IS NULL OR pqm.prev_acquisition_cost = 0 THEN NULL
        ELSE (pqm.prev_acquisition_cost - gqm.avg_acquisition_cost) / pqm.prev_acquisition_cost -- Note: for cost, lower is better
    END as acquisition_cost_vs_prev_quarter,
    
    CASE 
        WHEN pqm.prev_ctr IS NULL OR pqm.prev_ctr = 0 THEN NULL
        ELSE (gqm.ctr - pqm.prev_ctr) / pqm.prev_ctr 
    END as ctr_vs_prev_quarter,
    
    -- Add company-specific rank by ROI
    RANK() OVER (PARTITION BY gqm.Company ORDER BY gqm.avg_roi DESC) as roi_rank,
    
    -- Add company-specific rank by conversion rate
    RANK() OVER (PARTITION BY gqm.Company ORDER BY gqm.avg_conversion_rate DESC) as conversion_rate_rank,
    
    -- Calculate normalized scores (z-scores) for key metrics
    CASE 
        WHEN cm.stddev_company_conversion_rate > 0 
        THEN (gqm.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
        ELSE 0
    END AS conversion_rate_z_score,
    
    CASE 
        WHEN cm.stddev_company_roi > 0 
        THEN (gqm.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
        ELSE 0
    END AS roi_z_score,
    
    CASE 
        WHEN cm.stddev_company_ctr > 0 
        THEN (gqm.ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
        ELSE 0
    END AS ctr_z_score,
    
    CASE 
        WHEN cm.stddev_company_acquisition_cost > 0 
        THEN -1 * (gqm.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
        ELSE 0
    END AS cost_efficiency_z_score,
    
    -- Calculate composite performance score (weighted average of z-scores)
    (CASE 
        WHEN cm.stddev_company_conversion_rate > 0 
        THEN (gqm.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
        ELSE 0
    END * 0.35 + 
    CASE 
        WHEN cm.stddev_company_roi > 0 
        THEN (gqm.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
        ELSE 0
    END * 0.35 + 
    CASE 
        WHEN cm.stddev_company_ctr > 0 
        THEN (gqm.ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
        ELSE 0
    END * 0.15 + 
    CASE 
        WHEN cm.stddev_company_acquisition_cost > 0 
        THEN -1 * (gqm.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
        ELSE 0
    END * 0.15) AS composite_performance_score,
    
    -- Add performance tier based on composite score
    CASE
        WHEN (CASE 
                WHEN cm.stddev_company_conversion_rate > 0 
                THEN (gqm.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_roi > 0 
                THEN (gqm.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_ctr > 0 
                THEN (gqm.ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
                ELSE 0
            END * 0.15 + 
            CASE 
                WHEN cm.stddev_company_acquisition_cost > 0 
                THEN -1 * (gqm.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
                ELSE 0
            END * 0.15) >= 1.5 THEN 'Exceptional'
        WHEN (CASE 
                WHEN cm.stddev_company_conversion_rate > 0 
                THEN (gqm.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_roi > 0 
                THEN (gqm.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_ctr > 0 
                THEN (gqm.ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
                ELSE 0
            END * 0.15 + 
            CASE 
                WHEN cm.stddev_company_acquisition_cost > 0 
                THEN -1 * (gqm.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
                ELSE 0
            END * 0.15) >= 0.5 THEN 'High Performing'
        WHEN (CASE 
                WHEN cm.stddev_company_conversion_rate > 0 
                THEN (gqm.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_roi > 0 
                THEN (gqm.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_ctr > 0 
                THEN (gqm.ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
                ELSE 0
            END * 0.15 + 
            CASE 
                WHEN cm.stddev_company_acquisition_cost > 0 
                THEN -1 * (gqm.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
                ELSE 0
            END * 0.15) >= -0.5 THEN 'Average'
        WHEN (CASE 
                WHEN cm.stddev_company_conversion_rate > 0 
                THEN (gqm.avg_conversion_rate - cm.avg_company_conversion_rate) / cm.stddev_company_conversion_rate
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_roi > 0 
                THEN (gqm.avg_roi - cm.avg_company_roi) / cm.stddev_company_roi
                ELSE 0
            END * 0.35 + 
            CASE 
                WHEN cm.stddev_company_ctr > 0 
                THEN (gqm.ctr - cm.avg_company_ctr) / cm.stddev_company_ctr
                ELSE 0
            END * 0.15 + 
            CASE 
                WHEN cm.stddev_company_acquisition_cost > 0 
                THEN -1 * (gqm.avg_acquisition_cost - cm.avg_company_acquisition_cost) / cm.stddev_company_acquisition_cost
                ELSE 0
            END * 0.15) >= -1.5 THEN 'Underperforming'
        ELSE 'Poor'
    END AS performance_tier
    
FROM goal_quarterly_metrics gqm
LEFT JOIN previous_quarter_metrics pqm
    ON gqm.Company = pqm.Company AND gqm.goal = pqm.goal
LEFT JOIN company_metrics cm
    ON gqm.Company = cm.Company
ORDER BY gqm.Company, gqm.goal
