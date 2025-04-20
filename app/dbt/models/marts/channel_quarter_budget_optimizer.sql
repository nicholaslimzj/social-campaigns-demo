{{ config(materialized='table') }}

/*
Model: channel_quarter_budget_optimizer
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model analyzes channel performance and efficiency to provide optimal budget allocation
recommendations across channels based on historical spend data.

Key Features:
1. Channel as the primary dimension
2. Calculates key efficiency metrics (ROI, CPA, marginal returns)
3. Determines optimal spend allocation percentages based on historical performance
4. Provides projected performance improvements
5. Compares optimal to current allocation
6. Handles minimum spend requirements and maximum capacity constraints

Dashboard Usage:
- Budget Allocation Optimizer interactive tool
- Channel ROI Efficiency visualization
- Spend Optimization Recommendations
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

-- Calculate channel performance metrics
channel_performance AS (
    SELECT
        Channel_Used,
        COUNT(*) AS campaign_count,
        COUNT(DISTINCT Company) AS company_count,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        SUM(Clicks * Acquisition_Cost) AS total_spend,
        SUM(Clicks * Acquisition_Cost * ROI) AS total_revenue,
        AVG(ROI) AS avg_roi,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) AS avg_ctr
    FROM current_quarter_data
    GROUP BY Channel_Used
),

-- Calculate channel efficiency metrics
channel_efficiency AS (
    SELECT
        Channel_Used,
        campaign_count,
        company_count,
        total_clicks,
        total_impressions,
        total_spend,
        total_revenue,
        avg_roi,
        avg_conversion_rate,
        avg_acquisition_cost,
        avg_ctr,
        -- Efficiency metrics
        CASE 
            WHEN total_spend > 0 THEN total_revenue / total_spend 
            ELSE 0 
        END AS roi_efficiency,
        CASE 
            WHEN total_clicks > 0 THEN total_spend / total_clicks 
            ELSE 0 
        END AS cost_per_click,
        CASE 
            WHEN total_impressions > 0 THEN total_clicks::FLOAT / total_impressions 
            ELSE 0 
        END AS click_through_rate,
        -- Spend share
        CASE 
            WHEN SUM(total_spend) OVER () > 0 THEN total_spend / SUM(total_spend) OVER () 
            ELSE 0 
        END AS current_spend_share,
        -- Revenue share
        CASE 
            WHEN SUM(total_revenue) OVER () > 0 THEN total_revenue / SUM(total_revenue) OVER () 
            ELSE 0 
        END AS current_revenue_share
    FROM channel_performance
),

-- Calculate marginal returns (simplified model)
marginal_returns AS (
    SELECT
        Channel_Used,
        roi_efficiency,
        -- Diminishing returns model (simplified)
        -- Assumes ROI decreases as spend increases
        -- This is a simplified model and could be replaced with more sophisticated regression analysis
        CASE
            WHEN roi_efficiency > 0 THEN 
                CASE
                    WHEN current_spend_share < 0.1 THEN roi_efficiency * 1.2  -- Low spend, high potential
                    WHEN current_spend_share < 0.2 THEN roi_efficiency * 1.1  -- Medium-low spend
                    WHEN current_spend_share < 0.3 THEN roi_efficiency        -- Medium spend
                    WHEN current_spend_share < 0.4 THEN roi_efficiency * 0.9  -- Medium-high spend
                    ELSE roi_efficiency * 0.8                                 -- High spend, diminishing returns
                END
            ELSE 0
        END AS marginal_roi,
        -- Minimum spend requirements (5% of total or current, whichever is lower)
        CASE
            WHEN current_spend_share < 0.05 THEN current_spend_share
            ELSE 0.05
        END AS min_spend_share,
        -- Maximum capacity constraints (no more than double current share or 40%, whichever is lower)
        CASE
            WHEN current_spend_share * 2 < 0.4 THEN current_spend_share * 2
            ELSE 0.4
        END AS max_spend_share
    FROM channel_efficiency
),

-- Calculate optimal spend allocation
optimal_allocation AS (
    SELECT
        mr.Channel_Used,
        mr.roi_efficiency,
        mr.marginal_roi,
        ce.current_spend_share,
        ce.current_revenue_share,
        mr.min_spend_share,
        mr.max_spend_share,
        -- Rank channels by marginal ROI
        RANK() OVER (ORDER BY mr.marginal_roi DESC) AS efficiency_rank,
        -- Calculate optimal spend share (simplified model)
        -- In a real implementation, this would use a more sophisticated optimization algorithm
        CASE
            -- High efficiency channels get more budget
            WHEN RANK() OVER (ORDER BY mr.marginal_roi DESC) = 1 THEN 
                LEAST(mr.max_spend_share, GREATEST(mr.min_spend_share, 0.35))
            WHEN RANK() OVER (ORDER BY mr.marginal_roi DESC) = 2 THEN 
                LEAST(mr.max_spend_share, GREATEST(mr.min_spend_share, 0.25))
            WHEN RANK() OVER (ORDER BY mr.marginal_roi DESC) = 3 THEN 
                LEAST(mr.max_spend_share, GREATEST(mr.min_spend_share, 0.20))
            WHEN RANK() OVER (ORDER BY mr.marginal_roi DESC) = 4 THEN 
                LEAST(mr.max_spend_share, GREATEST(mr.min_spend_share, 0.15))
            ELSE 
                LEAST(mr.max_spend_share, GREATEST(mr.min_spend_share, 0.05))
        END AS raw_optimal_share
    FROM marginal_returns mr
    JOIN channel_efficiency ce ON mr.Channel_Used = ce.Channel_Used
),

-- Normalize optimal allocation to ensure it sums to 100%
normalized_allocation AS (
    SELECT
        Channel_Used,
        roi_efficiency,
        marginal_roi,
        current_spend_share,
        current_revenue_share,
        min_spend_share,
        max_spend_share,
        efficiency_rank,
        raw_optimal_share,
        -- Normalize to ensure sum is 100%
        raw_optimal_share / SUM(raw_optimal_share) OVER () AS optimal_spend_share
    FROM optimal_allocation
)

-- Final output with projected improvements
SELECT
    na.Channel_Used,
    ce.campaign_count,
    ce.company_count,
    ce.total_spend,
    ce.total_revenue,
    ce.avg_roi,
    ce.avg_conversion_rate,
    ce.avg_acquisition_cost,
    ce.avg_ctr,
    ce.roi_efficiency,
    ce.cost_per_click,
    ce.click_through_rate,
    na.current_spend_share,
    na.current_revenue_share,
    na.efficiency_rank,
    na.optimal_spend_share,
    -- Spend change
    na.optimal_spend_share - na.current_spend_share AS spend_share_change,
    -- Absolute spend values
    ce.total_spend AS current_spend,
    (SELECT SUM(total_spend) FROM channel_efficiency) * na.optimal_spend_share AS optimal_spend,
    (SELECT SUM(total_spend) FROM channel_efficiency) * na.optimal_spend_share - ce.total_spend AS spend_change,
    -- Projected improvements
    -- Simplified model assuming linear relationship with some diminishing returns
    CASE
        WHEN na.optimal_spend_share > na.current_spend_share THEN
            -- Increasing spend: apply diminishing returns
            ce.total_revenue * (1 + (na.optimal_spend_share / na.current_spend_share - 1) * 0.8)
        WHEN na.optimal_spend_share < na.current_spend_share THEN
            -- Decreasing spend: assume proportional decrease with some efficiency gain
            ce.total_revenue * (na.optimal_spend_share / na.current_spend_share) * 1.1
        ELSE
            -- No change
            ce.total_revenue
    END AS projected_revenue,
    -- Projected ROI
    CASE
        WHEN na.optimal_spend_share > 0 THEN
            CASE
                WHEN na.optimal_spend_share > na.current_spend_share THEN
                    -- Increasing spend: apply diminishing returns
                    ce.avg_roi * (1 - (na.optimal_spend_share / na.current_spend_share - 1) * 0.2)
                WHEN na.optimal_spend_share < na.current_spend_share THEN
                    -- Decreasing spend: assume efficiency gain
                    ce.avg_roi * (1 + (na.current_spend_share / na.optimal_spend_share - 1) * 0.1)
                ELSE
                    -- No change
                    ce.avg_roi
            END
        ELSE 0
    END AS projected_roi,
    -- Projected improvement percentage
    CASE
        WHEN ce.total_revenue > 0 THEN
            (CASE
                WHEN na.optimal_spend_share > na.current_spend_share THEN
                    -- Increasing spend: apply diminishing returns
                    ce.total_revenue * (1 + (na.optimal_spend_share / na.current_spend_share - 1) * 0.8)
                WHEN na.optimal_spend_share < na.current_spend_share THEN
                    -- Decreasing spend: assume proportional decrease with some efficiency gain
                    ce.total_revenue * (na.optimal_spend_share / na.current_spend_share) * 1.1
                ELSE
                    -- No change
                    ce.total_revenue
            END - ce.total_revenue) / ce.total_revenue
        ELSE 0
    END AS projected_improvement_pct,
    -- Recommendation strength
    CASE
        WHEN ABS(na.optimal_spend_share - na.current_spend_share) < 0.05 THEN 'minor_adjustment'
        WHEN ABS(na.optimal_spend_share - na.current_spend_share) < 0.15 THEN 'moderate_adjustment'
        ELSE 'major_adjustment'
    END AS recommendation_strength,
    -- Recommendation direction
    CASE
        WHEN na.optimal_spend_share > na.current_spend_share THEN 'increase_spend'
        WHEN na.optimal_spend_share < na.current_spend_share THEN 'decrease_spend'
        ELSE 'maintain_spend'
    END AS recommendation_direction
FROM normalized_allocation na
JOIN channel_efficiency ce ON na.Channel_Used = ce.Channel_Used
ORDER BY na.efficiency_rank
