{{ config(materialized='table') }}

/*
Model: metrics_quarterly_ranked
Time Context: Quarterly (current quarter vs previous quarter)

Description:
This model ranks all entities (companies, channels, segments) within their respective dimensions
based on key performance metrics, with quarter-over-quarter comparison.

Key Features:
1. Ranks entities within dimensions for conversion rate, ROI, acquisition cost, and CTR
2. Calculates quarter-over-quarter changes for all metrics
3. Identifies performance trends (improved/declined/unchanged)
4. Provides relative performance against dimension averages

Dashboard Usage:
- Performance Rankings section
- Quarter-over-Quarter Comparison charts
*/

WITH date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month,
        -- Use ANY_VALUE for quarter and year since we just need a representative value
        ANY_VALUE(EXTRACT(QUARTER FROM CAST(Date AS DATE))) AS current_quarter,
        ANY_VALUE(EXTRACT(YEAR FROM CAST(Date AS DATE))) AS current_year
    FROM {{ ref('stg_campaigns') }}
    -- No GROUP BY needed with MAX and ANY_VALUE aggregates
),

-- Current quarter data (last 3 months)
current_quarter_data AS (
    SELECT *
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
),

-- Previous quarter data (3 months before the current quarter)
previous_quarter_data AS (
    SELECT *
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 5 FROM date_ranges)
      AND EXTRACT(MONTH FROM CAST(Date AS DATE)) < (SELECT current_max_month - 2 FROM date_ranges)
),

-- Current quarter company metrics
current_company_metrics AS (
    SELECT
        'company' AS dimension,
        Company AS entity,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        CASE 
            WHEN SUM(Impressions) > 0 THEN SUM(Clicks)::FLOAT / SUM(Impressions)
            ELSE 0
        END AS overall_ctr
    FROM current_quarter_data
    GROUP BY Company
),

-- Previous quarter company metrics
previous_company_metrics AS (
    SELECT
        'company' AS dimension,
        Company AS entity,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        CASE 
            WHEN SUM(Impressions) > 0 THEN SUM(Clicks)::FLOAT / SUM(Impressions)
            ELSE 0
        END AS overall_ctr
    FROM previous_quarter_data
    GROUP BY Company
),

-- Current quarter channel metrics
current_channel_metrics AS (
    SELECT
        'channel' AS dimension,
        Channel_Used AS entity,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        CASE 
            WHEN SUM(Impressions) > 0 THEN SUM(Clicks)::FLOAT / SUM(Impressions)
            ELSE 0
        END AS overall_ctr
    FROM current_quarter_data
    GROUP BY Channel_Used
),

-- Previous quarter channel metrics
previous_channel_metrics AS (
    SELECT
        'channel' AS dimension,
        Channel_Used AS entity,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        CASE 
            WHEN SUM(Impressions) > 0 THEN SUM(Clicks)::FLOAT / SUM(Impressions)
            ELSE 0
        END AS overall_ctr
    FROM previous_quarter_data
    GROUP BY Channel_Used
),

-- Current quarter segment metrics
current_segment_metrics AS (
    SELECT
        'segment' AS dimension,
        Customer_Segment AS entity,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        CASE 
            WHEN SUM(Impressions) > 0 THEN SUM(Clicks)::FLOAT / SUM(Impressions)
            ELSE 0
        END AS overall_ctr
    FROM current_quarter_data
    GROUP BY Customer_Segment
),

-- Previous quarter segment metrics
previous_segment_metrics AS (
    SELECT
        'segment' AS dimension,
        Customer_Segment AS entity,
        COUNT(*) AS campaign_count,
        AVG(Conversion_Rate) AS avg_conversion_rate,
        AVG(ROI) AS avg_roi,
        AVG(Acquisition_Cost) AS avg_acquisition_cost,
        SUM(Clicks) AS total_clicks,
        SUM(Impressions) AS total_impressions,
        CASE 
            WHEN SUM(Impressions) > 0 THEN SUM(Clicks)::FLOAT / SUM(Impressions)
            ELSE 0
        END AS overall_ctr
    FROM previous_quarter_data
    GROUP BY Customer_Segment
),

-- Combine all current quarter metrics
current_all_metrics AS (
    SELECT * FROM current_company_metrics
    UNION ALL
    SELECT * FROM current_channel_metrics
    UNION ALL
    SELECT * FROM current_segment_metrics
),

-- Combine all previous quarter metrics
previous_all_metrics AS (
    SELECT * FROM previous_company_metrics
    UNION ALL
    SELECT * FROM previous_channel_metrics
    UNION ALL
    SELECT * FROM previous_segment_metrics
),

-- Add rankings for each metric within each dimension (current quarter)
ranked_current_metrics AS (
    SELECT
        dimension,
        entity,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        overall_ctr,
        ROW_NUMBER() OVER (PARTITION BY dimension ORDER BY avg_conversion_rate DESC) AS conversion_rate_rank,
        ROW_NUMBER() OVER (PARTITION BY dimension ORDER BY avg_roi DESC) AS roi_rank,
        ROW_NUMBER() OVER (PARTITION BY dimension ORDER BY avg_acquisition_cost ASC) AS acquisition_cost_rank,
        ROW_NUMBER() OVER (PARTITION BY dimension ORDER BY overall_ctr DESC) AS ctr_rank,
        COUNT(*) OVER (PARTITION BY dimension) AS total_entities
    FROM current_all_metrics
),

-- Join current and previous quarter data to calculate quarter-over-quarter changes
metrics_with_qoq AS (
    SELECT
        c.dimension,
        c.entity,
        c.campaign_count,
        c.avg_conversion_rate,
        c.avg_roi,
        c.avg_acquisition_cost,
        c.overall_ctr,
        c.conversion_rate_rank,
        c.roi_rank,
        c.acquisition_cost_rank,
        c.ctr_rank,
        c.total_entities,
        
        -- Previous quarter metrics (if available)
        p.avg_conversion_rate AS prev_avg_conversion_rate,
        p.avg_roi AS prev_avg_roi,
        p.avg_acquisition_cost AS prev_avg_acquisition_cost,
        p.overall_ctr AS prev_overall_ctr,
        
        -- Quarter-over-quarter changes
        CASE 
            WHEN p.avg_conversion_rate IS NOT NULL THEN (c.avg_conversion_rate - p.avg_conversion_rate) / NULLIF(p.avg_conversion_rate, 0)
            ELSE NULL
        END AS conversion_rate_qoq_change,
        
        CASE 
            WHEN p.avg_roi IS NOT NULL THEN (c.avg_roi - p.avg_roi) / NULLIF(p.avg_roi, 0)
            ELSE NULL
        END AS roi_qoq_change,
        
        CASE 
            WHEN p.avg_acquisition_cost IS NOT NULL THEN (p.avg_acquisition_cost - c.avg_acquisition_cost) / NULLIF(p.avg_acquisition_cost, 0)
            ELSE NULL
        END AS acquisition_cost_qoq_change,
        
        CASE 
            WHEN p.overall_ctr IS NOT NULL THEN (c.overall_ctr - p.overall_ctr) / NULLIF(p.overall_ctr, 0)
            ELSE NULL
        END AS ctr_qoq_change
        
    FROM ranked_current_metrics c
    LEFT JOIN previous_all_metrics p ON c.dimension = p.dimension AND c.entity = p.entity
),

-- Unpivot to get one row per dimension-entity-metric combination
unpivoted AS (
    -- Conversion Rate
    SELECT 
        dimension, 
        entity, 
        'conversion_rate' AS metric, 
        avg_conversion_rate AS metric_value, 
        conversion_rate_rank AS metric_rank,
        total_entities,
        campaign_count,
        prev_avg_conversion_rate AS prev_metric_value,
        conversion_rate_qoq_change AS metric_qoq_change
    FROM metrics_with_qoq
    
    UNION ALL
    
    -- ROI
    SELECT 
        dimension, 
        entity, 
        'roi' AS metric, 
        avg_roi AS metric_value, 
        roi_rank AS metric_rank,
        total_entities,
        campaign_count,
        prev_avg_roi AS prev_metric_value,
        roi_qoq_change AS metric_qoq_change
    FROM metrics_with_qoq
    
    UNION ALL
    
    -- Acquisition Cost
    SELECT 
        dimension, 
        entity, 
        'acquisition_cost' AS metric, 
        avg_acquisition_cost AS metric_value, 
        acquisition_cost_rank AS metric_rank,
        total_entities,
        campaign_count,
        prev_avg_acquisition_cost AS prev_metric_value,
        acquisition_cost_qoq_change AS metric_qoq_change
    FROM metrics_with_qoq
    
    UNION ALL
    
    -- CTR
    SELECT 
        dimension, 
        entity, 
        'ctr' AS metric, 
        overall_ctr AS metric_value, 
        ctr_rank AS metric_rank,
        total_entities,
        campaign_count,
        prev_overall_ctr AS prev_metric_value,
        ctr_qoq_change AS metric_qoq_change
    FROM metrics_with_qoq
),

-- Get window dates for reference
window_dates AS (
    SELECT
        MIN(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_window_start_month,
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_window_end_month,
        MIN(EXTRACT(MONTH FROM CAST(Date AS DATE))) - 3 AS previous_window_start_month,
        MIN(EXTRACT(MONTH FROM CAST(Date AS DATE))) - 1 AS previous_window_end_month
    FROM current_quarter_data
)

-- Final output
SELECT
    u.dimension,
    u.entity,
    u.metric,
    u.metric_value,
    u.metric_rank,
    u.total_entities,
    u.campaign_count,
    u.prev_metric_value,
    u.metric_qoq_change,
    CASE
        WHEN u.metric_qoq_change > 0 AND u.metric != 'acquisition_cost' THEN 'improved'
        WHEN u.metric_qoq_change < 0 AND u.metric = 'acquisition_cost' THEN 'improved'
        WHEN u.metric_qoq_change < 0 AND u.metric != 'acquisition_cost' THEN 'declined'
        WHEN u.metric_qoq_change > 0 AND u.metric = 'acquisition_cost' THEN 'declined'
        WHEN u.metric_qoq_change = 0 THEN 'unchanged'
        ELSE 'no_previous_data'
    END AS trend,
    wd.current_window_start_month,
    wd.current_window_end_month,
    wd.previous_window_start_month,
    wd.previous_window_end_month
FROM unpivoted u
CROSS JOIN window_dates wd
ORDER BY u.dimension, u.metric, u.metric_rank
