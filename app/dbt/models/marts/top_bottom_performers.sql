{{ config(materialized='table') }}

-- This model identifies top and bottom performers across different dimensions
-- such as companies, channels, and customer segments

-- First, get metrics by company
WITH recent_data AS (
    SELECT *
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) FROM {{ ref('stg_campaigns') }}) - 2
),
company_metrics AS (
    SELECT
        Company,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
        COUNT(*) as campaign_count
    FROM recent_data
    GROUP BY Company
),

-- Get metrics by channel
channel_metrics AS (
    SELECT
        Channel_Used as Channel,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
        COUNT(*) as campaign_count
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Channel_Used
),

-- Get metrics by customer segment
segment_metrics AS (
    SELECT
        Customer_Segment as Segment,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
        COUNT(*) as campaign_count
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Customer_Segment
),

-- Calculate global averages for comparison
global_metrics AS (
    SELECT
        AVG(Conversion_Rate) as global_avg_conversion_rate,
        AVG(ROI) as global_avg_roi,
        AVG(Acquisition_Cost) as global_avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as global_avg_ctr
    FROM {{ ref('stg_campaigns') }}
),

-- Rank companies by different metrics
company_rankings AS (
    SELECT
        Company,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        overall_ctr,
        campaign_count,
        
        -- Rankings (higher is better for conversion rate, ROI, CTR; lower is better for acquisition cost)
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate DESC) as conversion_rate_rank,
        ROW_NUMBER() OVER (ORDER BY avg_roi DESC) as roi_rank,
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost ASC) as acquisition_cost_rank,
        ROW_NUMBER() OVER (ORDER BY overall_ctr DESC) as ctr_rank,
        
        -- Reverse rankings for finding bottom performers
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate ASC) as conversion_rate_rank_asc,
        ROW_NUMBER() OVER (ORDER BY avg_roi ASC) as roi_rank_asc,
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost DESC) as acquisition_cost_rank_asc,
        ROW_NUMBER() OVER (ORDER BY overall_ctr ASC) as ctr_rank_asc,
        
        -- Count of companies for percentile calculations
        COUNT(*) OVER () as total_companies
    FROM company_metrics
),

-- Rank channels by different metrics
channel_rankings AS (
    SELECT
        Channel,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        overall_ctr,
        campaign_count,
        
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate DESC) as conversion_rate_rank,
        ROW_NUMBER() OVER (ORDER BY avg_roi DESC) as roi_rank,
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost ASC) as acquisition_cost_rank,
        ROW_NUMBER() OVER (ORDER BY overall_ctr DESC) as ctr_rank,
        
        -- Reverse rankings
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate ASC) as conversion_rate_rank_asc,
        ROW_NUMBER() OVER (ORDER BY avg_roi ASC) as roi_rank_asc,
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost DESC) as acquisition_cost_rank_asc,
        ROW_NUMBER() OVER (ORDER BY overall_ctr ASC) as ctr_rank_asc,
        
        -- Count of channels
        COUNT(*) OVER () as total_channels
    FROM channel_metrics
),

-- Rank segments by different metrics
segment_rankings AS (
    SELECT
        Segment,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        overall_ctr,
        campaign_count,
        
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate DESC) as conversion_rate_rank,
        ROW_NUMBER() OVER (ORDER BY avg_roi DESC) as roi_rank,
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost ASC) as acquisition_cost_rank,
        ROW_NUMBER() OVER (ORDER BY overall_ctr DESC) as ctr_rank,
        
        -- Reverse rankings
        ROW_NUMBER() OVER (ORDER BY avg_conversion_rate ASC) as conversion_rate_rank_asc,
        ROW_NUMBER() OVER (ORDER BY avg_roi ASC) as roi_rank_asc,
        ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost DESC) as acquisition_cost_rank_asc,
        ROW_NUMBER() OVER (ORDER BY overall_ctr ASC) as ctr_rank_asc,
        
        -- Count of segments
        COUNT(*) OVER () as total_segments
    FROM segment_metrics
)

-- Final output combining top and bottom performers across dimensions
SELECT
    'company' as dimension,
    Company as entity,
    'conversion_rate' as metric,
    avg_conversion_rate as value,
    conversion_rate_rank as rank,
    total_companies as total_entities,
    CASE 
        WHEN conversion_rate_rank = 1 THEN 'top'
        WHEN conversion_rate_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_conversion_rate FROM global_metrics) as global_avg,
    avg_conversion_rate / NULLIF((SELECT global_avg_conversion_rate FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM company_rankings
WHERE conversion_rate_rank = 1 OR conversion_rate_rank_asc = 1

UNION ALL

SELECT
    'company' as dimension,
    Company as entity,
    'roi' as metric,
    avg_roi as value,
    roi_rank as rank,
    total_companies as total_entities,
    CASE 
        WHEN roi_rank = 1 THEN 'top'
        WHEN roi_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_roi FROM global_metrics) as global_avg,
    avg_roi / NULLIF((SELECT global_avg_roi FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM company_rankings
WHERE roi_rank = 1 OR roi_rank_asc = 1

UNION ALL

SELECT
    'company' as dimension,
    Company as entity,
    'acquisition_cost' as metric,
    avg_acquisition_cost as value,
    acquisition_cost_rank as rank,
    total_companies as total_entities,
    CASE 
        WHEN acquisition_cost_rank = 1 THEN 'top'
        WHEN acquisition_cost_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_acquisition_cost FROM global_metrics) as global_avg,
    avg_acquisition_cost / NULLIF((SELECT global_avg_acquisition_cost FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM company_rankings
WHERE acquisition_cost_rank = 1 OR acquisition_cost_rank_asc = 1

UNION ALL

SELECT
    'company' as dimension,
    Company as entity,
    'ctr' as metric,
    overall_ctr as value,
    ctr_rank as rank,
    total_companies as total_entities,
    CASE 
        WHEN ctr_rank = 1 THEN 'top'
        WHEN ctr_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_ctr FROM global_metrics) as global_avg,
    overall_ctr / NULLIF((SELECT global_avg_ctr FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM company_rankings
WHERE ctr_rank = 1 OR ctr_rank_asc = 1

UNION ALL

-- Channel top/bottom performers
SELECT
    'channel' as dimension,
    Channel as entity,
    'conversion_rate' as metric,
    avg_conversion_rate as value,
    conversion_rate_rank as rank,
    total_channels as total_entities,
    CASE 
        WHEN conversion_rate_rank = 1 THEN 'top'
        WHEN conversion_rate_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_conversion_rate FROM global_metrics) as global_avg,
    avg_conversion_rate / NULLIF((SELECT global_avg_conversion_rate FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM channel_rankings
WHERE conversion_rate_rank = 1 OR conversion_rate_rank_asc = 1

UNION ALL

SELECT
    'channel' as dimension,
    Channel as entity,
    'roi' as metric,
    avg_roi as value,
    roi_rank as rank,
    total_channels as total_entities,
    CASE 
        WHEN roi_rank = 1 THEN 'top'
        WHEN roi_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_roi FROM global_metrics) as global_avg,
    avg_roi / NULLIF((SELECT global_avg_roi FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM channel_rankings
WHERE roi_rank = 1 OR roi_rank_asc = 1

UNION ALL

-- Segment top/bottom performers (just showing ROI for brevity)
SELECT
    'segment' as dimension,
    Segment as entity,
    'roi' as metric,
    avg_roi as value,
    roi_rank as rank,
    total_segments as total_entities,
    CASE 
        WHEN roi_rank = 1 THEN 'top'
        WHEN roi_rank_asc = 1 THEN 'bottom'
        ELSE 'middle'
    END as performance,
    (SELECT global_avg_roi FROM global_metrics) as global_avg,
    avg_roi / NULLIF((SELECT global_avg_roi FROM global_metrics), 0) - 1 as pct_diff_from_avg
FROM segment_rankings
WHERE roi_rank = 1 OR roi_rank_asc = 1

ORDER BY dimension, metric, performance DESC