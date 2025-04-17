{{ config(materialized='table') }}

/*
Model: segment_company_historical_rankings
Time Context: Historical (all available data)

Description:
This model ranks companies within each customer segment based on key performance metrics
across all historical data to identify which companies perform best for specific segments.

Key Features:
1. Calculates performance metrics by segment-company combinations
2. Ranks companies within each segment across multiple metrics
3. Provides relative performance indicators against segment averages
4. Identifies top performers for each customer segment

Dashboard Usage:
- Segment Performance Rankings section
- Company-Segment Affinity analysis
- Targeting Strategy recommendations
*/

WITH 
-- First, get metrics by segment and company
segment_company_metrics AS (
    SELECT
        Customer_Segment,
        Company,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
        COUNT(*) as campaign_count
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Customer_Segment, Company
),

-- Rank companies within each segment by different metrics
company_rankings_by_segment AS (
    SELECT
        Customer_Segment,
        Company,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        overall_ctr,
        campaign_count,
        
        -- Rank companies within each segment by conversion rate (descending)
        ROW_NUMBER() OVER (
            PARTITION BY Customer_Segment 
            ORDER BY avg_conversion_rate DESC
        ) as conversion_rate_rank,
        
        -- Rank companies within each segment by ROI (descending)
        ROW_NUMBER() OVER (
            PARTITION BY Customer_Segment 
            ORDER BY avg_roi DESC
        ) as roi_rank,
        
        -- Rank companies within each segment by acquisition cost (ascending - lower is better)
        ROW_NUMBER() OVER (
            PARTITION BY Customer_Segment 
            ORDER BY avg_acquisition_cost ASC
        ) as acquisition_cost_rank,
        
        -- Rank companies within each segment by CTR (descending)
        ROW_NUMBER() OVER (
            PARTITION BY Customer_Segment 
            ORDER BY overall_ctr DESC
        ) as ctr_rank,
        
        -- Count total companies per segment for context
        COUNT(*) OVER (PARTITION BY Customer_Segment) as total_companies_per_segment
    FROM segment_company_metrics
),

-- Global averages for comparison
global_metrics AS (
    SELECT
        AVG(avg_conversion_rate) as global_avg_conversion_rate,
        AVG(avg_roi) as global_avg_roi,
        AVG(avg_acquisition_cost) as global_avg_acquisition_cost,
        AVG(overall_ctr) as global_avg_ctr
    FROM segment_company_metrics
),

-- Segment averages for comparison
segment_metrics AS (
    SELECT
        Customer_Segment,
        AVG(avg_conversion_rate) as segment_avg_conversion_rate,
        AVG(avg_roi) as segment_avg_roi,
        AVG(avg_acquisition_cost) as segment_avg_acquisition_cost,
        AVG(overall_ctr) as segment_avg_ctr,
        COUNT(DISTINCT Company) as company_count
    FROM segment_company_metrics
    GROUP BY Customer_Segment
)

-- Final output combining rankings and comparisons
SELECT
    -- Segment and company information
    cr.Customer_Segment,
    cr.Company,
    
    -- Performance metrics
    cr.avg_conversion_rate,
    cr.avg_roi,
    cr.avg_acquisition_cost,
    cr.overall_ctr,
    cr.campaign_count,
    
    -- Segment-specific company rankings
    cr.conversion_rate_rank,
    cr.roi_rank,
    cr.acquisition_cost_rank,
    cr.ctr_rank,
    cr.total_companies_per_segment,
    
    -- Is this the best company for this segment by different metrics?
    CASE WHEN cr.conversion_rate_rank = 1 THEN TRUE ELSE FALSE END as is_top_conversion_company,
    CASE WHEN cr.roi_rank = 1 THEN TRUE ELSE FALSE END as is_top_roi_company,
    CASE WHEN cr.acquisition_cost_rank = 1 THEN TRUE ELSE FALSE END as is_top_acquisition_cost_company,
    CASE WHEN cr.ctr_rank = 1 THEN TRUE ELSE FALSE END as is_top_ctr_company,
    
    -- Segment averages for context
    sm.segment_avg_conversion_rate,
    sm.segment_avg_roi,
    sm.segment_avg_acquisition_cost,
    sm.segment_avg_ctr,
    sm.company_count,
    
    -- Performance relative to segment average
    cr.avg_conversion_rate / NULLIF(sm.segment_avg_conversion_rate, 0) - 1 as conversion_rate_vs_segment_avg,
    cr.avg_roi / NULLIF(sm.segment_avg_roi, 0) - 1 as roi_vs_segment_avg,
    sm.segment_avg_acquisition_cost / NULLIF(cr.avg_acquisition_cost, 0) - 1 as acquisition_cost_vs_segment_avg,
    cr.overall_ctr / NULLIF(sm.segment_avg_ctr, 0) - 1 as ctr_vs_segment_avg,
    
    -- Performance relative to global average
    cr.avg_conversion_rate / NULLIF((SELECT global_avg_conversion_rate FROM global_metrics), 0) - 1 as conversion_rate_vs_global_avg,
    cr.avg_roi / NULLIF((SELECT global_avg_roi FROM global_metrics), 0) - 1 as roi_vs_global_avg,
    (SELECT global_avg_acquisition_cost FROM global_metrics) / NULLIF(cr.avg_acquisition_cost, 0) - 1 as acquisition_cost_vs_global_avg,
    cr.overall_ctr / NULLIF((SELECT global_avg_ctr FROM global_metrics), 0) - 1 as ctr_vs_global_avg
FROM company_rankings_by_segment cr
JOIN segment_metrics sm ON cr.Customer_Segment = sm.Customer_Segment
ORDER BY 
    cr.Customer_Segment,
    cr.roi_rank
