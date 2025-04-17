{{ config(materialized='table') }}

-- This model identifies which segments perform best for each company
-- and analyzes segment performance within similar company types

WITH 
-- First, get metrics by company and segment
company_segment_metrics AS (
    SELECT
        Company,
        Customer_Segment,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
        COUNT(*) as campaign_count
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Company, Customer_Segment
),

-- Rank segments within each company by different metrics
segment_rankings_by_company AS (
    SELECT
        Company,
        Customer_Segment,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        overall_ctr,
        campaign_count,
        
        -- Rank segments within each company by conversion rate (descending)
        ROW_NUMBER() OVER (
            PARTITION BY Company 
            ORDER BY avg_conversion_rate DESC
        ) as conversion_rate_rank,
        
        -- Rank segments within each company by ROI (descending)
        ROW_NUMBER() OVER (
            PARTITION BY Company 
            ORDER BY avg_roi DESC
        ) as roi_rank,
        
        -- Rank segments within each company by acquisition cost (ascending - lower is better)
        ROW_NUMBER() OVER (
            PARTITION BY Company 
            ORDER BY avg_acquisition_cost ASC
        ) as acquisition_cost_rank,
        
        -- Rank segments within each company by CTR (descending)
        ROW_NUMBER() OVER (
            PARTITION BY Company 
            ORDER BY overall_ctr DESC
        ) as ctr_rank,
        
        -- Count total segments per company for context
        COUNT(*) OVER (PARTITION BY Company) as total_segments_per_company
    FROM company_segment_metrics
),

-- Get company categories (assuming companies in similar industries have similar naming patterns)
-- In a real scenario, you would have an actual industry/category field
company_categories AS (
    SELECT
        Company,
        CASE
            WHEN Company LIKE '%Tech%' OR Company LIKE '%Software%' OR Company LIKE '%Digital%' THEN 'Technology'
            WHEN Company LIKE '%Food%' OR Company LIKE '%Restaurant%' OR Company LIKE '%Eat%' THEN 'Food & Beverage'
            WHEN Company LIKE '%Retail%' OR Company LIKE '%Shop%' OR Company LIKE '%Store%' THEN 'Retail'
            WHEN Company LIKE '%Finance%' OR Company LIKE '%Bank%' OR Company LIKE '%Invest%' THEN 'Financial Services'
            WHEN Company LIKE '%Health%' OR Company LIKE '%Medical%' OR Company LIKE '%Care%' THEN 'Healthcare'
            ELSE 'Other'
        END as company_category
    FROM {{ ref('stg_campaigns') }}
    GROUP BY Company
),

-- Get segment performance by company category
segment_performance_by_category AS (
    SELECT
        cc.company_category,
        csm.Customer_Segment,
        AVG(csm.avg_conversion_rate) as category_segment_conversion_rate,
        AVG(csm.avg_roi) as category_segment_roi,
        AVG(csm.avg_acquisition_cost) as category_segment_acquisition_cost,
        AVG(csm.overall_ctr) as category_segment_ctr,
        COUNT(DISTINCT csm.Company) as company_count
    FROM company_segment_metrics csm
    JOIN company_categories cc ON csm.Company = cc.Company
    GROUP BY cc.company_category, csm.Customer_Segment
),

-- Rank segments within each category
segment_rankings_by_category AS (
    SELECT
        company_category,
        Customer_Segment,
        category_segment_conversion_rate,
        category_segment_roi,
        category_segment_acquisition_cost,
        category_segment_ctr,
        company_count,
        
        -- Rank segments within each category by conversion rate
        ROW_NUMBER() OVER (
            PARTITION BY company_category 
            ORDER BY category_segment_conversion_rate DESC
        ) as category_conversion_rate_rank,
        
        -- Rank segments within each category by ROI
        ROW_NUMBER() OVER (
            PARTITION BY company_category 
            ORDER BY category_segment_roi DESC
        ) as category_roi_rank,
        
        -- Count total segments per category for context
        COUNT(*) OVER (PARTITION BY company_category) as total_segments_per_category
    FROM segment_performance_by_category
)

-- Final output combining both company-specific and category-level insights
SELECT
    -- Company and segment information
    sr.Company,
    cc.company_category,
    sr.Customer_Segment,
    
    -- Company-specific segment metrics
    sr.avg_conversion_rate,
    sr.avg_roi,
    sr.avg_acquisition_cost,
    sr.overall_ctr,
    sr.campaign_count,
    
    -- Company-specific segment rankings
    sr.conversion_rate_rank,
    sr.roi_rank,
    sr.acquisition_cost_rank,
    sr.ctr_rank,
    sr.total_segments_per_company,
    
    -- Is this the best segment for this company by different metrics?
    CASE WHEN sr.conversion_rate_rank = 1 THEN TRUE ELSE FALSE END as is_best_conversion_segment,
    CASE WHEN sr.roi_rank = 1 THEN TRUE ELSE FALSE END as is_best_roi_segment,
    CASE WHEN sr.acquisition_cost_rank = 1 THEN TRUE ELSE FALSE END as is_best_acquisition_cost_segment,
    CASE WHEN sr.ctr_rank = 1 THEN TRUE ELSE FALSE END as is_best_ctr_segment,
    
    -- Category-level segment performance
    src.category_segment_conversion_rate,
    src.category_segment_roi,
    src.category_segment_acquisition_cost,
    src.category_segment_ctr,
    
    -- Category-level segment rankings
    src.category_conversion_rate_rank,
    src.category_roi_rank,
    
    -- Performance relative to category average
    sr.avg_conversion_rate / NULLIF(src.category_segment_conversion_rate, 0) - 1 as conversion_rate_vs_category_avg,
    sr.avg_roi / NULLIF(src.category_segment_roi, 0) - 1 as roi_vs_category_avg,
    src.category_segment_acquisition_cost / NULLIF(sr.avg_acquisition_cost, 0) - 1 as acquisition_cost_vs_category_avg,
    sr.overall_ctr / NULLIF(src.category_segment_ctr, 0) - 1 as ctr_vs_category_avg
FROM segment_rankings_by_company sr
JOIN company_categories cc ON sr.Company = cc.Company
JOIN segment_rankings_by_category src ON 
    cc.company_category = src.company_category AND 
    sr.Customer_Segment = src.Customer_Segment
ORDER BY 
    cc.company_category,
    sr.Company,
    sr.conversion_rate_rank
