{{ config(materialized='table') }}

/*
Model: campaign_month_performance_rankings
Time Context: Monthly (last 30 days)

Description:
This model ranks campaigns for each company based on the last 30 days of data,
identifying top and bottom performers across key metrics like ROI, conversion rate,
revenue, and CPA (Cost Per Acquisition).

Key Features:
1. Ranks campaigns by ROI, conversion rate, revenue, and CPA
2. Identifies top and bottom performers for each metric
3. Calculates performance metrics and comparison to company averages
4. Assigns performance tiers based on ROI
5. Provides recommended actions for campaign optimization

Dashboard Usage:
- Campaign Performance Rankings visualization
- Top and Bottom Performers analysis
- Campaign Optimization recommendations
*/

WITH 
-- First, determine the date range for the current month (last 30 days)
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_month,
        MAX(CAST(Date AS DATE)) AS latest_date,
        MAX(CAST(Date AS DATE)) - INTERVAL '30 days' AS thirty_days_ago
    FROM {{ ref('stg_campaigns') }}
),

-- Get campaign performance data for the last 30 days
recent_campaigns AS (
    SELECT 
        Company,
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel_Used as channel,
        Customer_Segment as segment,
        AVG(ROI) as roi,
        AVG(Conversion_Rate) as conversion_rate,
        SUM(Acquisition_Cost) as spend,
        SUM(Acquisition_Cost * ROI) as revenue,
        AVG(Acquisition_Cost) as acquisition_cost,
        -- Calculate CPA (Cost Per Acquisition)
        CASE 
            WHEN SUM(Clicks * Conversion_Rate) > 0 THEN SUM(Acquisition_Cost) / SUM(Clicks * Conversion_Rate)
            ELSE AVG(Acquisition_Cost) -- Fallback to acquisition cost if no conversions
        END as cpa,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr,
        SUM(Clicks) as clicks,
        SUM(Impressions) as impressions,
        MIN(Date) as start_date,
        MAX(Date) as end_date,
        DATEDIFF('day', MIN(Date), MAX(Date)) + 1 as duration_days
    FROM {{ ref('stg_campaigns') }}
    WHERE CAST(Date AS DATE) >= (SELECT thirty_days_ago FROM date_ranges)
    GROUP BY 
        Company,
        Campaign_ID,
        Campaign_Goal,
        Channel_Used,
        Customer_Segment
    HAVING SUM(Impressions) > 1000  -- Ensure we have enough data
),

-- Calculate company averages for comparison
company_averages AS (
    SELECT
        Company,
        AVG(roi) as avg_company_roi,
        AVG(conversion_rate) as avg_company_conversion_rate,
        AVG(acquisition_cost) as avg_company_acquisition_cost,
        AVG(ctr) as avg_company_ctr,
        AVG(cpa) as avg_company_cpa,
        SUM(spend) as total_company_spend,
        SUM(revenue) as total_company_revenue
    FROM recent_campaigns
    GROUP BY Company
),

-- Rank campaigns within each company by different metrics
ranked_campaigns AS (
    SELECT
        rc.*,
        -- Compare to company averages
        rc.roi / NULLIF(ca.avg_company_roi, 0) as roi_vs_company_avg,
        rc.conversion_rate / NULLIF(ca.avg_company_conversion_rate, 0) as conversion_vs_company_avg,
        ca.avg_company_acquisition_cost / NULLIF(rc.acquisition_cost, 0) as acquisition_efficiency,
        rc.revenue / NULLIF(ca.total_company_revenue, 0) as revenue_share,
        rc.spend / NULLIF(ca.total_company_spend, 0) as spend_share,
        
        -- Rank campaigns by different metrics
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.roi DESC) as roi_rank,
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.conversion_rate DESC) as conversion_rank,
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.revenue DESC) as revenue_rank,
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.cpa ASC) as cpa_rank, -- Lower CPA is better
        
        -- Also get reverse ranks for bottom performers
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.roi ASC) as roi_rank_asc,
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.conversion_rate ASC) as conversion_rank_asc,
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.revenue ASC) as revenue_rank_asc,
        ROW_NUMBER() OVER (PARTITION BY rc.Company ORDER BY rc.cpa DESC) as cpa_rank_asc, -- Higher CPA is worse
        
        -- Count total campaigns per company
        COUNT(*) OVER (PARTITION BY rc.Company) as total_campaigns_per_company,
        
        -- Assign performance tier based on ROI
        CASE 
            WHEN rc.roi > 2.5 THEN 'excellent'
            WHEN rc.roi > 1.5 THEN 'good'
            WHEN rc.roi > 1.0 THEN 'average'
            ELSE 'below_average'
        END as performance_tier,
        
        -- Generate optimization recommendation based on metrics
        CASE
            WHEN rc.roi > 2.0 AND rc.spend < (ca.total_company_spend * 0.1) THEN 'Increase budget allocation'
            WHEN rc.roi < 1.0 AND rc.spend > (ca.total_company_spend * 0.1) THEN 'Reduce budget allocation'
            WHEN rc.conversion_rate > (ca.avg_company_conversion_rate * 1.2) THEN 'Optimize for conversions'
            WHEN rc.acquisition_cost > (ca.avg_company_acquisition_cost * 1.2) THEN 'Improve cost efficiency'
            ELSE 'Maintain current strategy'
        END as recommended_action
    FROM recent_campaigns rc
    JOIN company_averages ca ON rc.Company = ca.Company
)

-- Final output with top and bottom performers flagged
SELECT
    Company,
    campaign_id,
    goal,
    channel,
    segment,
    roi,
    conversion_rate,
    spend,
    revenue,
    acquisition_cost,
    cpa,
    ctr,
    clicks,
    impressions,
    start_date,
    end_date,
    duration_days,
    roi_vs_company_avg,
    conversion_vs_company_avg,
    acquisition_efficiency,
    revenue_share,
    spend_share,
    roi_rank,
    conversion_rank,
    revenue_rank,
    cpa_rank,
    roi_rank_asc,
    conversion_rank_asc,
    revenue_rank_asc,
    cpa_rank_asc,
    performance_tier,
    recommended_action,
    
    -- Flag top performers (top 5 or top 10%)
    CASE WHEN roi_rank <= 5 OR roi_rank <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_top_roi_performer,
    CASE WHEN conversion_rank <= 5 OR conversion_rank <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_top_conversion_performer,
    CASE WHEN revenue_rank <= 5 OR revenue_rank <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_top_revenue_performer,
    CASE WHEN cpa_rank <= 5 OR cpa_rank <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_top_cpa_performer,
    
    -- Flag bottom performers (bottom 5 or bottom 10%)
    CASE WHEN roi_rank_asc <= 5 OR roi_rank_asc <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_bottom_roi_performer,
    CASE WHEN conversion_rank_asc <= 5 OR conversion_rank_asc <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_bottom_conversion_performer,
    CASE WHEN revenue_rank_asc <= 5 OR revenue_rank_asc <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_bottom_revenue_performer,
    CASE WHEN cpa_rank_asc <= 5 OR cpa_rank_asc <= CEIL(total_campaigns_per_company * 0.1) THEN TRUE ELSE FALSE END as is_bottom_cpa_performer
FROM ranked_campaigns
ORDER BY Company, roi_rank
