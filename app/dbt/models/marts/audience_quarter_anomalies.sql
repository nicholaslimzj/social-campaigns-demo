{{ config(materialized='table') }}

/*
Model: audience_quarter_anomalies
Time Context: Current Quarter
Time Range: Last 3 months of available data

Description:
This model identifies anomalies in audience performance metrics by company and target audience,
analyzing deviations from expected values based on recent trends and statistical thresholds.
It focuses on quarter-to-quarter changes and flags significant deviations that may require attention.

Key Features:
1. Hierarchical structure with Company as the primary dimension
2. Target_Audience as the secondary dimension within each Company
3. Calculates rolling statistics (mean, standard deviation) for key metrics using recent quarters
4. Identifies outliers based on z-score methodology comparing current quarter to previous quarters
5. Flags significant quarterly deviations that may require attention
6. Provides context for each anomaly to aid investigation

Dashboard Usage:
- Company-Audience Quarterly Anomaly Detection alerts
- Company-specific Audience Performance Monitoring section
- Audience Risk Management visualizations by company
*/

WITH 
-- First, determine the date range for the current quarter
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month,
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) / 3 + MAX(EXTRACT(YEAR FROM CAST(Date AS DATE))) * 4 AS current_quarter_id
    FROM {{ ref('stg_campaigns') }}
),

-- Get current quarter data
current_quarter_data AS (
    SELECT *
    FROM {{ ref('stg_campaigns') }}
    WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
),

-- First, we need to get company-audience metrics by quarter
company_audience_quarter_metrics AS (
    SELECT 
        Company,
        Target_Audience,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) / 3 + EXTRACT(YEAR FROM CAST(Date AS DATE)) * 4 as quarter_id,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as quarterly_ctr,
        SUM(Clicks * Acquisition_Cost) as total_spend,
        SUM(Clicks * Acquisition_Cost * ROI) as total_revenue,
        COUNT(DISTINCT Campaign_ID) as campaign_count
    FROM {{ ref('stg_campaigns') }}
    GROUP BY 
        Company,
        Target_Audience, 
        EXTRACT(MONTH FROM CAST(Date AS DATE)) / 3 + EXTRACT(YEAR FROM CAST(Date AS DATE)) * 4
    ORDER BY Company, Target_Audience, quarter_id
),

-- Calculate rolling statistics for each metric
rolling_stats AS (
    SELECT
        Company,
        Target_Audience,
        quarter_id,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        total_clicks,
        total_impressions,
        quarterly_ctr,
        total_spend,
        total_revenue,
        campaign_count,
        
        -- Rolling averages (3-quarter window excluding current quarter)
        AVG(avg_conversion_rate) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS conversion_rate_mean,
        
        AVG(avg_roi) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS roi_mean,
        
        AVG(avg_acquisition_cost) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS acquisition_cost_mean,
        
        AVG(total_clicks) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS clicks_mean,
        
        AVG(total_impressions) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS impressions_mean,
        
        AVG(quarterly_ctr) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS ctr_mean,
        
        AVG(campaign_count) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS campaign_count_mean,
        
        AVG(total_spend) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS spend_mean,
        
        AVG(total_revenue) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS revenue_mean,
        
        -- Rolling standard deviations
        STDDEV_POP(avg_conversion_rate) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS conversion_rate_std,
        
        STDDEV_POP(avg_roi) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS roi_std,
        
        STDDEV_POP(avg_acquisition_cost) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS acquisition_cost_std,
        
        STDDEV_POP(total_clicks) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS clicks_std,
        
        STDDEV_POP(total_impressions) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS impressions_std,
        
        STDDEV_POP(quarterly_ctr) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS ctr_std,
        
        STDDEV_POP(campaign_count) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS campaign_count_std,
        
        STDDEV_POP(total_spend) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS spend_std,
        
        STDDEV_POP(total_revenue) OVER (
            PARTITION BY Company, Target_Audience
            ORDER BY quarter_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS revenue_std
    FROM company_audience_quarter_metrics
)

-- Calculate z-scores and flag anomalies
SELECT
    Company,
    Target_Audience,
    
    -- Conversion Rate metrics
    avg_conversion_rate,
    conversion_rate_mean,
    conversion_rate_std,
    CASE
        WHEN conversion_rate_std IS NULL OR conversion_rate_std = 0 THEN NULL
        ELSE (avg_conversion_rate - conversion_rate_mean) / conversion_rate_std
    END AS conversion_rate_z,
    CASE
        WHEN conversion_rate_std IS NULL OR conversion_rate_std = 0 THEN 'normal'
        WHEN ABS((avg_conversion_rate - conversion_rate_mean) / conversion_rate_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS conversion_rate_anomaly,
    
    -- ROI metrics
    avg_roi,
    roi_mean,
    roi_std,
    CASE
        WHEN roi_std IS NULL OR roi_std = 0 THEN NULL
        ELSE (avg_roi - roi_mean) / roi_std
    END AS roi_z,
    CASE
        WHEN roi_std IS NULL OR roi_std = 0 THEN 'normal'
        WHEN ABS((avg_roi - roi_mean) / roi_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS roi_anomaly,
    
    -- Acquisition Cost metrics
    avg_acquisition_cost,
    acquisition_cost_mean,
    acquisition_cost_std,
    CASE
        WHEN acquisition_cost_std IS NULL OR acquisition_cost_std = 0 THEN NULL
        ELSE (avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std
    END AS acquisition_cost_z,
    CASE
        WHEN acquisition_cost_std IS NULL OR acquisition_cost_std = 0 THEN 'normal'
        WHEN ABS((avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS acquisition_cost_anomaly,
    
    -- Clicks metrics
    total_clicks,
    clicks_mean,
    clicks_std,
    CASE
        WHEN clicks_std IS NULL OR clicks_std = 0 THEN NULL
        ELSE (total_clicks - clicks_mean) / clicks_std
    END AS clicks_z,
    CASE
        WHEN clicks_std IS NULL OR clicks_std = 0 THEN 'normal'
        WHEN ABS((total_clicks - clicks_mean) / clicks_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS clicks_anomaly,
    
    -- Impressions metrics
    total_impressions,
    impressions_mean,
    impressions_std,
    CASE
        WHEN impressions_std IS NULL OR impressions_std = 0 THEN NULL
        ELSE (total_impressions - impressions_mean) / impressions_std
    END AS impressions_z,
    CASE
        WHEN impressions_std IS NULL OR impressions_std = 0 THEN 'normal'
        WHEN ABS((total_impressions - impressions_mean) / impressions_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS impressions_anomaly,
    
    -- CTR metrics
    quarterly_ctr,
    ctr_mean,
    ctr_std,
    CASE
        WHEN ctr_std IS NULL OR ctr_std = 0 THEN NULL
        ELSE (quarterly_ctr - ctr_mean) / ctr_std
    END AS ctr_z,
    CASE
        WHEN ctr_std IS NULL OR ctr_std = 0 THEN 'normal'
        WHEN ABS((quarterly_ctr - ctr_mean) / ctr_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS ctr_anomaly,
    
    -- Campaign Count metrics
    campaign_count,
    campaign_count_mean,
    campaign_count_std,
    CASE
        WHEN campaign_count_std IS NULL OR campaign_count_std = 0 THEN NULL
        ELSE (campaign_count - campaign_count_mean) / campaign_count_std
    END AS campaign_count_z,
    CASE
        WHEN campaign_count_std IS NULL OR campaign_count_std = 0 THEN 'normal'
        WHEN ABS((campaign_count - campaign_count_mean) / campaign_count_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS campaign_count_anomaly,
    
    -- Spend metrics
    total_spend,
    spend_mean,
    spend_std,
    CASE
        WHEN spend_std IS NULL OR spend_std = 0 THEN NULL
        ELSE (total_spend - spend_mean) / spend_std
    END AS spend_z,
    CASE
        WHEN spend_std IS NULL OR spend_std = 0 THEN 'normal'
        WHEN ABS((total_spend - spend_mean) / spend_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS spend_anomaly,
    
    -- Revenue metrics
    total_revenue,
    revenue_mean,
    revenue_std,
    CASE
        WHEN revenue_std IS NULL OR revenue_std = 0 THEN NULL
        ELSE (total_revenue - revenue_mean) / revenue_std
    END AS revenue_z,
    CASE
        WHEN revenue_std IS NULL OR revenue_std = 0 THEN 'normal'
        WHEN ABS((total_revenue - revenue_mean) / revenue_std) > 2 THEN 'anomaly'
        ELSE 'normal'
    END AS revenue_anomaly,
    
    -- Create a summary field for easy filtering
    CASE
        WHEN (
            (conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND ABS((avg_conversion_rate - conversion_rate_mean) / conversion_rate_std) > 2) OR
            (roi_std IS NOT NULL AND roi_std > 0 AND ABS((avg_roi - roi_mean) / roi_std) > 2) OR
            (acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND ABS((avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std) > 2) OR
            (clicks_std IS NOT NULL AND clicks_std > 0 AND ABS((total_clicks - clicks_mean) / clicks_std) > 2) OR
            (impressions_std IS NOT NULL AND impressions_std > 0 AND ABS((total_impressions - impressions_mean) / impressions_std) > 2) OR
            (ctr_std IS NOT NULL AND ctr_std > 0 AND ABS((quarterly_ctr - ctr_mean) / ctr_std) > 2) OR
            (campaign_count_std IS NOT NULL AND campaign_count_std > 0 AND ABS((campaign_count - campaign_count_mean) / campaign_count_std) > 2) OR
            (spend_std IS NOT NULL AND spend_std > 0 AND ABS((total_spend - spend_mean) / spend_std) > 2) OR
            (revenue_std IS NOT NULL AND revenue_std > 0 AND ABS((total_revenue - revenue_mean) / revenue_std) > 2)
        ) THEN TRUE
        ELSE FALSE
    END AS has_anomaly,
    
    -- Anomaly severity (count of metrics with anomalies)
    (
        CASE WHEN conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND ABS((avg_conversion_rate - conversion_rate_mean) / conversion_rate_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN roi_std IS NOT NULL AND roi_std > 0 AND ABS((avg_roi - roi_mean) / roi_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND ABS((avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN clicks_std IS NOT NULL AND clicks_std > 0 AND ABS((total_clicks - clicks_mean) / clicks_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN impressions_std IS NOT NULL AND impressions_std > 0 AND ABS((total_impressions - impressions_mean) / impressions_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN ctr_std IS NOT NULL AND ctr_std > 0 AND ABS((quarterly_ctr - ctr_mean) / ctr_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN campaign_count_std IS NOT NULL AND campaign_count_std > 0 AND ABS((campaign_count - campaign_count_mean) / campaign_count_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN spend_std IS NOT NULL AND spend_std > 0 AND ABS((total_spend - spend_mean) / spend_std) > 2 THEN 1 ELSE 0 END +
        CASE WHEN revenue_std IS NOT NULL AND revenue_std > 0 AND ABS((total_revenue - revenue_mean) / revenue_std) > 2 THEN 1 ELSE 0 END
    ) AS anomaly_count,
    
    -- Anomaly impact (positive or negative)
    CASE
        WHEN (
            -- Positive anomalies (higher is better)
            (conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND (avg_conversion_rate - conversion_rate_mean) / conversion_rate_std > 2) OR
            (roi_std IS NOT NULL AND roi_std > 0 AND (avg_roi - roi_mean) / roi_std > 2) OR
            (ctr_std IS NOT NULL AND ctr_std > 0 AND (quarterly_ctr - ctr_mean) / ctr_std > 2) OR
            (revenue_std IS NOT NULL AND revenue_std > 0 AND (total_revenue - revenue_mean) / revenue_std > 2) OR
            -- Negative anomalies (lower is better)
            (acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND (acquisition_cost_mean - avg_acquisition_cost) / acquisition_cost_std > 2)
        ) THEN 'positive'
        WHEN (
            -- Negative anomalies (higher is worse)
            (acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND (avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std > 2) OR
            -- Positive anomalies (lower is worse)
            (conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND (conversion_rate_mean - avg_conversion_rate) / conversion_rate_std > 2) OR
            (roi_std IS NOT NULL AND roi_std > 0 AND (roi_mean - avg_roi) / roi_std > 2) OR
            (ctr_std IS NOT NULL AND ctr_std > 0 AND (ctr_mean - quarterly_ctr) / ctr_std > 2) OR
            (revenue_std IS NOT NULL AND revenue_std > 0 AND (revenue_mean - total_revenue) / revenue_std > 2)
        ) THEN 'negative'
        ELSE 'normal'
    END AS anomaly_impact,
    
    -- Anomaly description for dashboard display
    CASE
        WHEN conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND (avg_conversion_rate - conversion_rate_mean) / conversion_rate_std > 2 
            THEN 'Unusually high conversion rate'
        WHEN conversion_rate_std IS NOT NULL AND conversion_rate_std > 0 AND (avg_conversion_rate - conversion_rate_mean) / conversion_rate_std < -2 
            THEN 'Unusually low conversion rate'
        WHEN roi_std IS NOT NULL AND roi_std > 0 AND (avg_roi - roi_mean) / roi_std > 2 
            THEN 'Unusually high ROI'
        WHEN roi_std IS NOT NULL AND roi_std > 0 AND (avg_roi - roi_mean) / roi_std < -2 
            THEN 'Unusually low ROI'
        WHEN acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND (avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std > 2 
            THEN 'Unusually high acquisition cost'
        WHEN acquisition_cost_std IS NOT NULL AND acquisition_cost_std > 0 AND (avg_acquisition_cost - acquisition_cost_mean) / acquisition_cost_std < -2 
            THEN 'Unusually low acquisition cost'
        WHEN ctr_std IS NOT NULL AND ctr_std > 0 AND (quarterly_ctr - ctr_mean) / ctr_std > 2 
            THEN 'Unusually high CTR'
        WHEN ctr_std IS NOT NULL AND ctr_std > 0 AND (quarterly_ctr - ctr_mean) / ctr_std < -2 
            THEN 'Unusually low CTR'
        WHEN campaign_count_std IS NOT NULL AND campaign_count_std > 0 AND (campaign_count - campaign_count_mean) / campaign_count_std > 2 
            THEN 'Unusually high campaign count'
        WHEN campaign_count_std IS NOT NULL AND campaign_count_std > 0 AND (campaign_count - campaign_count_mean) / campaign_count_std < -2 
            THEN 'Unusually low campaign count'
        WHEN spend_std IS NOT NULL AND spend_std > 0 AND (total_spend - spend_mean) / spend_std > 2 
            THEN 'Unusually high spend'
        WHEN spend_std IS NOT NULL AND spend_std > 0 AND (total_spend - spend_mean) / spend_std < -2 
            THEN 'Unusually low spend'
        WHEN revenue_std IS NOT NULL AND revenue_std > 0 AND (total_revenue - revenue_mean) / revenue_std > 2 
            THEN 'Unusually high revenue'
        WHEN revenue_std IS NOT NULL AND revenue_std > 0 AND (total_revenue - revenue_mean) / revenue_std < -2 
            THEN 'Unusually low revenue'
        ELSE NULL
    END AS anomaly_description
FROM rolling_stats
WHERE quarter_id = (SELECT current_quarter_id FROM date_ranges)
ORDER BY Company, Target_Audience