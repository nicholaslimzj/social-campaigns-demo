{{ config(materialized='table') }}

/*
Model: campaign_future_forecast
Time Context: Future (next 3 months projection)

Description:
This model creates performance forecasts for campaigns using time series analysis techniques
to project future performance metrics based on historical patterns.

Key Features:
1. Extracts historical monthly metrics by company
2. Calculates trend components using moving averages and growth rates
3. Identifies seasonal patterns (if any)
4. Projects metrics forward for the next 3 months
5. Provides confidence intervals based on historical volatility

Dashboard Usage:
- Campaign Performance Forecasting section
- Predictive Analytics visualizations

Note: The approach uses SQL-based forecasting techniques compatible with DuckDB
without requiring external ML libraries.
*/

WITH 
-- Get historical monthly data
monthly_metrics AS (
    SELECT
        Company,
        EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
        EXTRACT(YEAR FROM CAST(Date AS DATE)) as year,
        -- Create a numeric month_id for easier time series analysis (year*100 + month)
        (EXTRACT(YEAR FROM CAST(Date AS DATE)) * 100) + EXTRACT(MONTH FROM CAST(Date AS DATE)) as month_id,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
        COUNT(*) as campaign_count
    FROM {{ ref('stg_campaigns') }}
    GROUP BY 
        Company, 
        EXTRACT(MONTH FROM CAST(Date AS DATE)),
        EXTRACT(YEAR FROM CAST(Date AS DATE))
    ORDER BY Company, year, month
),

-- Calculate the max month_id to use as reference point for forecasting
max_date AS (
    SELECT 
        MAX(month_id) as max_month_id,
        -- Extract month and year components for seasonal adjustments
        CAST(MAX(month_id) % 100 AS INTEGER) as max_month,
        CAST(MAX(month_id) / 100 AS INTEGER) as max_year
    FROM monthly_metrics
),

-- Calculate moving averages and growth rates for trend analysis
trend_analysis AS (
    SELECT
        mm.*,
        
        -- 3-month moving average (smoothing)
        AVG(avg_conversion_rate) OVER (
            PARTITION BY Company
            ORDER BY month_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS conversion_rate_ma3,
        
        AVG(avg_roi) OVER (
            PARTITION BY Company
            ORDER BY month_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS roi_ma3,
        
        AVG(avg_acquisition_cost) OVER (
            PARTITION BY Company
            ORDER BY month_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS acquisition_cost_ma3,
        
        AVG(avg_ctr) OVER (
            PARTITION BY Company
            ORDER BY month_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS ctr_ma3,
        
        -- 6-month moving average (longer-term trend)
        AVG(avg_conversion_rate) OVER (
            PARTITION BY Company
            ORDER BY month_id
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS conversion_rate_ma6,
        
        AVG(avg_roi) OVER (
            PARTITION BY Company
            ORDER BY month_id
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS roi_ma6,
        
        -- Calculate month-over-month growth rates
        avg_conversion_rate / NULLIF(LAG(avg_conversion_rate, 1) OVER (
            PARTITION BY Company ORDER BY month_id
        ), 0) - 1 AS conversion_rate_mom_growth,
        
        avg_roi / NULLIF(LAG(avg_roi, 1) OVER (
            PARTITION BY Company ORDER BY month_id
        ), 0) - 1 AS roi_mom_growth,
        
        LAG(avg_acquisition_cost, 1) OVER (
            PARTITION BY Company ORDER BY month_id
        ) / NULLIF(avg_acquisition_cost, 0) - 1 AS acquisition_cost_mom_growth,
        
        avg_ctr / NULLIF(LAG(avg_ctr, 1) OVER (
            PARTITION BY Company ORDER BY month_id
        ), 0) - 1 AS ctr_mom_growth
    FROM monthly_metrics mm
),

-- Calculate average growth rates over recent periods (last 3 months)
recent_growth_rates AS (
    SELECT
        Company,
        -- Use median growth rate to reduce impact of outliers
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY conversion_rate_mom_growth) AS median_conversion_growth,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY roi_mom_growth) AS median_roi_growth,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY acquisition_cost_mom_growth) AS median_acquisition_cost_growth,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ctr_mom_growth) AS median_ctr_growth,
        
        -- Also calculate average of last 3 months for each metric as starting point
        AVG(avg_conversion_rate) AS recent_avg_conversion_rate,
        AVG(avg_roi) AS recent_avg_roi,
        AVG(avg_acquisition_cost) AS recent_avg_acquisition_cost,
        AVG(avg_ctr) AS recent_avg_ctr,
        
        -- Calculate volatility (standard deviation) for confidence intervals
        STDDEV_POP(avg_conversion_rate) AS conversion_rate_volatility,
        STDDEV_POP(avg_roi) AS roi_volatility,
        STDDEV_POP(avg_acquisition_cost) AS acquisition_cost_volatility,
        STDDEV_POP(avg_ctr) AS ctr_volatility
    FROM trend_analysis
    WHERE month_id >= (SELECT max_month_id - 3 FROM max_date)
    GROUP BY Company
),

-- Check for seasonality by comparing same month in previous years
seasonal_factors AS (
    SELECT
        ta.Company,
        ta.month,
        -- Calculate seasonal index (current value / moving average)
        AVG(ta.avg_conversion_rate / NULLIF(ta.conversion_rate_ma6, 0)) AS conversion_rate_seasonal_factor,
        AVG(ta.avg_roi / NULLIF(ta.roi_ma6, 0)) AS roi_seasonal_factor,
        AVG(ta.acquisition_cost_ma3 / NULLIF(ta.avg_acquisition_cost, 0)) AS acquisition_cost_seasonal_factor,
        AVG(ta.avg_ctr / NULLIF(ta.ctr_ma3, 0)) AS ctr_seasonal_factor
    FROM trend_analysis ta
    WHERE ta.conversion_rate_ma6 IS NOT NULL
    GROUP BY ta.Company, ta.month
),

-- Generate forecast months (next 3 months)
forecast_months AS (
    SELECT 
        md.max_month_id,
        md.max_month,
        md.max_year,
        -- Generate the 3 months after max_month_id
        s.period AS forecast_period,
        -- Calculate month and year for each forecast period
        -- For example, if max_month is 12 (December) and max_year is 2022:
        -- Period 1: Jan 2023 (month=1, year=2023)
        -- Period 2: Feb 2023 (month=2, year=2023)
        -- Period 3: Mar 2023 (month=3, year=2023)
        
        -- Calculate month number (1-12) for each forecast period
        1 + MOD(md.max_month + s.period - 1, 12) AS forecast_month,
        
        -- Calculate year for each forecast period (cast to integer to avoid decimal issues)
        CAST(md.max_year + FLOOR((md.max_month + s.period - 1) / 12) AS INTEGER) AS forecast_year,
        
        -- Calculate actual month_id for each forecast period (cast to integer to avoid decimal issues)
        CAST(
            (md.max_year + FLOOR((md.max_month + s.period - 1) / 12)) * 100 + 
            (1 + MOD(md.max_month + s.period - 1, 12))
        AS INTEGER) AS forecast_month_id
    FROM max_date md
    CROSS JOIN (VALUES (1), (2), (3)) AS s(period)
),

-- Generate the actual forecast
forecast_data AS (
    SELECT
        rgr.Company,
        fm.forecast_month_id AS month_id,
        fm.forecast_month AS month,
        fm.forecast_year AS year,
        fm.forecast_period,
        
        -- Conversion Rate Forecast
        -- Base forecast using recent average and growth rate
        rgr.recent_avg_conversion_rate * POWER(1 + rgr.median_conversion_growth, fm.forecast_period) AS conversion_rate_forecast_base,
        -- Apply seasonal adjustment if available
        COALESCE(
            rgr.recent_avg_conversion_rate * POWER(1 + rgr.median_conversion_growth, fm.forecast_period) * 
            sf.conversion_rate_seasonal_factor,
            rgr.recent_avg_conversion_rate * POWER(1 + rgr.median_conversion_growth, fm.forecast_period)
        ) AS conversion_rate_forecast,
        
        -- ROI Forecast
        COALESCE(
            rgr.recent_avg_roi * POWER(1 + rgr.median_roi_growth, fm.forecast_period) * 
            sf.roi_seasonal_factor,
            rgr.recent_avg_roi * POWER(1 + rgr.median_roi_growth, fm.forecast_period)
        ) AS roi_forecast,
        
        -- Acquisition Cost Forecast (note: lower is better, so growth is applied differently)
        COALESCE(
            rgr.recent_avg_acquisition_cost * POWER(1 - rgr.median_acquisition_cost_growth, fm.forecast_period) * 
            sf.acquisition_cost_seasonal_factor,
            rgr.recent_avg_acquisition_cost * POWER(1 - rgr.median_acquisition_cost_growth, fm.forecast_period)
        ) AS acquisition_cost_forecast,
        
        -- CTR Forecast
        COALESCE(
            rgr.recent_avg_ctr * POWER(1 + rgr.median_ctr_growth, fm.forecast_period) * 
            sf.ctr_seasonal_factor,
            rgr.recent_avg_ctr * POWER(1 + rgr.median_ctr_growth, fm.forecast_period)
        ) AS ctr_forecast,
        
        -- Calculate confidence intervals (using volatility)
        -- Lower bound (95% confidence interval uses ~2 standard deviations)
        GREATEST(0, rgr.recent_avg_conversion_rate * POWER(1 + rgr.median_conversion_growth, fm.forecast_period) - 
                (2 * rgr.conversion_rate_volatility * SQRT(fm.forecast_period))) AS conversion_rate_lower_bound,
        GREATEST(0, rgr.recent_avg_roi * POWER(1 + rgr.median_roi_growth, fm.forecast_period) - 
                (2 * rgr.roi_volatility * SQRT(fm.forecast_period))) AS roi_lower_bound,
        GREATEST(0, rgr.recent_avg_acquisition_cost * POWER(1 - rgr.median_acquisition_cost_growth, fm.forecast_period) - 
                (2 * rgr.acquisition_cost_volatility * SQRT(fm.forecast_period))) AS acquisition_cost_lower_bound,
        GREATEST(0, rgr.recent_avg_ctr * POWER(1 + rgr.median_ctr_growth, fm.forecast_period) - 
                (2 * rgr.ctr_volatility * SQRT(fm.forecast_period))) AS ctr_lower_bound,
                
        -- Upper bound
        rgr.recent_avg_conversion_rate * POWER(1 + rgr.median_conversion_growth, fm.forecast_period) + 
                (2 * rgr.conversion_rate_volatility * SQRT(fm.forecast_period)) AS conversion_rate_upper_bound,
        rgr.recent_avg_roi * POWER(1 + rgr.median_roi_growth, fm.forecast_period) + 
                (2 * rgr.roi_volatility * SQRT(fm.forecast_period)) AS roi_upper_bound,
        rgr.recent_avg_acquisition_cost * POWER(1 - rgr.median_acquisition_cost_growth, fm.forecast_period) + 
                (2 * rgr.acquisition_cost_volatility * SQRT(fm.forecast_period)) AS acquisition_cost_upper_bound,
        rgr.recent_avg_ctr * POWER(1 + rgr.median_ctr_growth, fm.forecast_period) + 
                (2 * rgr.ctr_volatility * SQRT(fm.forecast_period)) AS ctr_upper_bound,
                
        -- Flag as forecast data
        TRUE AS is_forecast
    FROM recent_growth_rates rgr
    CROSS JOIN forecast_months fm
    LEFT JOIN seasonal_factors sf ON 
        rgr.Company = sf.Company AND 
        fm.forecast_month = sf.month
),

-- Combine historical and forecast data
combined_data AS (
    -- Historical data
    SELECT
        Company,
        month_id,
        month,
        year,
        avg_conversion_rate AS conversion_rate,
        NULL AS conversion_rate_forecast,
        NULL AS conversion_rate_lower_bound,
        NULL AS conversion_rate_upper_bound,
        avg_roi AS roi,
        NULL AS roi_forecast,
        NULL AS roi_lower_bound,
        NULL AS roi_upper_bound,
        avg_acquisition_cost AS acquisition_cost,
        NULL AS acquisition_cost_forecast,
        NULL AS acquisition_cost_lower_bound,
        NULL AS acquisition_cost_upper_bound,
        avg_ctr AS ctr,
        NULL AS ctr_forecast,
        NULL AS ctr_lower_bound,
        NULL AS ctr_upper_bound,
        FALSE AS is_forecast
    FROM monthly_metrics
    
    UNION ALL
    
    -- Forecast data
    SELECT
        Company,
        month_id,
        month,
        year,
        NULL AS conversion_rate,
        conversion_rate_forecast,
        conversion_rate_lower_bound,
        conversion_rate_upper_bound,
        NULL AS roi,
        roi_forecast,
        roi_lower_bound,
        roi_upper_bound,
        NULL AS acquisition_cost,
        acquisition_cost_forecast,
        acquisition_cost_lower_bound,
        acquisition_cost_upper_bound,
        NULL AS ctr,
        ctr_forecast,
        ctr_lower_bound,
        ctr_upper_bound,
        is_forecast
    FROM forecast_data
)

-- Final output with additional metadata
SELECT
    cd.*,
    -- Add month name for better readability
    CASE 
        WHEN cd.month = 1 THEN 'January'
        WHEN cd.month = 2 THEN 'February'
        WHEN cd.month = 3 THEN 'March'
        WHEN cd.month = 4 THEN 'April'
        WHEN cd.month = 5 THEN 'May'
        WHEN cd.month = 6 THEN 'June'
        WHEN cd.month = 7 THEN 'July'
        WHEN cd.month = 8 THEN 'August'
        WHEN cd.month = 9 THEN 'September'
        WHEN cd.month = 10 THEN 'October'
        WHEN cd.month = 11 THEN 'November'
        WHEN cd.month = 12 THEN 'December'
    END AS month_name,
    
    -- Calculate forecast accuracy metrics (only for forecast periods)
    CASE 
        WHEN cd.is_forecast THEN
            COALESCE(
                ABS(cd.conversion_rate_upper_bound - cd.conversion_rate_lower_bound) / 
                NULLIF(cd.conversion_rate_forecast * 2, 0),
                0
            )
        ELSE NULL
    END AS conversion_rate_uncertainty,
    
    CASE 
        WHEN cd.is_forecast THEN
            COALESCE(
                ABS(cd.roi_upper_bound - cd.roi_lower_bound) / 
                NULLIF(cd.roi_forecast * 2, 0),
                0
            )
        ELSE NULL
    END AS roi_uncertainty
FROM combined_data cd
ORDER BY 
    cd.Company,
    cd.month_id
