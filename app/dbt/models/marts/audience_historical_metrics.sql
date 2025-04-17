{{ config(materialized='table') }}

/*
Model: audience_historical_metrics
Time Context: Historical (all available data)

Description:
This model aggregates campaign performance metrics by target audience across all historical data
to provide insights into audience-level performance and targeting effectiveness.

Key Features:
1. Calculates key performance metrics for each target audience
2. Enables audience-to-audience comparison
3. Provides a foundation for audience targeting strategy
4. Supports demographic targeting analysis

Dashboard Usage:
- Audience Performance section
- Demographic Targeting recommendations
- Audience Comparison visualizations
*/

SELECT 
    Target_Audience,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Target_Audience
