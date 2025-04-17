{{ config(materialized='table') }}

/*
Model: channel_historical_metrics
Time Context: Historical (all available data)

Description:
This model aggregates campaign performance metrics by channel across all historical data
to provide insights into channel effectiveness and ROI comparison.

Key Features:
1. Calculates key performance metrics for each marketing channel
2. Enables channel-to-channel comparison
3. Provides a foundation for channel allocation strategy
4. Supports media mix optimization

Dashboard Usage:
- Channel Performance section
- Media Mix recommendations
- Channel ROI comparison visualizations
*/

SELECT 
    Channel_Used,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Channel_Used
