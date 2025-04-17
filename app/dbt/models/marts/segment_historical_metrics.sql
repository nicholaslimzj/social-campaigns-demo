{{ config(materialized='table') }}

/*
Model: segment_historical_metrics
Time Context: Historical (all available data)

Description:
This model aggregates campaign performance metrics by customer segment across all historical data
to provide insights into segment-level performance and targeting effectiveness.

Key Features:
1. Calculates key performance metrics for each customer segment
2. Enables segment-to-segment comparison
3. Provides a foundation for segment targeting strategy
4. Supports customer segmentation analysis

Dashboard Usage:
- Customer Segment Performance section
- Audience Targeting recommendations
- Segment Comparison visualizations
*/

SELECT 
    Customer_Segment,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Customer_Segment
