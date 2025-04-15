{{ config(materialized='table') }}

SELECT 
    Customer_Segment,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Customer_Segment
