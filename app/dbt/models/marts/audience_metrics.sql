{{ config(materialized='table') }}

SELECT 
    Target_Audience,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Target_Audience
