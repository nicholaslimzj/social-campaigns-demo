{{ config(materialized='table') }}

SELECT 
    Company,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Company