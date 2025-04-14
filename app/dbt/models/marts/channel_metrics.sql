{{ config(materialized='table') }}

SELECT 
    Channel_Used,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY Channel_Used
