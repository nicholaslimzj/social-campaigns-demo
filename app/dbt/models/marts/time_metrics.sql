{{ config(materialized='table') }}

SELECT 
    EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
    {{ agg_metrics() }}
FROM {{ ref('stg_campaigns') }}
GROUP BY month
ORDER BY month
