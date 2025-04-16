{{ config(
    materialized='view'
) }}

SELECT 
    {{ base_columns() }},
    -- Add derived metrics
    {{ derived_metrics() }}
FROM read_parquet(DATA_ROOT() || '/processed/*/*/data.parquet')
