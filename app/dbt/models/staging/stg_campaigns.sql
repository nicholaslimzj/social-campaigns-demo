{{ config(
    materialized='view',
    pre_hook="CREATE OR REPLACE MACRO DATA_ROOT() AS '/data';"
) }}

SELECT 
    {{ base_columns() }},
    -- Add derived metrics
    {{ derived_metrics() }}
FROM read_parquet(DATA_ROOT() || '/processed/*/*/data.parquet')
