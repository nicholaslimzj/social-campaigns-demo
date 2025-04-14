# Meta Demo DBT Project

This directory contains the dbt (data build tool) implementation for the Meta Demo project.

## Project Structure

- `/models/staging/` - Base views with minimal transformations
  - `stg_campaigns.sql` - Base view of campaign data with pre-hook to create DATA_ROOT() macro
  
- `/models/marts/` - Business-level aggregation tables
  - `company_metrics.sql` - Metrics aggregated by company
  - `channel_metrics.sql` - Metrics aggregated by channel
  - `audience_metrics.sql` - Metrics aggregated by target audience
  - `time_metrics.sql` - Metrics aggregated by month

- `/macros/` - Reusable SQL fragments
  - Contains macros for column definitions and aggregations

## Materialization Strategy

- Staging models: materialized as views
- Mart models: materialized as tables

## Path Handling

The project uses a DuckDB macro called `DATA_ROOT()` for path portability:

- In container: `DATA_ROOT()` returns `/data`
- In Windows: Override with `CREATE OR REPLACE MACRO DATA_ROOT() AS 'c:/Users/wolve/code/meta-demo/data'`

This macro is created via a pre-hook in the `stg_campaigns.sql` model.

## Running the Models

You can run the models using the following commands:

```bash
# Run all models
dbt run

# Run a specific model
dbt run --select model_name

# Generate documentation
dbt docs generate
```

## Cross-Environment Support

This dbt project is designed to work seamlessly in both Docker container and local Windows environments:

1. **In Docker container**:
   - The `DATA_ROOT()` macro is set to `/data`
   - Models are executed with `python -m app.main dbt`

2. **In DBeaver or other DuckDB clients**:
   - Override the `DATA_ROOT()` macro with your local path:
   ```sql
   CREATE OR REPLACE MACRO DATA_ROOT() AS 'c:/Users/wolve/code/meta-demo/data';
   ```
   - Then query the models directly:
   ```sql
   SELECT * FROM company_metrics ORDER BY avg_roi DESC LIMIT 5;
   ```
