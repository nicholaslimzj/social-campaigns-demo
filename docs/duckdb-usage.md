# DuckDB Usage Guide

This document provides guidance on using DuckDB in different environments for the Meta Demo project.

## Overview

Our data pipeline uses DuckDB for analytics on processed parquet files. The database is designed to work in both container and local environments through the use of SQL macros.

## Environment Setup

### Container Environment

In the Docker container, DuckDB is configured to use:
- Data root path: `/data`
- Processed data: `/data/processed`
- Database file: `/data/db/meta_analytics.duckdb`

## Using DuckDB Client on Windows

When accessing the database file created in the container from your local Windows environment, follow these steps:

1. **Open the database in your DuckDB client**:
   ```
   .open c:\Users\wolve\code\meta-demo\data\db\meta_analytics.duckdb
   ```

2. **Create or replace the DATA_ROOT macro**:
   ```sql
   CREATE OR REPLACE MACRO DATA_ROOT() AS 'c:/Users/wolve/code/meta-demo/data';
   ```
   Note: Use forward slashes in the path, even on Windows, as DuckDB prefers them.

3. **Verify it works**:
   ```sql
   SELECT DATA_ROOT();
   ```
   This should output your local data path.

4. **Query your views**:
   ```sql
   SELECT * FROM campaigns LIMIT 5;
   ```

## Example SQL Queries

Here are some example queries you can run:

```sql
-- Get top performing campaigns by ROI
SELECT * FROM campaigns ORDER BY ROI DESC LIMIT 10;

-- View company metrics
SELECT * FROM company_metrics ORDER BY avg_roi DESC;

-- View channel performance
SELECT * FROM channel_metrics ORDER BY overall_ctr DESC;

-- View audience metrics
SELECT * FROM audience_metrics ORDER BY campaign_count DESC;

-- View monthly performance
SELECT * FROM time_metrics ORDER BY month;
```

## Troubleshooting

If you encounter path-related errors like:
```
SQL Error: java.sql.SQLException: IO Error: No files found that match the pattern "/data/processed/*/*/data.parquet"
```

This indicates that the DATA_ROOT macro needs to be set for your local environment as described above.

## Implementation Details

The database uses a macro-based approach for path portability:

1. The SQL views reference paths using the `DATA_ROOT()` macro:
   ```sql
   FROM read_parquet(DATA_ROOT() || '/processed/*/*/data.parquet')
   ```

2. This macro is defined when the database is created in the container:
   ```sql
   CREATE OR REPLACE MACRO DATA_ROOT() AS '/data'
   ```

3. When using the database locally, we override this macro to point to the local path.

This approach allows the same database file to be used across different environments without regenerating it.
