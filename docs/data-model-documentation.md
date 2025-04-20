# Meta-Demo Analytics Platform: Data Model Documentation

## Overview

This document provides comprehensive documentation for the data models in the Meta-Demo Analytics Platform, detailing their purpose, structure, time context, and relationships.

## Time Context Strategy

The platform implements a dual time context strategy:

1. **Quarterly Time Context (Primary)**
   - Used for higher-level dimensions (Company, Target_Audience)
   - Applied to models focused on strategic analysis
   - Implemented with standardized date_ranges CTE pattern
   - Filtering mechanism: Last 3 months of available data
   - Quarter identification formula: `EXTRACT(MONTH FROM CAST(Date AS DATE)) / 3 + EXTRACT(YEAR FROM CAST(Date AS DATE)) * 4`

2. **Monthly Time Context (Secondary)**
   - Maintained for campaign-level metrics
   - Used for more granular, tactical analysis
   - Provides finer time grain for tracking campaign progress
   - Appropriate for shorter-duration marketing activities

## Dimension Hierarchy

Two distinct types of dimension models are implemented:

1. **Company-Hierarchical Models**
   - Primary dimension: Company
   - Secondary dimension: Target_Audience
   - Tertiary dimensions: Location, Channel, Campaign Goal
   - Examples: audience_quarter_performance_matrix, audience_quarter_anomalies
   - Purpose: Company-specific performance analysis and anomaly detection

2. **Industry-Standard Models**
   - No company hierarchy
   - Dimensions treated as independent entities
   - Examples: dimensions_quarter_performance_rankings
   - Purpose: Industry benchmarking and cross-company comparisons
   - Enables visualization of industry standards for audiences, segments, channels

## Data Models

### Performance Matrix Models

#### `audience_quarter_performance_matrix.sql`
- **Purpose**: Cross-tabulation of audience performance metrics with hierarchical dimension structure
- **Primary Dimension**: Company
- **Secondary Dimension**: Target_Audience
- **Time Context**: Quarterly
- **Key Features**:
  - Comprehensive performance metrics (conversion rate, ROI, CTR, etc.)
  - Quarter-over-quarter performance comparisons
  - Performance rankings within company
  - Composite performance scores
  - Z-score normalization for cross-company comparison

#### `channel_quarter_performance_matrix.sql`
- **Purpose**: Channel-focused performance analysis with hierarchical dimension structure
- **Primary Dimension**: Company
- **Secondary Dimension**: Channel
- **Time Context**: Quarterly
- **Key Features**:
  - Channel-specific performance metrics
  - Quarter-over-quarter performance comparisons
  - Performance rankings within company
  - Composite performance scores
  - Z-score normalization for cross-company comparison

#### `campaign_quarter_performance_matrix.sql`
- **Purpose**: Campaign-level performance analysis with hierarchical dimension structure
- **Primary Dimension**: Company
- **Secondary Dimension**: Campaign
- **Time Context**: Quarterly
- **Key Features**:
  - Campaign-specific performance metrics
  - Quarter-over-quarter performance comparisons
  - Performance rankings within company
  - Campaign duration analysis
  - Composite performance scores
  - Z-score normalization for cross-company comparison

### Monthly Metrics Models

#### `channel_monthly_metrics.sql`
- **Purpose**: Granular, month-to-month tracking of channel performance
- **Primary Dimension**: Channel
- **Secondary Dimension**: Company
- **Time Context**: Monthly
- **Key Features**:
  - Month-over-month performance comparisons
  - Detailed trend analysis
  - Support for tactical decision-making

#### `audience_monthly_metrics.sql`
- **Purpose**: Granular, month-to-month tracking of audience performance
- **Primary Dimension**: Target_Audience
- **Secondary Dimension**: Company
- **Time Context**: Monthly
- **Key Features**:
  - Month-over-month performance comparisons
  - Detailed trend analysis
  - Support for tactical decision-making

### Anomaly Detection Models

#### `audience_quarter_anomalies.sql`
- **Purpose**: Identify statistical anomalies in audience performance
- **Primary Dimension**: Company
- **Secondary Dimension**: Target_Audience
- **Time Context**: Quarterly
- **Key Features**:
  - Z-score methodology for anomaly detection
  - Quarterly rolling window analysis
  - Detection of significant performance deviations
  - Multi-metric anomaly identification

#### `channel_quarter_anomalies.sql`
- **Purpose**: Identify statistical anomalies in channel performance
- **Primary Dimension**: Company
- **Secondary Dimension**: Channel
- **Time Context**: Quarterly
- **Key Features**:
  - Z-score methodology for anomaly detection
  - Quarterly rolling window analysis
  - Detection of significant performance deviations
  - Multi-metric anomaly identification

#### `metrics_quarter_anomalies.sql`
- **Purpose**: Identify statistical anomalies in company-level metrics
- **Primary Dimension**: Company
- **Secondary Dimension**: Metric Type
- **Time Context**: Quarterly
- **Key Features**:
  - Z-score methodology for anomaly detection
  - Quarterly rolling window analysis
  - Detection of significant performance deviations
  - Multi-metric anomaly identification

### Optimization Models

#### `channel_quarter_budget_optimizer.sql`
- **Purpose**: Recommend optimal budget allocation across channels
- **Primary Dimension**: Company
- **Secondary Dimension**: Channel
- **Time Context**: Quarterly
- **Key Features**:
  - Uses historical spend data instead of predefined budgets
  - Recommends spend allocation based on ROI and efficiency
  - Handles diminishing returns and capacity constraints
  - Provides spend recommendations with expected outcomes

#### `goal_quarter_metrics.sql`
- **Purpose**: Aggregate performance metrics by campaign goal
- **Primary Dimension**: Company
- **Secondary Dimension**: Campaign Goal
- **Time Context**: Quarterly
- **Key Features**:
  - Goal-specific performance metrics
  - Quarter-over-quarter changes
  - Performance rankings by goal
  - Support for strategic goal-based planning

#### `campaign_quarter_clusters.sql`
- **Purpose**: Identify optimal combinations of campaign parameters
- **Primary Dimension**: Company
- **Secondary Dimensions**: Goal, Segment, Channel, Duration
- **Time Context**: Quarterly
- **Key Features**:
  - Identifies winning combinations based on composite performance
  - Determines optimal duration ranges for each combination
  - Provides statistical significance through minimum sample sizes
  - Supports campaign optimization recommendations

### Forecasting Models

#### `campaign_future_forecast.sql`
- **Purpose**: Project future performance metrics with confidence intervals
- **Primary Dimension**: Company
- **Secondary Dimension**: Campaign Type
- **Time Context**: Monthly (forward-looking)
- **Key Features**:
  - Time series analysis techniques
  - Confidence interval calculations
  - Seasonality adjustments
  - Trend identification

## Common SQL Patterns

### Date Range Determination
```sql
date_ranges AS (
    SELECT
        MAX(EXTRACT(MONTH FROM CAST(Date AS DATE))) AS current_max_month
    FROM {{ ref('stg_campaigns') }}
)
```

### Quarterly Filtering
```sql
WHERE EXTRACT(MONTH FROM CAST(Date AS DATE)) >= (SELECT current_max_month - 2 FROM date_ranges)
```

### Z-Score Calculation
```sql
CASE 
    WHEN stddev_value > 0 
    THEN (metric_value - avg_value) / stddev_value
    ELSE 0
END AS metric_z_score
```

### Composite Score Calculation
```sql
(metric1_z_score * weight1 + 
 metric2_z_score * weight2 + 
 metric3_z_score * weight3 + 
 metric4_z_score * weight4) AS composite_score
```

## Data Flow

1. Source data from `stg_campaigns`
2. Transformation into quarterly and monthly context models
3. Derivation of performance matrices and anomaly detection
4. Generation of optimization recommendations
5. Visualization through dashboard components

## Dashboard Integration

See `dashboard-tables-mapping.yaml` for a comprehensive mapping of dashboard visualizations to their corresponding data models.
