# Campaign Performance Dashboard Architecture

## Overview

This document outlines the architecture for a scalable campaign performance dashboard that provides company-specific insights while using global data as benchmarks. The dashboard is designed to handle approximately 500 campaigns per month across multiple companies, with a focus on trend and anomaly detection.

## Data Structure

The dashboard is built on top of the following dbt models:

1. **Core Metrics**
   - `company_metrics.sql`: Aggregated metrics by company
   - `channel_metrics.sql`: Aggregated metrics by channel
   - `time_metrics.sql`: Time-series metrics by month

2. **Advanced Insights**
   - `anomaly_flags.sql`: Statistical anomalies in key metrics
   - `top_bottom_performers.sql`: Best and worst performing entities
   - `trend_change.sql`: Detection of trend direction changes
   - `segment_company_performance.sql`: Segment performance by company and industry
   - `campaign_clusters.sql`: Identifies high-performing campaign combinations
   - `campaign_duration_analysis.sql`: Analyzes optimal campaign duration
   - `campaign_performance_matrix.sql`: Cross-tabulation of goals vs segments
   - `campaign_forecasting.sql`: Predictive analytics for campaign performance

## Dashboard Architecture

### 1. Company Selector

A dropdown at the top of the dashboard allows users to select a specific company. All visualizations and insights will update to show data for the selected company, with global benchmarks for comparison.

### 2. Insights Panel

A natural language insights panel at the top of the dashboard that highlights:
- Recent anomalies detected for the selected company
- Trend changes in key metrics
- Top performing segments and channels
- Comparative performance against industry benchmarks

### 3. Key Performance Indicators

A set of KPI cards showing:
- Conversion Rate (with % change vs previous period)
- ROI (with % change vs previous period)
- Acquisition Cost (with % change vs previous period)
- CTR (with % change vs previous period)
- Campaign Count

### 4. Time Series Visualizations

Interactive charts showing:
- Monthly performance trends for key metrics
- Anomaly points highlighted on the charts
- Trend change points marked on the charts
- Ability to compare against global/industry benchmarks

### 5. Segment Performance

A heatmap or table showing:
- Performance of different customer segments for the selected company
- Comparison to industry averages for each segment
- Highlighting of best-performing segments

### 6. Channel Performance

A bar chart or table showing:
- Performance of different channels for the selected company
- Comparison to global averages for each channel
- Highlighting of best-performing channels

### 7. Campaign Details

A filterable table showing:
- Individual campaign performance for the selected company
- Ability to filter by location, goal, duration, language, segment
- Highlighting of anomalous campaigns

## Technical Implementation

### Frontend (Next.js)

1. **Component Structure**
   - `CompanySelector.tsx`: Dropdown for company selection
   - `InsightsPanel.tsx`: Natural language insights component
   - `KPICards.tsx`: Key performance indicator cards
   - `TimeSeriesChart.tsx`: Reusable time series chart component
   - `SegmentHeatmap.tsx`: Segment performance visualization
   - `ChannelBarChart.tsx`: Channel performance visualization
   - `CampaignTable.tsx`: Filterable campaign details table
   - `CampaignClusters.tsx`: High-performing campaign combinations
   - `DurationAnalysis.tsx`: Campaign duration optimization visualization
   - `PerformanceMatrix.tsx`: Goal vs segment matrix heatmap
   - `ForecastChart.tsx`: Predictive analytics visualization
   - `TopBottomPerformers.tsx`: Top/bottom performers comparison
   - `AnomalyTimeline.tsx`: Timeline with anomaly markers

2. **State Management**
   - Use React Context to manage the selected company state
   - Use SWR or React Query for data fetching and caching

3. **API Integration**
   - Create API endpoints for each visualization component
   - Implement filtering by company ID in all API calls

### Backend (Flask)

1. **API Endpoints**
   - `/api/companies`: List of all companies
   - `/api/insights/{company_id}`: Natural language insights for a company
   - `/api/metrics/{company_id}`: KPI metrics for a company
   - `/api/time-series/{company_id}`: Time series data for a company
   - `/api/segments/{company_id}`: Segment performance for a company
   - `/api/channels/{company_id}`: Channel performance for a company
   - `/api/campaigns/{company_id}`: Campaign details for a company
   - `/api/campaign-clusters/{company_id}`: High-performing campaign combinations
   - `/api/campaign-duration/{company_id}`: Optimal duration analysis by segment/channel
   - `/api/performance-matrix/{company_id}`: Goal vs segment performance matrix
   - `/api/forecasting/{company_id}`: Campaign performance forecasts
   - `/api/top-bottom/{company_id}`: Top and bottom performing campaigns
   - `/api/anomalies/{company_id}`: Anomaly detection with context

2. **Insight Generation**
   - Create a service that generates natural language insights from the dbt models
   - Prioritize anomalies, trend changes, and comparative performance

## Scalability Considerations

1. **Data Volume**
   - Implement pagination for campaign details
   - Use aggregated data for visualizations
   - Consider caching frequently accessed data

2. **Performance**
   - Optimize database queries with proper indexing
   - Use materialized views for complex aggregations
   - Implement client-side caching for dashboard components

3. **Extensibility**
   - Design components to be reusable and configurable
   - Use a modular architecture that allows adding new visualizations
   - Implement a plugin system for new types of insights

## Example Insights

The insights panel could generate text such as:

- "Conversion rates for Tech segment campaigns have increased by 15% this month, significantly outperforming the industry average."
- "ANOMALY DETECTED: CTR for Facebook campaigns dropped by 30% in the last week, well below the expected range."
- "TREND CHANGE: After 3 months of decline, your ROI is now showing an upward trend since last month."
- "Your best performing segment is 'Young Professionals', with 2.3x higher conversion rates than your company average."
