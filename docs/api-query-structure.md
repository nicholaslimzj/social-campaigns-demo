# Meta-Demo Analytics API Query Structure

This document outlines the recommended API structure and query patterns for efficiently populating the Meta-Demo Analytics dashboard frontend. The API design follows the hierarchical dimension structure established in our data models, with Company as the primary dimension.

## API Design Principles

1. **Hierarchical Query Structure**:
   - Primary dimension (Company) first
   - Secondary dimensions (Target_Audience, Channel, Campaign_Goal) second
   - Time context (Quarter, Month) last

2. **Efficient Data Loading**:
   - Use parameterized endpoints to minimize data transfer
   - Implement pagination for large result sets
   - Support filtering at the API level

3. **Consistent Response Format**:
   - Standard JSON structure across all endpoints
   - Consistent naming conventions
   - Include metadata about the query parameters

## Core API Endpoints

### 1. Company Overview Endpoints

#### `GET /api/companies`
- Returns list of all companies for dropdown selection
- No parameters required
- Used for initial dashboard loading

#### `GET /api/companies/{company_id}/metrics`
- Returns high-level company metrics
- Parameters:
  - `time_period`: "current_quarter" (default), "previous_quarter", "year_to_date"
- Used for Company Overview dashboard section

#### `GET /api/companies/{company_id}/performance_tiers`
- Returns performance tier distribution for the company
- Parameters:
  - `time_period`: "current_quarter" (default), "previous_quarter", "year_to_date"
- Used for Company Performance Tier visualization

### 2. Target Audience Endpoints

#### `GET /api/companies/{company_id}/audiences`
- Returns list of target audiences for the specified company
- Parameters:
  - `include_metrics`: boolean (include basic metrics with each audience)
- Used for audience selection dropdowns

#### `GET /api/companies/{company_id}/audiences/{audience_id}/metrics`
- Returns detailed metrics for a specific audience within a company
- Parameters:
  - `time_period`: "current_quarter" (default), "previous_quarter", "year_to_date"
  - `include_dimensions`: boolean (include breakdowns by other dimensions)
- Used for Target Audience Analysis tab

#### `GET /api/companies/{company_id}/audiences/performance_matrix`
- Returns the audience performance matrix data
- Parameters:
  - `dimension_type`: "language" (default), "location", "goal"
  - `time_period`: "current_quarter" (default), "previous_quarter"
- Used for Audience Performance Matrix visualization

#### `GET /api/companies/{company_id}/audiences/clusters`
- Returns audience clustering data
- Parameters:
  - `time_period`: "current_quarter" (default), "previous_quarter"
- Used for Audience Clustering visualization

### 3. Channel Endpoints

#### `GET /api/companies/{company_id}/channels`
- Returns list of channels for the specified company
- Parameters:
  - `include_metrics`: boolean (include basic metrics with each channel)
- Used for channel selection dropdowns

#### `GET /api/companies/{company_id}/channels/{channel_id}/metrics`
- Returns detailed metrics for a specific channel within a company
- Parameters:
  - `time_period`: "current_quarter" (default), "previous_quarter", "year_to_date"
  - `include_dimensions`: boolean (include breakdowns by other dimensions)
- Used for Channel Analysis tab

#### `GET /api/companies/{company_id}/channels/performance_matrix`
- Returns the channel performance matrix data
- Parameters:
  - `dimension_type`: "target_audience" (default), "location", "goal"
  - `time_period`: "current_quarter" (default), "previous_quarter"
- Used for Channel Performance Matrix visualization

### 4. Campaign Goal Endpoints

#### `GET /api/companies/{company_id}/goals`
- Returns list of campaign goals for the specified company
- Parameters:
  - `include_metrics`: boolean (include basic metrics with each goal)
- Used for goal selection dropdowns

#### `GET /api/companies/{company_id}/goals/{goal_id}/metrics`
- Returns detailed metrics for a specific campaign goal within a company
- Parameters:
  - `time_period`: "current_quarter" (default), "previous_quarter", "year_to_date"
- Used for Campaign Goal Analysis tab

### 5. Time Series Endpoints

#### `GET /api/companies/{company_id}/monthly_metrics`
- Returns monthly metrics for the specified company
- Parameters:
  - `dimension`: "target_audience", "channel", "goal"
  - `months`: integer (number of months to return, default 12)
- Used for Monthly Trend visualizations

#### `GET /api/companies/{company_id}/quarterly_metrics`
- Returns quarterly metrics for the specified company
- Parameters:
  - `dimension`: "target_audience", "channel", "goal"
  - `quarters`: integer (number of quarters to return, default 4)
- Used for Quarterly Trend visualizations

### 6. Anomaly Detection Endpoints

#### `GET /api/companies/{company_id}/anomalies`
- Returns detected anomalies for the specified company
- Parameters:
  - `dimension`: "target_audience", "channel", "goal"
  - `time_period`: "current_quarter" (default), "previous_quarter", "year_to_date"
  - `threshold`: float (z-score threshold for anomaly detection, default 2.0)
- Used for Anomaly Detection visualizations

### 7. Forecast Endpoints

#### `GET /api/companies/{company_id}/forecasts`
- Returns forecast data for the specified company
- Parameters:
  - `dimension`: "target_audience", "channel", "goal"
  - `periods`: integer (number of future periods to forecast, default 3)
  - `metric`: "conversion_rate", "roi", "acquisition_cost", "ctr"
- Used for Forecast visualizations

## Query Optimization Strategies

### 1. Caching Strategy
- Cache frequently accessed company-level metrics
- Implement time-based cache invalidation (refresh every 24 hours)
- Use ETags for client-side caching

### 2. Query Parameterization
- Allow frontend to request only needed fields
- Support partial responses with field selection
- Implement query complexity limits

### 3. Batch Requests
- Support batch API requests for dashboard initialization
- Allow multiple dimension queries in a single request
- Provide bulk export endpoints for data analysts

### 4. Pagination
- Implement cursor-based pagination for large datasets
- Allow custom page sizes (with reasonable limits)
- Include pagination metadata in responses

## Dashboard-to-API Mapping

| Dashboard Component | API Endpoint | Query Parameters |
|---------------------|--------------|-----------------|
| Company Overview | `/api/companies/{company_id}/metrics` | `time_period=current_quarter` |
| Performance Tiers | `/api/companies/{company_id}/performance_tiers` | `time_period=current_quarter` |
| Target Audience Analysis | `/api/companies/{company_id}/audiences/performance_matrix` | `dimension_type=language` |
| Channel Analysis | `/api/companies/{company_id}/channels/performance_matrix` | `dimension_type=target_audience` |
| Monthly Trends | `/api/companies/{company_id}/monthly_metrics` | `dimension=target_audience&months=12` |
| Quarterly Trends | `/api/companies/{company_id}/quarterly_metrics` | `dimension=channel&quarters=4` |
| Anomaly Detection | `/api/companies/{company_id}/anomalies` | `dimension=target_audience&threshold=2.0` |
| Forecasts | `/api/companies/{company_id}/forecasts` | `dimension=channel&periods=3&metric=roi` |

## Implementation Considerations

1. **Authentication and Authorization**:
   - Implement OAuth 2.0 for API authentication
   - Role-based access control for company data
   - API key management for external integrations

2. **Rate Limiting**:
   - Implement per-user and per-endpoint rate limits
   - Provide rate limit headers in responses
   - Graceful degradation for high-traffic scenarios

3. **Error Handling**:
   - Consistent error response format
   - Detailed error messages for debugging
   - Appropriate HTTP status codes

4. **Monitoring and Analytics**:
   - Track API usage patterns
   - Monitor endpoint performance
   - Identify optimization opportunities

5. **Versioning Strategy**:
   - Use URL versioning (e.g., `/api/v1/companies`)
   - Maintain backward compatibility
   - Deprecation notices for outdated endpoints
