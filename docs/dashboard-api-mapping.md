# Dashboard to API Endpoint Mapping

This document maps each visualization component in the Meta-Demo dashboard to the specific API endpoints it should call. The mapping follows our hierarchical API structure with Company as the primary dimension.

## Overview Tab

### KPI Cards
**Visualization**: Top KPI Cards (Conversion Rate, ROI, Acquisition Cost, CTR, Campaign Count)
**API Endpoint**: `GET /api/companies/{company_id}/monthly_metrics`
**Query Parameters**:
- `include_anomalies`: false
**Response Format**:
```json
{
  "metrics": {
    "conversion_rate": [
      {"month": "2024-01", "value": 0.0321, "is_anomaly": false},
      {"month": "2024-02", "value": 0.0334, "is_anomaly": false},
      {"month": "2024-03", "value": 0.0412, "is_anomaly": true},
      ...
    ],
    "roi": [
      {"month": "2024-01", "value": 2.4},
      {"month": "2024-02", "value": 2.6},
      {"month": "2024-03", "value": 2.7},
      ...
    ],
    "acquisition_cost": [
      {"month": "2024-01", "value": 43.21},
      {"month": "2024-02", "value": 42.85},
      {"month": "2024-03", "value": 42.18},
      ...
    ],
    "ctr": [
      {"month": "2024-01", "value": 0.0205},
      {"month": "2024-02", "value": 0.0210},
      {"month": "2024-03", "value": 0.0215},
      ...
    ]
  }
}
```

### Conversion Rate Trends
**Visualization**: Conversion Rate Trends (Line chart with anomaly)
**API Endpoint**: `GET /api/companies/{company_id}/monthly_metrics`
**Query Parameters**:
- `include_anomalies`: true
**Response Format**:
```json
{
  "metrics": {
    "conversion_rate": [
      {"month": "2024-01", "value": 0.0321, "is_anomaly": false},
      {"month": "2024-02", "value": 0.0334, "is_anomaly": false},
      {"month": "2024-03", "value": 0.0412, "is_anomaly": true},
      ...
    ],
    "roi": [
      {"month": "2024-01", "value": 2.4},
      {"month": "2024-02", "value": 2.6},
      {"month": "2024-03", "value": 2.7},
      ...
    ],
    "acquisition_cost": [
      {"month": "2024-01", "value": 43.21},
      {"month": "2024-02", "value": 42.85},
      {"month": "2024-03", "value": 42.18},
      ...
    ],
    "ctr": [
      {"month": "2024-01", "value": 0.0205},
      {"month": "2024-02", "value": 0.0210},
      {"month": "2024-03", "value": 0.0215},
      ...
    ]
  },
  "anomalies": [
    {
      "metric": "conversion_rate",
      "month": "2024-03", 
      "value": 0.0412, 
      "z_score": 2.3, 
      "explanation": "Unusual spike in conversion rate coinciding with product launch"
    }
  ]
}
```

### Top Performing Audiences
**Visualization**: Top Performing Target Audience (Table)
**API Endpoint**: `GET /api/companies/{company_id}/audiences`
**Query Parameters**:
- `include_metrics`: true
**Response Format**:
```json
{
  "audiences": [
    {
      "audience_id": "Young Professionals",
      "campaign_count": 12,
      "avg_roi": 3.2,
      "avg_conversion_rate": 0.042,
      "avg_acquisition_cost": 38.25,
      "avg_ctr": 0.028
    },
    ...
  ]
}
```

### Top Performing Channels
**Visualization**: Top Performing Channels (Table)
**API Endpoint**: `GET /api/companies/{company_id}/channels`
**Query Parameters**:
- `include_metrics`: true
**Response Format**:
```json
{
  "channels": [
    {
      "channel_id": "Social Media",
      "campaign_count": 15,
      "avg_roi": 3.5,
      "avg_conversion_rate": 0.038,
      "avg_acquisition_cost": 35.12,
      "avg_ctr": 0.032
    },
    ...
  ]
}
```

## Campaign Analysis Tab

### Campaign Duration Impact
**Visualization**: Campaign Duration Impact (Scatter plot)
**API Endpoint**: `GET /api/companies/{company_id}/campaign_duration_analysis`
**Query Parameters**:
- No parameters required
**Response Format**:
```json
{
  "data": [
    {
      "campaign_id": "C12345",
      "duration_days": 45,
      "roi": 2.8,
      "conversion_rate": 0.035,
      "dimension_value": "Young Professionals"
    },
    ...
  ]
}
```

### Campaign Goals Analysis
**Visualization**: Campaign Goals Analysis (Bar chart)
**API Endpoint**: `GET /api/companies/{company_id}/goals`
**Query Parameters**:
- `include_metrics`: true
**Response Format**:
```json
{
  "goals": [
    {
      "goal_id": "Brand Awareness",
      "campaign_count": 8,
      "avg_roi": 2.3,
      "avg_conversion_rate": 0.028,
      "avg_acquisition_cost": 42.15,
      "avg_ctr": 0.025
    },
    ...
  ]
}
```

### Campaign Performance Timeline
**Visualization**: Campaign Performance Timeline (Interactive timeline)
**API Endpoint**: `GET /api/companies/{company_id}/monthly_campaign_metrics`
**Response Format**:
```json
{
  "metrics": {
    "roi": [
      {"month": "2024-01", "value": 2.4},
      {"month": "2024-02", "value": 2.6},
      {"month": "2024-03", "value": 2.7},
      ...
    ],
    "conversion_rate": [
      {"month": "2024-01", "value": 0.031},
      {"month": "2024-02", "value": 0.033},
      {"month": "2024-03", "value": 0.034},
      ...
    ],
    "acquisition_cost": [
      {"month": "2024-01", "value": 43.21},
      {"month": "2024-02", "value": 42.85},
      {"month": "2024-03", "value": 42.18},
      ...
    ],
    "ctr": [
      {"month": "2024-01", "value": 0.022},
      {"month": "2024-02", "value": 0.023},
      {"month": "2024-03", "value": 0.024},
      ...
    ]
  }
}
```

### Winning Campaign Combinations
**Visualization**: Winning Campaign Combinations (Table)
**API Endpoint**: `GET /api/companies/{company_id}/campaign_clusters`
**Query Parameters**:
- `min_performance_score`: 8.0
**Response Format**:
```json
{
  "clusters": [
    {
      "cluster_id": 1,
      "goal": "Lead Generation",
      "target_audience": "Young Professionals",
      "channel": "Social Media",
      "avg_duration_days": 42,
      "avg_roi": 3.2,
      "avg_conversion_rate": 0.041,
      "campaign_count": 5
    },
    ...
  ]
}
```

### Top & Bottom Performers
**Visualization**: Top & Bottom Performers (Bar charts)
**API Endpoint**: `GET /api/companies/{company_id}/top_bottom_performers`
**Query Parameters**:
- `limit`: 5
**Response Format**:
```json
{
  "top_performers": [
    {
      "campaign_id": "C12345",
      "name": "Summer Promo 2024",
      "roi": 4.2,
      "conversion_rate": 0.048,
      "acquisition_cost": 32.15,
      "ctr": 0.035,
      "target_audience": "Young Professionals",
      "channel": "Social Media",
      "goal": "Lead Generation"
    },
    ...
  ],
  "bottom_performers": [
    {
      "campaign_id": "C54321",
      "name": "Spring Email Campaign",
      "roi": 1.2,
      "conversion_rate": 0.018,
      "acquisition_cost": 52.35,
      "ctr": 0.012,
      "target_audience": "Senior Executives",
      "channel": "Email",
      "goal": "Brand Awareness"
    },
    ...
  ]
}
```

### Campaign Anomalies
**Visualization**: Campaign Anomalies (Timeline)
**API Endpoint**: `GET /api/companies/{company_id}/campaign_anomalies`
**Query Parameters**:
- `threshold`: 2.0
**Response Format**:
```json
{
  "anomalies": [
    {
      "campaign_id": "C12345",
      "name": "Summer Promo 2024",
      "metric": "conversion_rate",
      "expected_value": 0.032,
      "actual_value": 0.052,
      "z_score": 2.8,
      "date": "2024-03-15",
      "explanation": "Significant positive deviation possibly due to viral social media post"
    },
    ...
  ]
}
```

### Campaign Performance Forecasting
**Visualization**: Campaign Performance Forecasting
**API Endpoint**: `GET /api/companies/{company_id}/campaign_forecasts`
**Response Format**:
```json
{
  "forecasts": [
    {
      "period": "2024-Q2",
      "roi": {
        "forecast": 2.8,
        "lower_bound": 2.5,
        "upper_bound": 3.1
      },
      "conversion_rate": {
        "forecast": 0.036,
        "lower_bound": 0.032,
        "upper_bound": 0.040
      },
      "acquisition_cost": {
        "forecast": 41.50,
        "lower_bound": 39.75,
        "upper_bound": 43.25
      },
      "ctr": {
        "forecast": 0.025,
        "lower_bound": 0.022,
        "upper_bound": 0.028
      }
    },
    ...
  ]
}
```

## Channel Analysis Tab

### Channel ROI vs Acquisition Cost
**Visualization**: Channel ROI vs Acquisition Cost (Bubble chart)
**API Endpoint**: `GET /api/companies/{company_id}/channels`
**Query Parameters**:
- `include_metrics`: true
**Response Format**:
```json
{
  "channels": [
    {
      "channel_id": "Social Media",
      "campaign_count": 15,
      "avg_roi": 3.5,
      "avg_conversion_rate": 0.038,
      "avg_acquisition_cost": 35.12,
      "avg_ctr": 0.032,
      "total_spend": 125000
    },
    ...
  ]
}
```

### Channel Mix Analysis
**Visualization**: Channel Mix Analysis (Pie chart)
**API Endpoint**: `GET /api/companies/{company_id}/channels`
**Query Parameters**:
- `include_metrics`: true
**Response Format**:
```json
{
  "channels": [
    {
      "channel_id": "Social Media",
      "campaign_count": 15,
      "avg_roi": 3.5,
      "avg_conversion_rate": 0.038,
      "avg_acquisition_cost": 35.12,
      "avg_ctr": 0.032,
      "total_spend": 125000
    },
    {
      "channel_id": "Search",
      "campaign_count": 12,
      "avg_roi": 3.2,
      "avg_conversion_rate": 0.042,
      "avg_acquisition_cost": 38.50,
      "avg_ctr": 0.025,
      "total_spend": 95000
    },
    ...
  ]
}
```

*Note: The frontend can calculate percentages based on total_spend values*

### Channel Conversion Trends
**Visualization**: Channel Conversion Trends (Line chart)
**API Endpoint**: `GET /api/companies/{company_id}/monthly_channel_metrics`
**Response Format**:
```json
{
  "channels": [
    {
      "channel_id": "Social Media",
      "metrics": {
        "conversion_rate": [
          {"month": "2024-01", "value": 0.034},
          {"month": "2024-02", "value": 0.036},
          ...
        ],
        "roi": [
          {"month": "2024-01", "value": 3.3},
          {"month": "2024-02", "value": 3.4},
          ...
        ],
        "acquisition_cost": [
          {"month": "2024-01", "value": 36.25},
          {"month": "2024-02", "value": 35.80},
          ...
        ],
        "ctr": [
          {"month": "2024-01", "value": 0.030},
          {"month": "2024-02", "value": 0.031},
          ...
        ]
      }
    },
    {
      "channel_id": "Search",
      "metrics": {
        "conversion_rate": [
          {"month": "2024-01", "value": 0.042},
          {"month": "2024-02", "value": 0.044},
          ...
        ],
        "roi": [
          {"month": "2024-01", "value": 3.0},
          {"month": "2024-02", "value": 3.1},
          ...
        ],
        "acquisition_cost": [
          {"month": "2024-01", "value": 38.50},
          {"month": "2024-02", "value": 38.10},
          ...
        ],
        "ctr": [
          {"month": "2024-01", "value": 0.025},
          {"month": "2024-02", "value": 0.026},
          ...
        ]
      }
    },
    ...
  ]
}
```

### Channel Effectiveness Comparison
**Visualization**: Channel Effectiveness Comparison (Bar chart)
**API Endpoint**: `GET /api/companies/{company_id}/channels/benchmarks`
**Response Format**:
```json
{
  "channels": [
    {
      "channel_id": "Social Media",
      "company_roi": 3.5,
      "industry_roi": 2.8,
      "roi_percentile": 85,
      "company_conversion_rate": 0.038,
      "industry_conversion_rate": 0.032,
      "conversion_rate_percentile": 78,
      "company_acquisition_cost": 35.12,
      "industry_acquisition_cost": 40.25,
      "acquisition_cost_percentile": 65,
      "company_ctr": 0.032,
      "industry_ctr": 0.025,
      "ctr_percentile": 82
    },
    ...
  ]
}
```

### Channel Performance Metrics
**Visualization**: Channel Performance Metrics (Table)
**API Endpoint**: `GET /api/companies/{company_id}/channels`
**Query Parameters**:
- `include_metrics`: true
- `include_extended_metrics`: true
**Response Format**:
```json
{
  "channels": [
    {
      "channel_id": "Social Media",
      "campaign_count": 15,
      "avg_roi": 3.5,
      "avg_conversion_rate": 0.038,
      "avg_acquisition_cost": 35.12,
      "avg_ctr": 0.032,
      "total_spend": 125000,
      "total_impressions": 4500000,
      "total_clicks": 144000,
      "total_conversions": 5472,
      "total_revenue": 437500
    },
    ...
  ]
}
```

### Channel by Campaign Goal
**Visualization**: Channel by Campaign Goal (Heatmap)
**API Endpoint**: `GET /api/companies/{company_id}/channels/performance_matrix`
**Query Parameters**:
- `dimension_type`: "goal"
**Response Format**:
```json
{
  "matrix": [
    {
      "channel_id": "Social Media",
      "dimensions": [
        {
          "dimension_value": "Brand Awareness",
          "metrics": {
            "roi": 2.8,
            "conversion_rate": 0.032,
            "acquisition_cost": 38.25,
            "ctr": 0.028
          }
        },
        {
          "dimension_value": "Lead Generation",
          "metrics": {
            "roi": 3.7,
            "conversion_rate": 0.045,
            "acquisition_cost": 32.15,
            "ctr": 0.035
          }
        },
        {
          "dimension_value": "Sales Conversion",
          "metrics": {
            "roi": 3.2,
            "conversion_rate": 0.038,
            "acquisition_cost": 35.80,
            "ctr": 0.030
          }
        },
        ...
      ]
    },
    ...
  ]
}
```

### Channel by Target Audience
**Visualization**: Channel by Target Audience (Heatmap)
**API Endpoint**: `GET /api/companies/{company_id}/channels/performance_matrix`
**Query Parameters**:
- `dimension_type`: "target_audience"
**Response Format**:
```json
{
  "matrix": [
    {
      "channel_id": "Social Media",
      "dimensions": [
        {
          "dimension_value": "Young Professionals",
          "metrics": {
            "roi": 3.9,
            "conversion_rate": 0.048,
            "acquisition_cost": 30.25,
            "ctr": 0.038
          }
        },
        {
          "dimension_value": "Senior Executives",
          "metrics": {
            "roi": 2.5,
            "conversion_rate": 0.025,
            "acquisition_cost": 45.75,
            "ctr": 0.020
          }
        },
        ...
      ]
    },
    ...
  ]
}
```

### Channel Anomalies
**Visualization**: Channel Anomalies (Timeline)
**API Endpoint**: `GET /api/companies/{company_id}/channel_anomalies`
**Query Parameters**:
- `threshold`: 2.0
**Response Format**:
```json
{
  "anomalies": [
    {
      "channel_id": "Email",
      "metric": "conversion_rate",
      "expected_value": 0.024,
      "actual_value": 0.038,
      "z_score": 2.3,
      "date": "2024-03-10",
      "explanation": "Significant improvement in email conversion rates following template redesign"
    },
    ...
  ]
}
```

### Budget Allocation Optimizer
**Visualization**: Budget Allocation Optimizer (Interactive tool)
**API Endpoint**: `GET /api/companies/{company_id}/channels/budget_optimizer`
**Query Parameters**:
- `total_budget`: 500000
- `optimization_goal`: "roi"
**Response Format**:
```json
{
  "current_allocation": [
    {"channel_id": "Social Media", "budget": 125000, "percentage": 35.2, "projected_roi": 3.5},
    {"channel_id": "Search", "budget": 95000, "percentage": 26.8, "projected_roi": 3.2},
    ...
  ],
  "optimized_allocation": [
    {"channel_id": "Social Media", "budget": 150000, "percentage": 30.0, "projected_roi": 3.6},
    {"channel_id": "Search", "budget": 125000, "percentage": 25.0, "projected_roi": 3.3},
    ...
  ],
  "optimization_metrics": {
    "current_total_roi": 3.1,
    "optimized_total_roi": 3.4,
    "improvement_percentage": 9.7
  }
}
```

## Cohort Analysis Tab

### Target Audience Performance Comparison
**Visualization**: Target Audience Performance Comparison (Bar chart)
**API Endpoint**: `GET /api/companies/{company_id}/audiences`
**Query Parameters**:
- `include_metrics`: true
**Response Format**:
```json
{
  "audiences": [
    {
      "audience_id": "Young Professionals",
      "campaign_count": 12,
      "avg_roi": 3.2,
      "avg_conversion_rate": 0.042,
      "avg_acquisition_cost": 38.25,
      "avg_ctr": 0.028
    },
    ...
  ]
}
```

### Target Audience ROI Trends
**Visualization**: Target Audience ROI Trends (Line chart)
**API Endpoint**: `GET /api/companies/{company_id}/audiences/monthly_metrics`
**Response Format**:
```json
{
  "audiences": [
    {
      "audience_id": "Young Professionals",
      "metrics": {
        "roi": [
          {"month": "2024-01", "value": 3.0},
          {"month": "2024-02", "value": 3.1},
          ...
        ],
        "conversion_rate": [
          {"month": "2024-01", "value": 0.040},
          {"month": "2024-02", "value": 0.041},
          ...
        ],
        "acquisition_cost": [
          {"month": "2024-01", "value": 39.25},
          {"month": "2024-02", "value": 38.75},
          ...
        ],
        "ctr": [
          {"month": "2024-01", "value": 0.026},
          {"month": "2024-02", "value": 0.027},
          ...
        ]
      }
    },
    {
      "audience_id": "Senior Executives",
      "metrics": {
        "roi": [
          {"month": "2024-01", "value": 2.4},
          {"month": "2024-02", "value": 2.5},
          ...
        ],
        "conversion_rate": [
          {"month": "2024-01", "value": 0.022},
          {"month": "2024-02", "value": 0.023},
          ...
        ],
        "acquisition_cost": [
          {"month": "2024-01", "value": 48.50},
          {"month": "2024-02", "value": 47.75},
          ...
        ],
        "ctr": [
          {"month": "2024-01", "value": 0.018},
          {"month": "2024-02", "value": 0.019},
          ...
        ]
      }
    },
    ...
  ]
}
```

### High-ROI Target Audience Clusters
**Visualization**: High-ROI Target Audience Clusters (Table)
**API Endpoint**: `GET /api/companies/{company_id}/audiences/clusters`
**Query Parameters**:
- `min_roi`: 3.0
**Response Format**:
```json
{
  "clusters": [
    {
      "cluster_id": 1,
      "primary_audience": "Young Professionals",
      "related_audiences": ["Tech Enthusiasts", "Urban Millennials"],
      "avg_roi": 3.4,
      "avg_conversion_rate": 0.044,
      "avg_acquisition_cost": 36.25,
      "avg_ctr": 0.033,
      "campaign_count": 8,
      "common_channels": ["Social Media", "Mobile"]
    },
    ...
  ]
}
```

### Target Audience Performance by Location
**Visualization**: Target Audience Performance by Location (Heatmap)
**API Endpoint**: `GET /api/companies/{company_id}/audiences/performance_matrix`
**Query Parameters**:
- `dimension_type`: "location"
**Response Format**:
```json
{
  "matrix": [
    {
      "audience_id": "Young Professionals",
      "dimensions": [
        {
          "dimension_value": "Urban",
          "metrics": {
            "roi": 3.5,
            "conversion_rate": 0.045,
            "acquisition_cost": 36.25,
            "ctr": 0.034
          }
        },
        {
          "dimension_value": "Suburban",
          "metrics": {
            "roi": 3.1,
            "conversion_rate": 0.040,
            "acquisition_cost": 38.75,
            "ctr": 0.030
          }
        },
        {
          "dimension_value": "Rural",
          "metrics": {
            "roi": 2.7,
            "conversion_rate": 0.035,
            "acquisition_cost": 41.50,
            "ctr": 0.025
          }
        },
        ...
      ]
    },
    ...
  ]
}
```

### Target Audience Performance by Language
**Visualization**: Target Audience Performance by Language (Heatmap)
**API Endpoint**: `GET /api/companies/{company_id}/audiences/performance_matrix`
**Query Parameters**:
- `dimension_type`: "language"
**Response Format**:
```json
{
  "matrix": [
    {
      "audience_id": "Young Professionals",
      "dimensions": [
        {
          "dimension_value": "English",
          "metrics": {
            "roi": 3.3,
            "conversion_rate": 0.042,
            "acquisition_cost": 37.50,
            "ctr": 0.032
          }
        },
        {
          "dimension_value": "Spanish",
          "metrics": {
            "roi": 3.5,
            "conversion_rate": 0.045,
            "acquisition_cost": 36.25,
            "ctr": 0.034
          }
        },
        ...
      ]
    },
    ...
  ]
}
```

### Target Audience Response by Goal
**Visualization**: Target Audience Response by Goal (Bar chart)
**API Endpoint**: `GET /api/companies/{company_id}/audiences/performance_matrix`
**Query Parameters**:
- `dimension_type`: "goal"
**Response Format**:
```json
{
  "matrix": [
    {
      "audience_id": "Young Professionals",
      "dimensions": [
        {
          "dimension_value": "Brand Awareness",
          "metrics": {
            "roi": 2.9,
            "conversion_rate": 0.035,
            "acquisition_cost": 40.25,
            "ctr": 0.026
          }
        },
        {
          "dimension_value": "Lead Generation",
          "metrics": {
            "roi": 3.6,
            "conversion_rate": 0.048,
            "acquisition_cost": 35.75,
            "ctr": 0.036
          }
        },
        {
          "dimension_value": "Sales Conversion",
          "metrics": {
            "roi": 3.2,
            "conversion_rate": 0.041,
            "acquisition_cost": 38.50,
            "ctr": 0.030
          }
        },
        ...
      ]
    },
    ...
  ]
}
```

### Target Audience Anomalies
**Visualization**: Target Audience Anomalies (Timeline)
**API Endpoint**: `GET /api/companies/{company_id}/audience_anomalies`
**Query Parameters**:
- `threshold`: 2.0
**Response Format**:
```json
{
  "anomalies": [
    {
      "audience_id": "Young Professionals",
      "metric": "roi",
      "expected_value": 3.2,
      "actual_value": 4.1,
      "z_score": 2.5,
      "date": "2024-03-20",
      "explanation": "Significant ROI increase for Young Professionals segment following targeted campaign optimization"
    },
    ...
  ]
}
```

### Target Audience Industry Benchmarks
**Visualization**: Target Audience Industry Benchmarks (Table)
**API Endpoint**: `GET /api/companies/{company_id}/audiences/benchmarks`
**Response Format**:
```json
{
  "audiences": [
    {
      "audience_id": "Young Professionals",
      "company_roi": 3.2,
      "industry_roi": 2.7,
      "roi_percentile": 82,
      "company_conversion_rate": 0.042,
      "industry_conversion_rate": 0.035,
      "conversion_rate_percentile": 75,
      "company_acquisition_cost": 38.25,
      "industry_acquisition_cost": 42.50,
      "acquisition_cost_percentile": 68,
      "company_ctr": 0.028,
      "industry_ctr": 0.023,
      "ctr_percentile": 80
    },
    ...
  ]
}
```
