"""
API Documentation for the Meta Demo project.

This module contains documentation for all API endpoints in the Meta Demo project.
Each endpoint is documented with its purpose, parameters, and response format.
The API follows a hierarchical structure with Company as the primary dimension.
"""

# Dictionary of API endpoint documentation
API_DOCS = {
    # Company-level endpoints
    "/api/companies": {
        "description": "Get a list of all companies in the dataset with metadata",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "company": "string",
                    "campaign_count": "integer",
                    "audience_count": "integer",
                    "channel_count": "integer",
                    "goal_count": "integer"
                }
            },
            "example": [
                {
                    "company": "Cyber Circuit",
                    "campaign_count": 24,
                    "audience_count": 5,
                    "channel_count": 6,
                    "goal_count": 4
                }
            ]
        }
    },
    

    
    "/api/companies/{company_id}/monthly_metrics": {
        "description": "Get monthly metrics for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "include_anomalies",
                "in": "query",
                "required": False,
                "description": "Whether to include anomaly detection",
                "type": "boolean",
                "default": False
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "object",
                    "properties": {
                        "conversion_rate": "array",
                        "roi": "array",
                        "acquisition_cost": "array",
                        "ctr": "array"
                    }
                },
                "anomalies": "array"
            },
            "example": {
                "metrics": {
                    "conversion_rate": [
                        {"month": "2024-01", "value": 0.0321, "is_anomaly": False},
                        {"month": "2024-02", "value": 0.0334, "is_anomaly": False},
                        {"month": "2024-03", "value": 0.0412, "is_anomaly": True}
                    ],
                    "roi": [
                        {"month": "2024-01", "value": 2.4},
                        {"month": "2024-02", "value": 2.6},
                        {"month": "2024-03", "value": 2.7}
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
        }
    },
    
    # Audience-level endpoints
    "/api/companies/{company_id}/audiences": {
        "description": "Get list of target audiences for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "include_metrics",
                "in": "query",
                "required": False,
                "description": "Whether to include metrics with each audience",
                "type": "boolean",
                "default": False
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "audiences": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "audience_id": "string",
                            "campaign_count": "integer",
                            "avg_roi": "number",
                            "avg_conversion_rate": "number",
                            "avg_acquisition_cost": "number",
                            "avg_ctr": "number"
                        }
                    }
                }
            },
            "example": {
                "audiences": [
                    {
                        "audience_id": "Young Professionals",
                        "campaign_count": 12,
                        "avg_roi": 3.2,
                        "avg_conversion_rate": 0.042,
                        "avg_acquisition_cost": 38.25,
                        "avg_ctr": 0.028
                    }
                ]
            }
        }
    },
    
    "/api/companies/{company_id}/audiences/monthly_metrics": {
        "description": "Get monthly metrics by target audience for a specific company",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "audiences": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "audience_id": "string",
                            "metrics": {
                                "type": "object",
                                "properties": {
                                    "roi": "array",
                                    "conversion_rate": "array",
                                    "acquisition_cost": "array",
                                    "ctr": "array"
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/audiences/performance_matrix": {
        "description": "Get audience performance matrix data",
        "method": "GET",
        "parameters": [
            {
                "name": "dimension_type",
                "in": "query",
                "required": True,
                "description": "Type of dimension to analyze (location, language, goal)",
                "type": "string",
                "enum": ["location", "language", "goal"]
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "matrix": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "audience_id": "string",
                            "dimensions": "array"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/audiences/clusters": {
        "description": "Get audience clustering data",
        "method": "GET",
        "parameters": [
            {
                "name": "min_roi",
                "in": "query",
                "required": False,
                "description": "Minimum ROI threshold for clusters",
                "type": "number",
                "default": 3.0
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "clusters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cluster_id": "integer",
                            "primary_audience": "string",
                            "related_audiences": "array",
                            "avg_roi": "number",
                            "avg_conversion_rate": "number",
                            "avg_acquisition_cost": "number",
                            "avg_ctr": "number",
                            "campaign_count": "integer",
                            "common_channels": "array"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/audiences/benchmarks": {
        "description": "Get audience industry benchmarks",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "audiences": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "audience_id": "string",
                            "company_roi": "number",
                            "industry_roi": "number",
                            "roi_percentile": "number",
                            "company_conversion_rate": "number",
                            "industry_conversion_rate": "number",
                            "conversion_rate_percentile": "number",
                            "company_acquisition_cost": "number",
                            "industry_acquisition_cost": "number",
                            "acquisition_cost_percentile": "number",
                            "company_ctr": "number",
                            "industry_ctr": "number",
                            "ctr_percentile": "number"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/audience_anomalies": {
        "description": "Get audience anomalies for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "threshold",
                "in": "query",
                "required": False,
                "description": "Z-score threshold for anomaly detection",
                "type": "number",
                "default": 2.0
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "anomalies": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "audience_id": "string",
                            "metric": "string",
                            "expected_value": "number",
                            "actual_value": "number",
                            "z_score": "number",
                            "date": "string",
                            "explanation": "string"
                        }
                    }
                }
            },
            "example": {
                "anomalies": [
                    {
                        "audience_id": "Young Professionals",
                        "metric": "roi",
                        "expected_value": 3.2,
                        "actual_value": 4.1,
                        "z_score": 2.5,
                        "date": "2022-03-31",
                        "explanation": "Unusually high ROI"
                    }
                ]
            }
        }
    },
    
    # Channel-level endpoints
    "/api/companies/{company_id}/channels": {
        "description": "Get list of channels for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "include_metrics",
                "in": "query",
                "required": False,
                "description": "Whether to include metrics with each channel",
                "type": "boolean",
                "default": False
            },
            {
                "name": "include_extended_metrics",
                "in": "query",
                "required": False,
                "description": "Whether to include extended metrics (impressions, clicks, etc.)",
                "type": "boolean",
                "default": False
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "channels": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "campaign_count": "integer",
                            "avg_roi": "number",
                            "avg_conversion_rate": "number",
                            "avg_acquisition_cost": "number",
                            "avg_ctr": "number",
                            "total_spend": "number",
                            "total_impressions": "number",
                            "total_clicks": "number",
                            "total_conversions": "number",
                            "total_revenue": "number"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/monthly_channel_metrics": {
        "description": "Get monthly metrics by channel for a specific company",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "channels": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "metrics": {
                                "type": "object",
                                "properties": {
                                    "conversion_rate": "array",
                                    "roi": "array",
                                    "acquisition_cost": "array",
                                    "ctr": "array"
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    

    
    "/api/companies/{company_id}/channels/performance_matrix": {
        "description": "Get channel performance matrix data",
        "method": "GET",
        "parameters": [
            {
                "name": "dimension_type",
                "in": "query",
                "required": True,
                "description": "Type of dimension to analyze (target_audience, goal)",
                "type": "string",
                "enum": ["target_audience", "goal"]
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "matrix": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "dimensions": "array"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/channels/benchmarks": {
        "description": "Get channel industry benchmarks",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "channels": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "company_roi": "number",
                            "industry_roi": "number",
                            "roi_percentile": "number",
                            "company_conversion_rate": "number",
                            "industry_conversion_rate": "number",
                            "conversion_rate_percentile": "number",
                            "company_acquisition_cost": "number",
                            "industry_acquisition_cost": "number",
                            "acquisition_cost_percentile": "number",
                            "company_ctr": "number",
                            "industry_ctr": "number",
                            "ctr_percentile": "number"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/channels/budget_optimizer": {
        "description": "Get budget allocation optimization recommendations",
        "method": "GET",
        "parameters": [
            {
                "name": "total_budget",
                "in": "query",
                "required": True,
                "description": "Total budget to allocate",
                "type": "number"
            },
            {
                "name": "optimization_goal",
                "in": "query",
                "required": False,
                "description": "Metric to optimize for",
                "type": "string",
                "default": "roi",
                "enum": ["roi"]
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "current_allocation": "array",
                "optimized_allocation": "array",
                "optimization_metrics": "object"
            }
        }
    },
    
    "/api/companies/{company_id}/channel_anomalies": {
        "description": "Get anomalies for channels of a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "threshold",
                "in": "query",
                "required": False,
                "description": "Z-score threshold for anomaly detection",
                "type": "number",
                "default": 2.0
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "anomalies": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "metric": "string",
                            "expected_value": "number",
                            "actual_value": "number",
                            "z_score": "number",
                            "date": "string",
                            "explanation": "string"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/channel_budget_optimizer": {
        "description": "Get budget allocation optimization recommendations for channels of a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "total_budget",
                "in": "query",
                "required": False,
                "description": "Total budget to allocate across channels. If not provided, current total spend will be used.",
                "type": "number",
                "default": 0
            },
            {
                "name": "optimization_goal",
                "in": "query",
                "required": False,
                "description": "Metric to optimize for (currently only supports 'roi')",
                "type": "string",
                "default": "roi"
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "current_allocation": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "amount": "number",
                            "percentage": "number",
                            "roi": "number"
                        }
                    }
                },
                "optimized_allocation": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "channel_id": "string",
                            "amount": "number",
                            "percentage": "number",
                            "roi": "number",
                            "change_direction": "string",
                            "change_strength": "string"
                        }
                    }
                },
                "optimization_metrics": {
                    "type": "object",
                    "properties": {
                        "total_budget": "number",
                        "optimization_goal": "string",
                        "projected_improvement": "number"
                    }
                }
            },
            "example": {
                "current_allocation": [
                    {
                        "channel_id": "Social Media",
                        "amount": 25000,
                        "percentage": 25.0,
                        "roi": 3.2
                    },
                    {
                        "channel_id": "Search",
                        "amount": 35000,
                        "percentage": 35.0,
                        "roi": 4.1
                    }
                ],
                "optimized_allocation": [
                    {
                        "channel_id": "Social Media",
                        "amount": 20000,
                        "percentage": 20.0,
                        "roi": 3.5,
                        "change_direction": "decrease_spend",
                        "change_strength": "moderate"
                    },
                    {
                        "channel_id": "Search",
                        "amount": 40000,
                        "percentage": 40.0,
                        "roi": 4.5,
                        "change_direction": "increase_spend",
                        "change_strength": "strong"
                    }
                ],
                "optimization_metrics": {
                    "total_budget": 100000,
                    "optimization_goal": "roi",
                    "projected_improvement": 12.5
                }
            }
        }
    },
    
    # Campaign-level endpoints
    "/api/companies/{company_id}/goals": {
        "description": "Get list of campaign goals for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "include_metrics",
                "in": "query",
                "required": False,
                "description": "Whether to include metrics with each goal",
                "type": "boolean",
                "default": False
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "goals": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "goal_id": "string",
                            "campaign_count": "integer",
                            "avg_roi": "number",
                            "avg_conversion_rate": "number",
                            "avg_acquisition_cost": "number",
                            "avg_ctr": "number"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/monthly_campaign_metrics": {
        "description": "Get monthly campaign metrics for a specific company",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "object",
                    "properties": {
                        "roi": "array",
                        "conversion_rate": "array",
                        "acquisition_cost": "array",
                        "ctr": "array"
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/goals": {
        "description": "Get list of campaign goals for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "include_metrics",
                "in": "query",
                "required": False,
                "description": "Whether to include metrics with each goal",
                "type": "boolean",
                "default": False
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "goals": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "goal_id": "string",
                            "campaign_count": "integer",
                            "avg_roi": "number",
                            "avg_conversion_rate": "number",
                            "avg_acquisition_cost": "number",
                            "avg_ctr": "number"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/monthly_campaign_metrics": {
        "description": "Get monthly campaign metrics for a specific company",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "object",
                    "properties": {
                        "roi": "array",
                        "conversion_rate": "array",
                        "acquisition_cost": "array",
                        "ctr": "array"
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/campaign_duration_analysis": {
        "description": "Get campaign duration impact analysis",
        "method": "GET",
        "parameters": [
            {
                "name": "dimension",
                "in": "query",
                "required": False,
                "description": "Dimension to analyze (audience, channel, goal)",
                "type": "string",
                "default": "audience"
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "data": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "campaign_id": "string",
                            "duration_days": "integer",
                            "roi": "number",
                            "conversion_rate": "number",
                            "dimension_value": "string"
                        }
                    }
                },
                "summary": {
                    "type": "object",
                    "properties": {
                        "avg_duration": "number",
                        "min_duration": "number",
                        "max_duration": "number",
                        "count": "integer"
                    }
                }
            }
        }
    },
    

    
    "/api/companies/{company_id}/campaign_clusters": {
        "description": "Get winning campaign combinations",
        "method": "GET",
        "parameters": [
            {
                "name": "min_performance_score",
                "in": "query",
                "required": False,
                "description": "Minimum performance score for clusters",
                "type": "number",
                "default": 8.0
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "clusters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cluster_id": "integer",
                            "goal": "string",
                            "target_audience": "string",
                            "channel": "string",
                            "avg_duration_days": "integer",
                            "avg_roi": "number",
                            "avg_conversion_rate": "number",
                            "campaign_count": "integer"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/top_bottom_performers": {
        "description": "Get top and bottom performing campaigns",
        "method": "GET",
        "parameters": [
            {
                "name": "limit",
                "in": "query",
                "required": False,
                "description": "Number of campaigns to return in each category",
                "type": "integer",
                "default": 5
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "top_performers": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "campaign_id": "string",
                            "name": "string",
                            "roi": "number",
                            "conversion_rate": "number",
                            "acquisition_cost": "number",
                            "ctr": "number",
                            "target_audience": "string",
                            "channel": "string",
                            "goal": "string"
                        }
                    }
                },
                "bottom_performers": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "campaign_id": "string",
                            "name": "string",
                            "roi": "number",
                            "conversion_rate": "number",
                            "acquisition_cost": "number",
                            "ctr": "number",
                            "target_audience": "string",
                            "channel": "string",
                            "goal": "string"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/campaign_anomalies": {
        "description": "Get campaign anomalies for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "threshold",
                "in": "query",
                "required": False,
                "description": "Z-score threshold for anomaly detection",
                "type": "number",
                "default": 2.0
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "anomalies": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "campaign_id": "string",
                            "name": "string",
                            "metric": "string",
                            "expected_value": "number",
                            "actual_value": "number",
                            "z_score": "number",
                            "date": "string",
                            "explanation": "string"
                        }
                    }
                }
            }
        }
    },
    
    "/api/companies/{company_id}/campaign_forecasts": {
        "description": "Get campaign performance forecasts",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "object",
            "properties": {
                "forecasts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "period": "string",
                            "roi": "object",
                            "conversion_rate": "object",
                            "acquisition_cost": "object",
                            "ctr": "object"
                        }
                    }
                }
            }
        }
    }
}

def get_endpoint_docs(endpoint: str) -> dict:
    """Get documentation for a specific endpoint."""
    if endpoint in API_DOCS:
        return API_DOCS[endpoint]
    return {"error": f"Endpoint '{endpoint}' not found in documentation"}

def get_all_docs() -> dict:
    """Get documentation for all endpoints."""
    return API_DOCS
