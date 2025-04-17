"""
API Documentation for the Meta Demo project.

This module contains documentation for all API endpoints in the Meta Demo project.
Each endpoint is documented with its purpose, parameters, and response format.
"""

# Dictionary of API endpoint documentation
API_DOCS = {
    "/api/companies": {
        "description": "Get a list of all companies in the dataset",
        "method": "GET",
        "parameters": [],
        "response_format": {
            "type": "array",
            "items": "string",
            "example": ["Cyber Circuit", "TechNova", "Quantum Dynamics"]
        }
    },
    "/api/insights/{company_id}": {
        "description": "Get natural language insights for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get insights for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "insight": "string"
            },
            "example": {
                "insight": "TechNova's Facebook campaigns outperformed expectations with a 15% increase in ROI."
            }
        }
    },
    "/api/metrics/{company_id}": {
        "description": "Get KPI metrics for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": False,
                "description": "Name of the company to get metrics for. If not provided, returns metrics for all companies.",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "roi": "number",
                "ctr": "number",
                "conversion_rate": "number",
                "acquisition_cost": "number",
                "engagement_score": "number"
            },
            "example": {
                "roi": 2.1,
                "ctr": 0.028,
                "conversion_rate": 0.019,
                "acquisition_cost": 38.75,
                "engagement_score": 3.9
            }
        }
    },
    "/api/time-series/{company_id}": {
        "description": "Get time series data for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get time series data for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": "string",
                    "roi": "number",
                    "ctr": "number",
                    "conversion_rate": "number",
                    "acquisition_cost": "number"
                }
            },
            "example": [
                {
                    "date": "2023-01",
                    "roi": 2.1,
                    "ctr": 0.028,
                    "conversion_rate": 0.019,
                    "acquisition_cost": 38.75
                }
            ]
        }
    },
    "/api/segments/{company_id}": {
        "description": "Get segment performance for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get segment performance for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "segment": "string",
                    "roi": "number",
                    "conversion_rate": "number",
                    "acquisition_cost": "number",
                    "ctr": "number",
                    "roi_rank": "number",
                    "vs_segment_avg": "number"
                }
            }
        }
    },
    "/api/channels/{company_id}": {
        "description": "Get channel performance for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get channel performance for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "channel": "string",
                    "roi": "number",
                    "conversion_rate": "number",
                    "acquisition_cost": "number",
                    "ctr": "number"
                }
            }
        }
    },
    "/api/campaigns/{company_id}": {
        "description": "Get campaign details for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get campaign details for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "campaign_id": "string",
                    "goal": "string",
                    "channel": "string",
                    "segment": "string",
                    "roi": "number",
                    "conversion_rate": "number",
                    "acquisition_cost": "number",
                    "ctr": "number",
                    "duration": "number"
                }
            }
        }
    },
    "/api/campaign-clusters/{company_id}": {
        "description": "Get high-performing campaign combinations for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get campaign clusters for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "goal": "string",
                    "segment": "string",
                    "channel": "string",
                    "duration": "string",
                    "roi": "number",
                    "conversion_rate": "number",
                    "acquisition_cost": "number",
                    "ctr": "number",
                    "is_winning_combination": "boolean"
                }
            }
        }
    },
    "/api/campaign-duration/{company_id}": {
        "description": "Get optimal duration analysis by segment/channel for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get campaign duration analysis for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "overall_optimal": "string",
                "by_segment": "array",
                "by_channel": "array",
                "by_goal": "array"
            }
        }
    },
    "/api/performance-matrix/{company_id}": {
        "description": "Get goal vs segment performance matrix for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get performance matrix for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "goal": "string",
                    "segment": "string",
                    "roi": "number",
                    "conversion_rate": "number",
                    "acquisition_cost": "number",
                    "ctr": "number",
                    "composite_score": "number",
                    "vs_goal_avg": "number",
                    "vs_segment_avg": "number",
                    "is_top_performer": "boolean"
                }
            }
        }
    },
    "/api/forecasting/{company_id}": {
        "description": "Get campaign performance forecasts for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get forecasts for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": "string",
                    "roi": "number",
                    "roi_lower": "number",
                    "roi_upper": "number",
                    "conversion_rate": "number",
                    "conversion_rate_lower": "number",
                    "conversion_rate_upper": "number",
                    "acquisition_cost": "number",
                    "acquisition_cost_lower": "number",
                    "acquisition_cost_upper": "number",
                    "ctr": "number",
                    "ctr_lower": "number",
                    "ctr_upper": "number",
                    "is_forecast": "boolean"
                }
            }
        }
    },
    "/api/top-bottom/{company_id}": {
        "description": "Get top and bottom performing campaigns for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get top/bottom performers for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "object",
            "properties": {
                "top_roi": "array",
                "bottom_roi": "array",
                "top_conversion": "array",
                "bottom_conversion": "array",
                "top_acquisition_cost": "array",
                "bottom_acquisition_cost": "array"
            }
        }
    },
    "/api/anomalies/{company_id}": {
        "description": "Get anomaly detection with context for a specific company",
        "method": "GET",
        "parameters": [
            {
                "name": "company",
                "in": "query",
                "required": True,
                "description": "Name of the company to get anomalies for",
                "type": "string"
            }
        ],
        "response_format": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": "string",
                    "metric": "string",
                    "value": "number",
                    "expected_value": "number",
                    "z_score": "number",
                    "is_anomaly": "boolean",
                    "context": "string"
                }
            }
        }
    }
}

def get_endpoint_docs(endpoint):
    """
    Get documentation for a specific endpoint.
    
    Args:
        endpoint: The endpoint path
        
    Returns:
        dict: Documentation for the endpoint
    """
    return API_DOCS.get(endpoint, {"description": "Endpoint documentation not available"})

def get_all_docs():
    """
    Get documentation for all endpoints.
    
    Returns:
        dict: Documentation for all endpoints
    """
    return API_DOCS
