"""
API module for the Meta Demo project.
Provides REST endpoints for various functionalities including Vanna.ai integration,
campaign analytics, and performance metrics.
"""

import os
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import pandas as pd
import logging
from typing import Dict, Any, Optional, List, Union
import json
from http import HTTPStatus

from app.vanna_core import initialize_vanna, VANNA_AVAILABLE, GEMINI_AVAILABLE
from app.api_utils import (
    get_companies, get_company_metrics, get_time_series_data,
    get_segment_performance, get_channel_performance, get_campaign_details,
    get_campaign_clusters, get_campaign_duration_analysis, get_performance_matrix,
    get_forecasting, get_top_bottom_performers, get_anomalies
)
from app.api_docs import get_endpoint_docs, get_all_docs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables
vanna_instance = None
app = None

def init_app() -> Flask:
    """Initialize and return the Flask app with all routes configured."""
    global vanna_instance, app
    
    # Create a new Flask app instance
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes
    
    # Initialize Vanna using environment variables
    if VANNA_AVAILABLE and GEMINI_AVAILABLE:
        try:
            # Get configuration from environment variables
            api_key = os.environ.get("GOOGLE_API_KEY")
            model = os.environ.get("VANNA_MODEL", "gemini-2.5-pro-preview-03-25")
            temp_str = os.environ.get("VANNA_TEMPERATURE", "0.2")
            
            # Convert temperature to float
            try:
                temperature = float(temp_str)
            except ValueError:
                logger.warning(f"Invalid temperature value '{temp_str}', using default 0.2")
                temperature = 0.2
            
            if not api_key:
                logger.error("No Google API key found in environment variables. Vanna cannot be initialized.")
            else:
                # Initialize Vanna with explicit parameters
                vanna_instance = initialize_vanna(
                    api_key=api_key,
                    model=model,
                    temperature=temperature,
                    train=False
                )
                logger.info(f"Vanna initialized successfully with model={model}, temperature={temperature}")
        except Exception as e:
            logger.error(f"Error initializing Vanna: {str(e)}")
    
    # Register routes
    register_routes(app)
    
    return app

def register_routes(app: Flask) -> None:
    """Register all API routes."""
    
    @app.route('/api/docs', methods=['GET'])
    def api_docs():
        """API documentation endpoint."""
        endpoint = request.args.get('endpoint')
        if endpoint:
            return jsonify(get_endpoint_docs(endpoint))
        else:
            return jsonify(get_all_docs())
    
    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({
            "status": "ok",
            "vanna_available": VANNA_AVAILABLE,
            "gemini_available": GEMINI_AVAILABLE,
            "vanna_initialized": vanna_instance is not None
        })
    
    @app.route('/api/ask', methods=['POST'])
    def ask_question():
        """
        Process a natural language question and return SQL and results.
        
        Expected JSON payload:
        {
            "question": "What is the ROI by channel?",
            "company": "Cyber Circuit"  # Optional filter
        }
        """
        # Check if Vanna is initialized
        if not vanna_instance:
            return jsonify({
                "error": "Vanna is not initialized. Make sure the API key is provided."
            }), 500
        
        # Get request data
        data = request.json
        if not data or 'question' not in data:
            return jsonify({
                "error": "Missing required parameter: question"
            }), 400
        
        question = data['question']
        company_filter = data.get('company')  # Optional
        
        # Modify question if company filter is provided
        if company_filter:
            question = f"{question} for {company_filter}"
        
        try:
            # Process the question using Vanna
            result = vanna_instance.ask(question)
            
            # Check for errors in the result
            if 'error' in result:
                return jsonify({
                    "error": result['error']
                }), 500
            
            # Return the result
            return jsonify(result)
        
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}")
            return jsonify({
                "error": f"Failed to process question: {str(e)}"
            }), 500
    
    @app.route('/api/companies', methods=['GET'])
    def get_companies_endpoint():
        """Get list of available companies."""
        try:
            companies = get_companies()
            return jsonify(companies)
        except Exception as e:
            logger.error(f"Error getting companies: {str(e)}")
            return jsonify({
                "error": f"Failed to get companies: {str(e)}"
            }), 500
    
    @app.route('/api/insights/<company_id>', methods=['GET'])
    def get_insights_endpoint(company_id):
        """
        Get AI-generated insights for a specific company.
        
        Path parameters:
        - company_id: Company name to get insights for
        """
        try:
            # Simple placeholder text - will be implemented with real insights later
            insight_text = f"Based on recent performance data for {company_id}, email campaigns show higher ROI while social media channels need optimization. Consider reallocating budget to high-performing segments."
            
            # Return the insight as expected by the frontend
            return jsonify({
                "insight": insight_text
            })
        
        except Exception as e:
            logger.error(f"Error getting insights: {str(e)}")
            return jsonify({
                "error": f"Failed to get insights: {str(e)}"
            }), 500
    
    @app.route('/api/metrics/<company_id>', methods=['GET'])
    def get_metrics_endpoint(company_id):
        """
        Get KPI metrics for a specific company.
        
        Path parameters:
        - company_id: Company name to get metrics for
        """
        try:
            metrics = get_company_metrics(company_id)
            if not metrics:
                return jsonify({
                    "error": f"Company '{company_id}' not found"
                }), 404
            return jsonify(metrics)
        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            return jsonify({
                "error": f"Failed to get metrics: {str(e)}"
            }), 500
    
    @app.route('/api/time-series/<company_id>', methods=['GET'])
    def get_time_series_endpoint(company_id):
        """
        Get time series data for a specific company.
        
        Path parameters:
        - company_id: Company name to get time series data for
        """
        try:
            time_series = get_time_series_data(company_id)
            if not time_series:
                return jsonify({
                    "error": f"No time series data found for company '{company_id}'"
                }), 404
            return jsonify(time_series)
        except Exception as e:
            logger.error(f"Error getting time series data: {str(e)}")
            return jsonify({
                "error": f"Failed to get time series data: {str(e)}"
            }), 500
    
    @app.route('/api/segments/<company_id>', methods=['GET'])
    def get_segments_endpoint(company_id):
        """
        Get segment performance for a specific company.
        
        Path parameters:
        - company_id: Company name to get segment performance for
        """
        try:
            segments = get_segment_performance(company_id)
            if not segments:
                return jsonify({
                    "error": f"No segment data found for company '{company_id}'"
                }), 404
            return jsonify(segments)
        except Exception as e:
            logger.error(f"Error getting segment performance: {str(e)}")
            return jsonify({
                "error": f"Failed to get segment performance: {str(e)}"
            }), 500
    
    @app.route('/api/channels/<company_id>', methods=['GET'])
    def get_channels_endpoint(company_id):
        """
        Get channel performance for a specific company.
        
        Path parameters:
        - company_id: Company name to get channel performance for
        """
        try:
            channels = get_channel_performance(company_id)
            if not channels:
                return jsonify({
                    "error": f"No channel data found for company '{company_id}'"
                }), 404
            return jsonify(channels)
        except Exception as e:
            logger.error(f"Error getting channel performance: {str(e)}")
            return jsonify({
                "error": f"Failed to get channel performance: {str(e)}"
            }), 500
    
    @app.route('/api/campaigns/<company_id>', methods=['GET'])
    def get_campaigns_endpoint(company_id):
        """
        Get campaign details for a specific company.
        
        Path parameters:
        - company_id: Company name to get campaign details for
        """
        try:
            campaigns = get_campaign_details(company_id)
            if not campaigns:
                return jsonify({
                    "error": f"No campaign data found for company '{company_id}'"
                }), 404
            return jsonify(campaigns)
        except Exception as e:
            logger.error(f"Error getting campaign details: {str(e)}")
            return jsonify({
                "error": f"Failed to get campaign details: {str(e)}"
            }), 500
    
    @app.route('/api/campaign-clusters/<company_id>', methods=['GET'])
    def get_campaign_clusters_endpoint(company_id):
        """
        Get high-performing campaign combinations for a specific company.
        
        Path parameters:
        - company_id: Company name to get campaign clusters for
        """
        try:
            clusters = get_campaign_clusters(company_id)
            if not clusters:
                return jsonify({
                    "error": f"No campaign cluster data found for company '{company_id}'"
                }), 404
            return jsonify(clusters)
        except Exception as e:
            logger.error(f"Error getting campaign clusters: {str(e)}")
            return jsonify({
                "error": f"Failed to get campaign clusters: {str(e)}"
            }), 500
    
    @app.route('/api/campaign-duration/<company_id>', methods=['GET'])
    def get_campaign_duration_endpoint(company_id):
        """
        Get optimal duration analysis by segment/channel for a specific company.
        
        Path parameters:
        - company_id: Company name to get campaign duration analysis for
        """
        try:
            duration_analysis = get_campaign_duration_analysis(company_id)
            if not duration_analysis or not duration_analysis.get("overall_optimal"):
                return jsonify({
                    "error": f"No campaign duration data found for company '{company_id}'"
                }), 404
            return jsonify(duration_analysis)
        except Exception as e:
            logger.error(f"Error getting campaign duration analysis: {str(e)}")
            return jsonify({
                "error": f"Failed to get campaign duration analysis: {str(e)}"
            }), 500
    
    @app.route('/api/performance-matrix/<company_id>', methods=['GET'])
    def get_performance_matrix_endpoint(company_id):
        """
        Get goal vs segment performance matrix for a specific company.
        
        Path parameters:
        - company_id: Company name to get performance matrix for
        """
        try:
            matrix = get_performance_matrix(company_id)
            if not matrix:
                return jsonify({
                    "error": f"No performance matrix data found for company '{company_id}'"
                }), 404
            return jsonify(matrix)
        except Exception as e:
            logger.error(f"Error getting performance matrix: {str(e)}")
            return jsonify({
                "error": f"Failed to get performance matrix: {str(e)}"
            }), 500
    
    @app.route('/api/forecasting/<company_id>', methods=['GET'])
    def get_forecasting_endpoint(company_id):
        """
        Get campaign performance forecasts for a specific company.
        
        Path parameters:
        - company_id: Company name to get forecasts for
        """
        try:
            forecasts = get_forecasting(company_id)
            if not forecasts:
                return jsonify({
                    "error": f"No forecast data found for company '{company_id}'"
                }), 404
            return jsonify(forecasts)
        except Exception as e:
            logger.error(f"Error getting forecasts: {str(e)}")
            return jsonify({
                "error": f"Failed to get forecasts: {str(e)}"
            }), 500
    
    @app.route('/api/top-bottom/<company_id>', methods=['GET'])
    def get_top_bottom_endpoint(company_id):
        """
        Get top and bottom performing campaigns for a specific company.
        
        Path parameters:
        - company_id: Company name to get top/bottom performers for
        """
        try:
            performers = get_top_bottom_performers(company_id)
            if not performers or not any(performers.values()):
                return jsonify({
                    "error": f"No performance data found for company '{company_id}'"
                }), 404
            return jsonify(performers)
        except Exception as e:
            logger.error(f"Error getting top/bottom performers: {str(e)}")
            return jsonify({
                "error": f"Failed to get top/bottom performers: {str(e)}"
            }), 500
    
    @app.route('/api/anomalies/<company_id>', methods=['GET'])
    def get_anomalies_endpoint(company_id):
        """
        Get anomaly detection with context for a specific company.
        
        Path parameters:
        - company_id: Company name to get anomalies for
        """
        try:
            anomalies = get_anomalies(company_id)
            if not anomalies:
                return jsonify({
                    "error": f"No anomaly data found for company '{company_id}'"
                }), 404
            return jsonify(anomalies)
        except Exception as e:
            logger.error(f"Error getting anomalies: {str(e)}")
            return jsonify({
                "error": f"Failed to get anomalies: {str(e)}"
            }), 500
            
    # Legacy endpoint for backward compatibility
    @app.route('/api/kpis', methods=['GET'])
    def get_kpis():
        """
        Get KPIs for a specific company or all companies.
        
        Query parameters:
        - company: Optional company name to filter KPIs
        """
        company = request.args.get('company')
        
        try:
            if company:
                metrics = get_company_metrics(company)
                if not metrics:
                    return jsonify({
                        "error": f"Company '{company}' not found"
                    }), 404
                return jsonify(metrics)
            else:
                metrics_by_company = get_company_metrics()
                return jsonify(metrics_by_company)
        
        except Exception as e:
            logger.error(f"Error getting KPIs: {str(e)}")
            return jsonify({
                "error": f"Failed to get KPIs: {str(e)}"
            }), 500
    
    # Legacy endpoint for backward compatibility
    @app.route('/api/insights', methods=['GET'])
    def get_insights():
        """
        Get AI-generated insights for a specific company.
        
        Query parameters:
        - company: Company name to get insights for
        """
        company = request.args.get('company')
        if not company:
            return jsonify({
                "error": "Missing required parameter: company"
            }), 400
        
        # Redirect to the new endpoint
        return get_insights_endpoint(company)

# Note: The Flask app is initialized in the init_app() function
# and the handler function is now in index.py
