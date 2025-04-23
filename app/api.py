"""
API endpoints for the Meta Demo application.

This module provides the API endpoints for the Meta Demo application,
implementing the first 7 APIs from the API documentation.
"""

import logging
import os
from datetime import datetime
from flask import Flask, Blueprint, jsonify, request
from flask_cors import CORS
from typing import Dict, List, Any, Optional, Union

# Import Vanna core functionality
from app.vanna_core import initialize_vanna, VANNA_AVAILABLE, GEMINI_AVAILABLE

from app.api_utils import (
    get_companies, 
    get_monthly_company_metrics
)

from app.audience_api_utils import (
    get_company_audiences,
    get_monthly_audience_metrics,
    get_audience_performance_matrix,
    get_audience_clusters,
    get_audience_benchmarks,
    get_audience_anomalies
)

from app.channel_api_utils import (
    get_company_channels,
    get_monthly_channel_metrics,
    get_channel_performance_matrix,
    get_channel_clusters,
    get_channel_benchmarks,
    get_channel_anomalies,
    get_channel_budget_optimizer
)

from app.campaign_api_utils import (
    get_company_goals,
    get_monthly_campaign_metrics,
    get_campaign_duration_analysis,
    get_campaign_clusters,
    get_campaign_future_forecast,
    get_campaign_performance_rankings
)

from app.scripts.insights_generator import get_insights_connection

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

# Create a Blueprint for the API
api_blueprint = Blueprint('api', __name__)

def register_routes(app: Flask) -> None:
    """Register all API routes."""
    
    # Register the blueprint
    app.register_blueprint(api_blueprint, url_prefix='/api')
    
    @app.route('/api/docs', methods=['GET'])
    def api_docs():
        """API documentation endpoint."""
        from app.api_docs import get_endpoint_docs, get_all_docs
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

@api_blueprint.route('/companies', methods=['GET'])
def companies():
    """
    Get a list of all companies in the dataset with metadata.
    
    Returns:
        JSON: List of companies with metadata
    """
    try:
        results = get_companies()
        return jsonify({"companies": results})
    except Exception as e:
        logger.error(f"Error in companies endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/monthly_metrics', methods=['GET'])
def company_monthly_metrics(company_id: str):
    """
    Get monthly metrics for a specific company.
    
    Args:
        company_id: Company name to get monthly metrics for
        
    Returns:
        JSON: Monthly metrics for the company
    """
    try:
        # Check for query parameters
        include_anomalies = request.args.get('include_anomalies', 'false').lower() == 'true'
        
        results = get_monthly_company_metrics(company_id, include_anomalies)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in company_monthly_metrics endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/audiences', methods=['GET'])
def company_audiences(company_id: str):
    """
    Get list of target audiences for a specific company.
    
    Args:
        company_id: Company name to get audiences for
        
    Returns:
        JSON: List of audiences for the company
    """
    try:
        # Check for query parameters
        include_metrics = request.args.get('include_metrics', 'false').lower() == 'true'
        
        results = get_company_audiences(company_id, include_metrics)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in company_audiences endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/audiences/monthly_metrics', methods=['GET'])
def audience_monthly_metrics(company_id: str):
    """
    Get monthly metrics by target audience for a specific company.
    
    Args:
        company_id: Company name to get monthly audience metrics for
        
    Returns:
        JSON: Monthly audience metrics for the company
    """
    try:
        results = get_monthly_audience_metrics(company_id)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in audience_monthly_metrics endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/audiences/performance_matrix', methods=['GET'])
def audience_performance_matrix(company_id: str):
    """
    Get audience performance matrix data.
    
    Args:
        company_id: Company name to get performance matrix for
        
    Returns:
        JSON: Performance matrix for the company
    """
    try:
        # Check for query parameters
        dimension_type = request.args.get('dimension_type', 'goal')
        
        # Validate dimension_type
        valid_dimension_types = ['goal', 'location', 'language']
        if dimension_type not in valid_dimension_types:
            return jsonify({
                "error": f"Invalid dimension_type. Must be one of: {', '.join(valid_dimension_types)}"
            }), 400
        
        results = get_audience_performance_matrix(company_id, dimension_type)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in audience_performance_matrix endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/audiences/clusters', methods=['GET'])
def audience_clusters(company_id: str):
    """
    Get high-performing audience clusters data, separated by ROI and conversion rate.
    
    Args:
        company_id: Company name to get audience clusters for
        
    Returns:
        JSON: High-performing audience clusters for the company, separated by ROI and conversion rate
    """
    try:
        # Check for query parameters
        limit = int(request.args.get('limit', 5))
        
        results = get_audience_clusters(company_id, limit)
        return jsonify(results)
    except ValueError:
        return jsonify({
            "error": "Invalid parameter value. limit must be a valid integer."
        }), 400
    except Exception as e:
        logger.error(f"Error in audience_clusters endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/audience_anomalies', methods=['GET'])
def audience_anomalies(company_id: str):
    """
    Get anomalies for target audiences of a specific company.
    
    Args:
        company_id: Company name to get audience anomalies for
        
    Returns:
        JSON: Audience anomalies for the company
    """
    try:
        # Check for query parameters
        threshold = float(request.args.get('threshold', '2.0'))
        
        results = get_audience_anomalies(company_id, threshold)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in audience_anomalies endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/audiences/benchmarks', methods=['GET'])
def audience_benchmarks(company_id: str):
    """
    Get audience industry benchmarks.
    
    Args:
        company_id: Company name to get audience benchmarks for
        
    Returns:
        JSON: Audience benchmarks for the company
    """
    try:
        results = get_audience_benchmarks(company_id)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in audience_benchmarks endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channels', methods=['GET'])
def company_channels(company_id: str):
    """
    Get channels for a specific company.
    
    Args:
        company_id: Company name to get channels for
        
    Returns:
        JSON: Channels for the company
    """
    try:
        # Check for query parameters
        include_metrics = request.args.get('include_metrics', 'false').lower() == 'true'
        
        results = get_company_channels(company_id, include_metrics)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in company_channels endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channels/monthly_metrics', methods=['GET'])
def monthly_channel_metrics(company_id: str):
    """
    Get monthly metrics for channels of a specific company.
    
    Args:
        company_id: Company name to get monthly channel metrics for
        
    Returns:
        JSON: Monthly channel metrics for the company
    """
    try:
        results = get_monthly_channel_metrics(company_id)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in monthly_channel_metrics endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channels/performance_matrix', methods=['GET'])
def channel_performance_matrix(company_id: str):
    """
    Get performance matrix for channels of a specific company.
    
    Args:
        company_id: Company name to get channel performance matrix for
        
    Returns:
        JSON: Channel performance matrix for the company
    """
    try:
        # Check for query parameters
        dimension_type = request.args.get('dimension_type', 'goal')
        
        results = get_channel_performance_matrix(company_id, dimension_type)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in channel_performance_matrix endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channels/clusters', methods=['GET'])
def channel_clusters(company_id: str):
    """
    Get channel clustering data for a specific company.
    
    Args:
        company_id: Company name to get channel clusters for
        
    Returns:
        JSON: Channel clusters for the company
    """
    try:
        # Check for query parameters
        min_roi = float(request.args.get('min_roi', '0'))
        min_conversion_rate = float(request.args.get('min_conversion_rate', '0'))
        
        results = get_channel_clusters(company_id, min_roi, min_conversion_rate)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in channel_clusters endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channels/benchmarks', methods=['GET'])
def channel_benchmarks(company_id: str):
    """
    Get channel industry benchmarks.
    
    Args:
        company_id: Company name to get channel benchmarks for
        
    Returns:
        JSON: Channel benchmarks for the company
    """
    try:
        results = get_channel_benchmarks(company_id)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in channel_benchmarks endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channel_anomalies', methods=['GET'])
def channel_anomalies(company_id: str):
    """
    Get anomalies for channels of a specific company.
    
    Args:
        company_id: Company name to get channel anomalies for
        
    Returns:
        JSON: Channel anomalies for the company
    """
    try:
        # Check for query parameters
        threshold = float(request.args.get('threshold', '2.0'))
        
        results = get_channel_anomalies(company_id, threshold)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in channel_anomalies endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/channel_budget_optimizer', methods=['GET'])
def channel_budget_optimizer(company_id: str):
    """
    Get budget allocation optimization recommendations for channels of a specific company.
    
    Args:
        company_id: Company name to get budget optimization for
        
    Returns:
        JSON: Budget allocation recommendations for the company
    """
    try:
        # Check for query parameters
        total_budget = float(request.args.get('total_budget', '0'))
        if total_budget <= 0:
            # If no budget specified, use the sum of current spend as default
            # This will be handled inside the get_channel_budget_optimizer function
            pass
            
        optimization_goal = request.args.get('optimization_goal', 'roi')
        
        results = get_channel_budget_optimizer(company_id, total_budget, optimization_goal)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in channel_budget_optimizer endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Campaign-level endpoints
@api_blueprint.route('/companies/<company_id>/goals', methods=['GET'])
def company_goals(company_id: str):
    """
    Get list of campaign goals for a specific company.
    
    Args:
        company_id: Company name to get goals for
        
    Returns:
        JSON: List of goals for the company
    """
    try:
        # Check for query parameters
        include_metrics = request.args.get('include_metrics', 'false').lower() == 'true'
        
        results = get_company_goals(company_id, include_metrics)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in company_goals endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/monthly_campaign_metrics', methods=['GET'])
def monthly_campaign_metrics(company_id: str):
    """
    Get monthly campaign metrics for a specific company.
    
    Args:
        company_id: Company name to get monthly campaign metrics for
        
    Returns:
        JSON: Monthly campaign metrics for the company
    """
    try:
        results = get_monthly_campaign_metrics(company_id)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in monthly_campaign_metrics endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/campaign_duration_analysis', methods=['GET'])
def campaign_duration_analysis(company_id: str):
    """
    Get campaign duration impact analysis.
    
    Args:
        company_id: Company name to get campaign duration analysis for
        
    Returns:
        JSON: Campaign duration analysis for the company
    """
    try:
        # Check for query parameters
        dimension = request.args.get('dimension', 'company')
        
        results = get_campaign_duration_analysis(company_id, dimension)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in campaign_duration_analysis endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/campaign_clusters', methods=['GET'])
def campaign_clusters(company_id: str):
    """
    Get high-performing campaign clusters data, separated by ROI and conversion rate.
    
    Args:
        company_id: Company name to get campaign clusters for
        
    Query Parameters:
        limit: Maximum number of clusters to return for each category, default is 5
        
    Returns:
        JSON: High-performing campaign clusters for the company, separated by ROI and conversion rate
    """
    try:
        # Check for query parameters
        limit = int(request.args.get('limit', 5))
        
        results = get_campaign_clusters(company_id, limit)
        return jsonify(results)
    except ValueError:
        return jsonify({
            "error": "Invalid parameter value. limit must be a valid integer."
        }), 400
    except Exception as e:
        logger.error(f"Error in campaign_clusters endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/campaign_performance_rankings', methods=['GET'])
def campaign_performance_rankings(company_id: str):
    """
    Get campaign performance rankings for a specific company, including top and bottom
    performers for ROI, conversion rate, revenue, and CPA (Cost Per Acquisition).
    
    Args:
        company_id: Company name to get campaign performance rankings for
        
    Query Parameters:
        limit: Maximum number of campaigns to return for each category, default is 5
        
    Returns:
        JSON: Top and bottom performing campaigns for the company, categorized by metric
    """
    try:
        # Check for query parameters
        limit = int(request.args.get('limit', 5))
        
        results = get_campaign_performance_rankings(company_id, limit)
        return jsonify(results)
    except ValueError:
        return jsonify({
            "error": "Invalid parameter value. limit must be a valid integer."
        }), 400
    except Exception as e:
        logger.error(f"Error in campaign_performance_rankings endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@api_blueprint.route('/companies/<company_id>/campaign_future_forecast', methods=['GET'])
def campaign_future_forecast(company_id: str):
    """
    Get campaign future forecast data for a specific company.
    
    Args:
        company_id: Company name to get campaign future forecast for
        
    Returns:
        JSON: Campaign future forecast data for the company
    """
    try:
        # Check for query parameters
        metric = request.args.get('metric', 'revenue')
        
        results = get_campaign_future_forecast(company_id, metric)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in campaign_future_forecast endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_blueprint.route('/companies/<company_id>/insights', methods=['GET'])
def company_insights(company_id: str):
    """
    Get the latest insights for a company.
    
    Args:
        company_id: The company ID
        
    Returns:
        JSON response with the insights
    """
    try:
        # Connect to the insights database
        conn = get_insights_connection()
        
        # Query for the latest insight
        result = conn.execute(
            """SELECT insight_text, generated_at 
            FROM insights_cache 
            WHERE company_name = ?
            ORDER BY generated_at DESC LIMIT 1""", 
            [company_id]
        ).fetchone()
        
        conn.close()
        
        if not result:
            # If no insight exists in the database, check for a file
            file_path = f"/app/static/insights/{company_id.replace(' ', '_').lower()}_insights.html"
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    insight_text = f.read()
                    return jsonify({
                        "company": company_id,
                        "insight": insight_text,
                        "generated_at": datetime.now().isoformat(),
                        "source": "file"
                    })
            else:
                return jsonify({
                    "error": f"No insights found for company: {company_id}"
                }), 404
        
        # Return the insight
        insight_text, generated_at = result
        
        # Remove backticks if they exist (they're used for database safety)
        if insight_text.startswith('```html') and insight_text.endswith('```'):
            insight_text = insight_text[7:-3]  # Remove ```html at the start and ``` at the end
        elif insight_text.startswith('`') and insight_text.endswith('`'):
            insight_text = insight_text[1:-1]  # Remove single backticks
        
        return jsonify({
            "company": company_id,
            "insight": insight_text,
            "generated_at": generated_at.isoformat() if isinstance(generated_at, datetime) else generated_at,
            "source": "database"
        })
    except Exception as e:
        logger.error(f"Error getting insights for {company_id}: {str(e)}")
        return jsonify({"error": str(e)}), 500


