"""
API module for the Meta Demo project.
Provides REST endpoints for various functionalities including Vanna.ai integration.
Adapted for Vercel serverless deployment.
"""

import os
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import pandas as pd
import logging
from typing import Dict, Any, Optional
import json
from http import HTTPStatus

from app.vanna_core import initialize_vanna, VANNA_AVAILABLE, GEMINI_AVAILABLE

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
            model = os.environ.get("VANNA_MODEL", "gemini-2.5-pro-exp-03-25")
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
            return jsonify({
                "question": question,
                "sql": result['sql'],
                "results": {
                    "columns": result['results'].columns.tolist(),
                    "data": result['results'].values.tolist()
                }
            })
        
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}")
            return jsonify({
                "error": f"Failed to process question: {str(e)}"
            }), 500
    
    @app.route('/api/kpis', methods=['GET'])
    def get_kpis():
        """
        Get KPIs for a specific company or all companies.
        
        Query parameters:
        - company: Optional company name to filter KPIs
        """
        company = request.args.get('company')
        
        try:
            # This is a placeholder - in a real implementation, 
            # you would query your database for actual KPI data
            kpis = {
                "Cyber Circuit": {
                    "roi": 1.85,
                    "ctr": 0.032,
                    "conversion_rate": 0.021,
                    "acquisition_cost": 42.50,
                    "engagement_score": 3.7
                },
                "TechNova": {
                    "roi": 2.1,
                    "ctr": 0.028,
                    "conversion_rate": 0.019,
                    "acquisition_cost": 38.75,
                    "engagement_score": 3.9
                },
                "Quantum Dynamics": {
                    "roi": 1.65,
                    "ctr": 0.025,
                    "conversion_rate": 0.017,
                    "acquisition_cost": 45.20,
                    "engagement_score": 3.2
                },
                "Stellar Systems": {
                    "roi": 1.95,
                    "ctr": 0.030,
                    "conversion_rate": 0.022,
                    "acquisition_cost": 40.10,
                    "engagement_score": 3.5
                }
            }
            
            if company:
                if company in kpis:
                    return jsonify(kpis[company])
                else:
                    return jsonify({
                        "error": f"Company '{company}' not found"
                    }), 404
            
            return jsonify(kpis)
        
        except Exception as e:
            logger.error(f"Error getting KPIs: {str(e)}")
            return jsonify({
                "error": f"Failed to get KPIs: {str(e)}"
            }), 500
    
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
        
        try:
            # This is a placeholder - in a real implementation, 
            # you would generate insights based on actual data analysis
            insights = {
                "Cyber Circuit": "Email campaigns achieved higher turnfs for Cyber Circuit, while Instagram showed low engagement. An anomaly in acquisition cost was detected recently.",
                "TechNova": "TechNova's Facebook campaigns outperformed expectations with a 15% increase in ROI. Consider allocating more budget to this channel.",
                "Quantum Dynamics": "Quantum Dynamics is seeing declining engagement on Twitter. Their email campaigns continue to deliver the highest conversion rates.",
                "Stellar Systems": "Stellar Systems' recent Pinterest campaign showed promising results with a 22% increase in clicks. Their acquisition cost has decreased by 8% month-over-month."
            }
            
            if company in insights:
                # Return just the insight property as expected by the frontend
                return jsonify({
                    "insight": insights[company]
                })
            else:
                return jsonify({
                    "error": f"Insights for company '{company}' not found"
                }), 404
        
        except Exception as e:
            logger.error(f"Error getting insights: {str(e)}")
            return jsonify({
                "error": f"Failed to get insights: {str(e)}"
            }), 500
    
    @app.route('/api/companies', methods=['GET'])
    def get_companies():
        """Get list of available companies."""
        try:
            # This is a placeholder - in a real implementation, 
            # you would query your database for the actual list of companies
            companies = [
                "Cyber Circuit",
                "TechNova",
                "Quantum Dynamics",
                "Stellar Systems"
            ]
            
            return jsonify(companies)
        
        except Exception as e:
            logger.error(f"Error getting companies: {str(e)}")
            return jsonify({
                "error": f"Failed to get companies: {str(e)}"
            }), 500

# Note: The Flask app is initialized in the init_app() function
# and the handler function is now in index.py
