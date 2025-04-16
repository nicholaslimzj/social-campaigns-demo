import os
import sys
import json
from http import HTTPStatus
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables before importing any app modules
try:
    from dotenv import load_dotenv
    
    # Always look for .env files in the project root (parent of app/)
    project_root = Path(__file__).parent
    root_env_path = project_root / '.env'
    if root_env_path.exists():
        load_dotenv(dotenv_path=root_env_path)
        logger.info(f"Loaded environment variables from {root_env_path}")
    
    # Check for environment-specific .env files
    env = os.environ.get('ENVIRONMENT', 'development')
    env_specific_path = project_root / f".env.{env}"
    if env_specific_path.exists():
        load_dotenv(dotenv_path=env_specific_path)
        logger.info(f"Loaded environment-specific variables from {env_specific_path}")
    
    # Log if no environment files were found
    if not (root_env_path.exists() or env_specific_path.exists()):
        logger.warning("No .env files found in project root, using Vercel environment variables")
    
    # Log key environment variables (excluding sensitive values)
    logger.info(f"Environment: {env}")
    logger.info(f"VANNA_MODEL: {os.environ.get('VANNA_MODEL', 'not set')}")
    logger.info(f"VANNA_TEMPERATURE: {os.environ.get('VANNA_TEMPERATURE', 'not set')}")
    logger.info(f"API key present: {bool(os.environ.get('GOOGLE_API_KEY'))}")
    
except ImportError:
    logger.warning("Could not import dotenv. Environment variables must be set manually.")

# Check if we should run in standby mode (no web server)
STANDBY_MODE = os.environ.get('STANDBY_MODE', '').lower() in ('true', '1', 'yes')

# Only import Flask app if not in standby mode
app = None
if not STANDBY_MODE:
    # Import the API handler after environment variables are loaded
    from app.api import init_app
    
    # Initialize the Flask app
    app = init_app()
    logger.info("Flask application initialized and ready to serve requests")
else:
    logger.info("Running in STANDBY_MODE - web server not started")

# This file serves as the entry point for Vercel serverless functions
def vercel_handler(request):
    """
    Entry point for Vercel serverless functions.
    This function is called by Vercel when a request is made to the API.
    """
    if STANDBY_MODE:
        return {
            'statusCode': HTTPStatus.SERVICE_UNAVAILABLE,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': "Server is running in standby mode. Web server is not available."})
        }
        
    # Get the path and method from the request
    path = request.get('path', '')
    http_method = request.get('method', 'GET').upper()
    
    # Get query parameters and body
    query_params = request.get('query', {})
    body = request.get('body', '{}')
    
    # Create a Flask request context
    with app.test_request_context(
        path=path,
        method=http_method,
        query_string=query_params,
        data=body,
        headers=request.get('headers', {})
    ):
        # Process the request using Flask
        try:
            # Dispatch the request to Flask
            flask_response = app.full_dispatch_request()
            
            # Extract response data
            status_code = flask_response.status_code
            headers = dict(flask_response.headers)
            body = flask_response.get_data(as_text=True)
            
            # Return the response in the format expected by Vercel
            return {
                'statusCode': status_code,
                'headers': headers,
                'body': body
            }
        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': f"Internal server error: {str(e)}"})
            }
