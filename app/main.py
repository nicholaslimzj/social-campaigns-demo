#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main entry point for the Meta Demo application.
This module serves as the entry point for the Docker container.

Commands:
    python -m app.main check      - Check data files and environment
    python -m app.main process    - Process data (CSV to parquet)
    python -m app.main duckdb     - Initialize DuckDB and create views
    python -m app.main dashboard  - Start the dashboard
    python -m app.main serve      - Start a simple web server
    python -m app.main vanna      - Start Vanna natural language to SQL CLI
"""

import os
import sys
import time
import logging
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    
    # Load from .env file in the project root
    root_env_path = Path(__file__).parent.parent / '.env'
    if root_env_path.exists():
        load_dotenv(dotenv_path=root_env_path)
        logger.info(f"Loaded environment variables from {root_env_path}")
        
    # Check for environment-specific .env files
    env = os.environ.get('ENVIRONMENT', 'development')
    env_specific_path = Path(__file__).parent.parent / f".env.{env}"
    if env_specific_path.exists():
        load_dotenv(dotenv_path=env_specific_path)
        logger.info(f"Loaded environment-specific variables from {env_specific_path}")
    
    # Log if no environment files were found
    if not (root_env_path.exists() or env_specific_path.exists()):
        logger.warning("No .env files found in project root")
except ImportError:
    print("python-dotenv not installed, skipping .env loading")

# Define paths
# Check if we're running in a container
BASE_DIR = Path('/')
DATA_DIR = Path('/dataset')
DATA_ROOT = Path('/data')
PROCESSED_DIR = DATA_ROOT / "processed"


def check_environment():
    """
    Check if the data files and environment are properly set up.
    """
    logger.info("Checking environment...")
    
    # Check if the data directory exists
    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        return False
    
    # Check if the CSV file exists
    csv_file = DATA_DIR / "Social_Media_Advertising.csv"
    if not csv_file.exists():
        logger.error(f"CSV file not found: {csv_file}")
        return False
    
    # Check if data directories exist
    for dir_path in [DATA_ROOT, DATA_ROOT / "raw", PROCESSED_DIR, DATA_ROOT / "db"]:
        if not dir_path.exists():
            logger.warning(f"Directory not found: {dir_path}")
            logger.info(f"Creating directory: {dir_path}")
            dir_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Environment check completed successfully")
    return True


def process_data():
    """
    Process the data (CSV to parquet).
    """
    logger.info("Starting data processing...")
    
    if not check_environment():
        return False
    
    # Import and run data processing
    from app.scripts.data_processor import process_data as run_processor
    result = run_processor()
    
    logger.info("Data processing completed")
    return result


def start_dashboard():
    """
    Start the dashboard application.
    """
    logger.info("Starting dashboard...")
    
    if not check_environment():
        return False
    
    try:
        # Import and run dashboard
        from app.dashboard.app import run_dashboard
        run_dashboard()
        return True
    except ImportError:
        logger.error("Dashboard module not found or not implemented yet")
        return False


def start_server(args=None):
    """
    Start the API server for the Meta Demo project.
    
    Args:
        args: Command line arguments (optional)
    """
    from app.api import init_app
    
    # Use the standard Flask port 5000 for Docker
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Using port {port} for the API server")
    
    logger.info(f"Starting Meta Demo API server on port {port}...")
    
    # Check if API key is available in environment
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        logger.warning("No Google API key found in environment. Vanna functionality will be limited.")
    
    # Log Vanna configuration from environment
    model = os.environ.get("VANNA_MODEL", "gemini-2.5-pro-preview-03-25")
    temp_str = os.environ.get("VANNA_TEMPERATURE", "0.2")
    logger.info(f"Vanna configuration: model={model}, temperature={temp_str}")
    
    # Initialize the Flask app (it will read environment variables itself)
    app = init_app()
    
    logger.info(f"API server running at http://localhost:{port}/")
    logger.info("Available endpoints:")
    logger.info("  /api/health - Health check")
    logger.info("  /api/ask - Process natural language questions")
    logger.info("  /api/kpis - Get KPIs for companies")
    logger.info("  /api/insights - Get AI-generated insights")
    logger.info("  /api/companies - Get list of companies")
    logger.info("Press Ctrl+C to stop the server")
    
    try:
        # Simple configuration - bind to all interfaces (0.0.0.0)
        # This works for both local development and Docker
        host = '0.0.0.0'
        logger.info(f"Starting Flask server with host='{host}', accessible from all interfaces")
        logger.info(f"You should be able to access the API at http://localhost:{port}/api/health")
            
        app.run(host=host, port=port)
    except KeyboardInterrupt:
        logger.info("Server stopped")
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
    
    return True


def run_dbt_models():
    """
    Run dbt models to create views and tables in DuckDB.
    """
    logger.info("Running dbt models...")
    
    if not check_environment():
        return False
    
    try:
        # Run dbt from the command line
        import subprocess
        
        # Change to the dbt directory
        dbt_dir = Path('/app/dbt')
        
        # Run dbt
        result = subprocess.run(
            ['dbt', 'run'],
            cwd=str(dbt_dir),
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("dbt models executed successfully")
            logger.info(result.stdout)
            return True
        else:
            logger.error("dbt execution failed")
            logger.error(result.stderr)
            return False
    except Exception as e:
        logger.error(f"Error running dbt models: {e}")
        return False


def start_vanna(command=None):
    """
    Start the Vanna natural language to SQL CLI.
    
    Args:
        command: Optional command to run (train, query, view-training)
    """
    logger.info(f"Starting Vanna natural language to SQL CLI with command: {command or 'interactive'}...")
    
    if not check_environment():
        return False
    
    try:
        # Import the vanna_cli module
        import sys
        
        # Save original sys.argv
        original_argv = sys.argv.copy()
        
        # Set sys.argv to just the script name for the CLI
        # This prevents passing the 'vanna' argument to the CLI
        sys.argv = [sys.argv[0]]
        
        # Add the appropriate command and flags
        if command == 'train':
            logger.info("Running Vanna training...")
            sys.argv.append('train')
        elif command == 'view-training':
            logger.info("Viewing Vanna training data...")
            sys.argv.append('view-training')
        elif command == 'query':
            logger.info("Starting Vanna query mode...")
            sys.argv.append('query')
            sys.argv.append('--interactive')
        else:
            # Default to interactive query mode
            logger.info("Starting Vanna interactive query mode...")
            sys.argv.append('query')
            sys.argv.append('--interactive')
        
        try:
            # Import and run Vanna CLI
            from app.vanna_cli import main as run_vanna_cli
            result = run_vanna_cli()
            success = result == 0
            
            if success:
                logger.info(f"Vanna command '{command or 'interactive'}' completed successfully")
            else:
                logger.error(f"Vanna command '{command or 'interactive'}' failed")
                
        finally:
            # Restore original sys.argv
            sys.argv = original_argv
            
        return success
    except ImportError:
        logger.error("Vanna module not found. Install with 'pip install vanna'")
        return False

def generate_dbt_metadata(model_type=None, model_name=None, skip_existing=False, vanna_json=False):
    """
    Generate metadata for dbt models using LLM.
    
    Args:
        model_type: Type of models to process ("all", "staging", or "marts")
        model_name: Name of a specific model to process (optional)
        skip_existing: If True, skip models that already have YAML files
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Generating metadata for dbt models: type={model_type or 'all'}, model={model_name or 'all'}")
    
    if not check_environment():
        return False
    
    try:
        # Import the metadata generator module
        from app.scripts.metadata_generator import generate_metadata
        
        # Get API key from environment
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("No API key found in environment. Set OPENAI_API_KEY or GOOGLE_API_KEY.")
            return False
        
        # Set default model type if not specified
        if not model_type:
            model_type = "all"
        
        # Get model name from environment
        model = os.environ.get("VANNA_MODEL", "gemini-2.5-pro-preview-03-25")
        
        # Generate metadata
        result = generate_metadata(model_type=model_type, model_name=model_name, llm_model=model, skip_existing=skip_existing, vanna_json=vanna_json)
        
        if result:
            logger.info("Metadata generation completed successfully")
        else:
            logger.error("Metadata generation failed")
        
        return result
    except ImportError as e:
        logger.error(f"Error importing metadata generator: {e}")
        logger.error("Make sure required packages are installed: pip install langchain-core langchain-google-genai pydantic pyyaml")
        return False
    except Exception as e:
        logger.error(f"Error generating metadata: {e}")
        return False

def main():
    """
    Main function that runs when the application starts.
    """
    logger.info("Starting Meta Demo application")
    
    # Parse command line arguments
    command = "check"  # Default command
    subcommand = None  # For vanna subcommands
    model_type = None  # For metadata subcommands
    model_name = None  # For metadata specific model
    skip_existing = False  # For metadata skip existing flag
    vanna_json = False  # For Vanna JSON export
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        # Check for vanna subcommands
        if command == "vanna" and len(sys.argv) > 2:
            subcommand = sys.argv[2].lower()
        
        # Check for metadata subcommands
        if command == "metadata" and len(sys.argv) > 2:
            # Check for flags first
            args = sys.argv[2:]
            if "--skip-existing" in args:
                skip_existing = True
                args.remove("--skip-existing")
            if "--vanna-json" in args:
                vanna_json = True
                args.remove("--vanna-json")
            
            # Now process the remaining arguments
            if args:
                # Check if it's a model type or specific model
                arg = args[0].lower()
                if arg in ["all", "staging", "marts"]:
                    model_type = arg
                    # Check if there's also a model name
                    if len(args) > 1:
                        model_name = args[1]
                else:
                    # Assume it's a model name
                    model_name = arg
    
    # Execute the requested command
    if command == "check":
        check_environment()
    elif command == "process":
        process_data()
    elif command == "dbt":
        run_dbt_models()
    elif command == "dashboard":
        start_dashboard()
    elif command == "serve":
        # Call start_server without arguments
        start_server()
    elif command == "vanna":
        if subcommand == "train":
            start_vanna(command="train")
        elif subcommand == "view-training":
            start_vanna(command="view-training")
        elif subcommand == "query":
            start_vanna(command="query")
        else:
            # Default to interactive mode
            start_vanna()
    elif command == "metadata":
        generate_dbt_metadata(model_type=model_type, model_name=model_name, skip_existing=skip_existing, vanna_json=vanna_json)
    else:
        logger.error(f"Unknown command: {command}")
        logger.info("Available commands: check, process, dbt, dashboard, serve, vanna, metadata")
        logger.info("Vanna subcommands: vanna train, vanna query, vanna view-training")
        logger.info("Metadata subcommands: metadata [--skip-existing] [--vanna-json] [all|staging|marts] [model_name]")
    
    logger.info("Meta Demo application completed")


if __name__ == "__main__":
    main()
