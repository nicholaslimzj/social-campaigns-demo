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

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from app directory
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded environment variables from {env_path}")
    else:
        print(f"No .env file found at {env_path}")
except ImportError:
    print("python-dotenv not installed, skipping .env loading")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    
    logger.info(f"Found data file: {csv_file}")
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


def start_server(port=8080):
    """
    Start a simple web server.
    """
    logger.info(f"Starting web server on port {port}...")
    
    class CustomHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(BASE_DIR), **kwargs)
        
        def log_message(self, format, *args):
            logger.info(f"{self.client_address[0]} - {format % args}")
    
    server_address = ('', port)
    httpd = HTTPServer(server_address, CustomHandler)
    
    logger.info(f"Server running at http://localhost:{port}/")
    logger.info("Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped")
    
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

def main():
    """
    Main function that runs when the application starts.
    """
    logger.info("Starting Meta Demo application")
    
    # Parse command line arguments
    command = "check"  # Default command
    subcommand = None  # For vanna subcommands
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        # Check for vanna subcommands
        if command == "vanna" and len(sys.argv) > 2:
            subcommand = sys.argv[2].lower()
    
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
    else:
        logger.error(f"Unknown command: {command}")
        logger.info("Available commands: check, process, dbt, dashboard, serve, vanna")
        logger.info("Vanna subcommands: vanna train, vanna query, vanna view-training")
    
    logger.info("Meta Demo application completed")


if __name__ == "__main__":
    main()
