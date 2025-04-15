#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Simple CLI for testing Vanna integration in the Meta Demo project.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import pandas as pd
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from app.vanna_core import initialize_vanna, VANNA_AVAILABLE, GEMINI_AVAILABLE
except ImportError:
    # Try relative import if running directly
    sys.path.append(str(Path(__file__).parent.parent))
    try:
        from app.vanna_core import initialize_vanna, VANNA_AVAILABLE, GEMINI_AVAILABLE
    except ImportError:
        logger.error("Could not import vanna_core module")
        VANNA_AVAILABLE = False
        GEMINI_AVAILABLE = False

def get_vanna_config():
    """Get Vanna configuration from environment variables or user input."""
    # Get API key
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("Google API key not found in environment variables.")
        api_key = input("Please enter your Google API key: ")
        if api_key:
            os.environ["GOOGLE_API_KEY"] = api_key
    
    # Get model name
    model = os.environ.get("VANNA_MODEL")
    if not model:
        print("Using default model: gemini-2.5-pro-exp-03-25")
        model = "gemini-2.5-pro-exp-03-25"
    
    # Get temperature
    temperature = None
    temp_str = os.environ.get("VANNA_TEMPERATURE")
    if temp_str:
        try:
            temperature = float(temp_str)
        except ValueError:
            print(f"Invalid temperature value '{temp_str}' in environment variables, using default 0.2")
            temperature = 0.2
    else:
        temperature = 0.2
        print(f"Using default temperature: {temperature}")
    
    return {
        "api_key": api_key,
        "model": model,
        "temperature": temperature
    }

def print_results(results_df):
    """Print results in a formatted way."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_rows', 20)
    print("\nResults:")
    print("=" * 80)
    print(results_df)
    print("=" * 80)

def interactive_mode(vanna_instance):
    """Run an interactive session with Vanna."""
    print("\nWelcome to the Meta Demo Vanna CLI!")
    print("Ask questions about your social media advertising data in natural language.")
    print("Type 'exit', 'quit', or 'q' to exit.")
    print("\n")
    
    while True:
        question = input("\nEnter your question: ")
        
        if question.lower() in ['exit', 'quit', 'q']:
            print("Goodbye!")
            break
        
        try:
            result = vanna_instance.ask(question)
            
            if 'error' in result:
                print(f"\nError: {result['error']}")
                continue
            
            print(f"\nGenerated SQL:")
            print("-" * 80)
            print(result['sql'])
            print("-" * 80)
            
            print_results(result['results'])
            
        except Exception as e:
            print(f"Error: {str(e)}")

def view_training_data(vanna_instance):
    """View the training data in Vanna."""
    print("\nViewing Vanna training data...")
    
    # Get training data (returns DataFrame or None)
    import pandas as pd
    training_data = vanna_instance.get_training_data()
    
    # Handle None case
    if training_data is None:
        print("No training data found.")
        return
    
    # Handle empty DataFrame case
    if isinstance(training_data, pd.DataFrame) and training_data.empty:
        print("No training data found (empty DataFrame).")
        return
    
    # Handle DataFrame case
    if isinstance(training_data, pd.DataFrame):
        print(f"\nFound training data (DataFrame with {len(training_data)} rows):")
        print("-" * 80)
        
        # Process each row individually for better formatting
        for i, row in training_data.iterrows():
            print(f"Item {i+1}:")
            
            # Display ID
            if 'id' in row and not pd.isna(row['id']):
                print(f"  ID: {row['id']}")
            
            # Display training data type
            if 'training_data_type' in row and not pd.isna(row['training_data_type']):
                print(f"  Type: {row['training_data_type']}")
                
            # Display full content
            if 'content' in row and not pd.isna(row['content']):
                content = str(row['content'])
                print(f"  Content: {content}")
                
            # Display question if present
            if 'question' in row and not pd.isna(row['question']):
                print(f"  Question: {row['question']}")
                
            print("-" * 80)
            
            # Limit to 10 items for readability
            if i >= 9 and len(training_data) > 10:
                print(f"... and {len(training_data) - 10} more items")
                break
                
        return
    
    # Handle unexpected case
    print(f"\nUnexpected training data type: {type(training_data)}")
    print(training_data)

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Meta Demo Vanna CLI")
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Train command
    train_parser = subparsers.add_parser('train', help='Train Vanna on dbt models')
    train_parser.add_argument("--api-key", help="Google API key")
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Query Vanna with natural language')
    query_parser.add_argument("--question", "-q", help="Question to ask")
    query_parser.add_argument("--api-key", help="Google API key")
    query_parser.add_argument("--interactive", "-i", action="store_true", help="Run in interactive mode")
    
    # View training data command
    view_parser = subparsers.add_parser('view-training', help='View Vanna training data')
    view_parser.add_argument("--api-key", help="Google API key")
    
    args = parser.parse_args()
    
    if not VANNA_AVAILABLE:
        print("Vanna not installed. Install with 'pip install vanna'")
        return 1
        
    if not GEMINI_AVAILABLE:
        print("Google Generative AI not installed. Install with 'pip install google-generativeai'")
        return 1
    
    # Get Vanna configuration from environment variables or command-line arguments
    config = get_vanna_config()
    if args.api_key:
        config["api_key"] = args.api_key
    
    if not config["api_key"]:
        print("No API key provided. Exiting.")
        return 1
    
    try:
        # Handle different commands
        if args.command == 'train':
            print(f"Initializing Vanna with model={config['model']}, temperature={config['temperature']} and training on dbt models...")
            vanna_instance = initialize_vanna(
                api_key=config["api_key"], 
                model=config["model"], 
                temperature=config["temperature"], 
                train=True
            )
            print("Vanna initialized and training completed successfully!")
            return 0
            
        # Initialize Vanna for all other commands
        print(f"Initializing Vanna with model={config['model']}, temperature={config['temperature']}...")
        vanna_instance = initialize_vanna(
            api_key=config["api_key"], 
            model=config["model"], 
            temperature=config["temperature"], 
            train=False
        )
        print("Vanna initialized successfully!")
        
        if args.command == 'view-training':
            view_training_data(vanna_instance)
            return 0
            
        elif args.command == 'query':
            if args.interactive:
                interactive_mode(vanna_instance)
                return 0
            elif args.question:
                result = vanna_instance.ask(args.question)
                
                if 'error' in result:
                    print(f"Error: {result['error']}")
                    return 1
                
                print(f"Generated SQL:")
                print("-" * 80)
                print(result['sql'])
                print("-" * 80)
                
                print_results(result['results'])
                return 0
            else:
                print("No question provided. Use --question or --interactive")
                return 1
        else:
            # Default behavior for backward compatibility
            print("No command specified. Starting interactive mode...")
            interactive_mode(vanna_instance)
            return 0
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
