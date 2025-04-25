#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Command-line interface for LlamaIndex text-to-SQL functionality.
This module provides a CLI for the LlamaIndex text-to-SQL integration.
"""

import os
import sys
import logging
import argparse
from typing import Optional, List, Dict, Any, Union

# Configure logging
logger = logging.getLogger(__name__)

def setup_parser() -> argparse.ArgumentParser:
    """Set up command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="LlamaIndex text-to-SQL command-line interface"
    )
    
    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Train command
    train_parser = subparsers.add_parser("train", help="Train LlamaIndex on dbt models")
    train_parser.add_argument(
        "--api-key", 
        dest="api_key",
        help="Google API key (if not provided, will use GOOGLE_API_KEY environment variable)"
    )
    train_parser.add_argument(
        "--model", 
        help="Gemini model name (default: gemini-1.5-pro)",
        default="gemini-1.5-pro"
    )
    train_parser.add_argument(
        "--temperature", 
        type=float,
        help="Temperature for the model (default: 0.2)",
        default=0.2
    )
    
    # Ask command
    ask_parser = subparsers.add_parser("ask", help="Ask a question")
    ask_parser.add_argument(
        "question",
        help="Natural language question about the data"
    )
    ask_parser.add_argument(
        "--company",
        help="Optional company filter"
    )
    ask_parser.add_argument(
        "--api-key", 
        dest="api_key",
        help="Google API key (if not provided, will use GOOGLE_API_KEY environment variable)"
    )
    ask_parser.add_argument(
        "--model", 
        help="Gemini model name (default: gemini-1.5-pro)",
        default="gemini-1.5-pro"
    )
    ask_parser.add_argument(
        "--temperature", 
        type=float,
        help="Temperature for the model (default: 0.2)",
        default=0.2
    )
    ask_parser.add_argument(
        "--backend",
        choices=["llamaindex", "vanna"],
        help="Text-to-SQL backend to use (default: llamaindex)",
        default="llamaindex"
    )
    
    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare LlamaIndex and Vanna on the same question")
    compare_parser.add_argument(
        "question",
        help="Natural language question about the data"
    )
    compare_parser.add_argument(
        "--company",
        help="Optional company filter"
    )
    compare_parser.add_argument(
        "--api-key", 
        dest="api_key",
        help="Google API key (if not provided, will use GOOGLE_API_KEY environment variable)"
    )
    compare_parser.add_argument(
        "--model", 
        help="Gemini model name (default: gemini-1.5-pro)",
        default="gemini-1.5-pro"
    )
    compare_parser.add_argument(
        "--temperature", 
        type=float,
        help="Temperature for the model (default: 0.2)",
        default=0.2
    )
    
    return parser

def train_llamaindex(api_key: Optional[str] = None, model: str = "gemini-1.5-pro", temperature: float = 0.2) -> None:
    """Train LlamaIndex on dbt models."""
    from app.llamaindex_core import initialize_llamaindex
    
    # Initialize LlamaIndex
    llamaindex_instance = initialize_llamaindex(
        api_key=api_key,
        model=model,
        temperature=temperature,
        train=True
    )
    
    # Print training data
    training_data = llamaindex_instance.get_training_data()
    print(f"\nTraining completed with {len(training_data)} tables indexed:")
    for item in training_data:
        print(f"- {item['table']}")

def ask_question(
    question: str, 
    company: Optional[str] = None,
    api_key: Optional[str] = None, 
    model: str = "gemini-1.5-pro", 
    temperature: float = 0.2,
    backend: str = "llamaindex"
) -> Dict[str, Any]:
    """Ask a question using the text-to-SQL adapter."""
    from app.text_to_sql_adapter import initialize_text_to_sql
    
    # Initialize text-to-SQL adapter
    text_to_sql_instance = initialize_text_to_sql(
        backend=backend,
        api_key=api_key,
        model=model,
        temperature=temperature
    )
    
    # Modify question if company filter is provided
    if company:
        question = f"{question} for {company}"
    
    # Ask the question
    result = text_to_sql_instance.ask(question)
    
    return result

def compare_backends(
    question: str, 
    company: Optional[str] = None,
    api_key: Optional[str] = None, 
    model: str = "gemini-1.5-pro", 
    temperature: float = 0.2
) -> Dict[str, Dict[str, Any]]:
    """Compare LlamaIndex and Vanna on the same question."""
    from app.text_to_sql_adapter import initialize_text_to_sql
    
    # Modify question if company filter is provided
    if company:
        question = f"{question} for {company}"
    
    results = {}
    
    # Test with LlamaIndex
    try:
        print("Testing with LlamaIndex...")
        llamaindex_instance = initialize_text_to_sql(
            backend="llamaindex",
            api_key=api_key,
            model=model,
            temperature=temperature,
            train=True
        )
        llamaindex_result = llamaindex_instance.ask(question)
        results["llamaindex"] = llamaindex_result
    except Exception as e:
        print(f"Error with LlamaIndex: {e}")
        results["llamaindex"] = {"error": str(e)}
    
    # Test with Vanna
    try:
        print("Testing with Vanna...")
        vanna_instance = initialize_text_to_sql(
            backend="vanna",
            api_key=api_key,
            model=model,
            temperature=temperature,
            train=True
        )
        vanna_result = vanna_instance.ask(question)
        results["vanna"] = vanna_result
    except Exception as e:
        print(f"Error with Vanna: {e}")
        results["vanna"] = {"error": str(e)}
    
    return results

def format_result(result: Dict[str, Any]) -> str:
    """Format a result for display."""
    output = []
    
    # Add question
    if "question" in result:
        output.append(f"Question: {result['question']}")
    
    # Add SQL
    if "sql" in result:
        output.append(f"\nSQL:\n{result['sql']}")
    
    # Add error if present
    if "error" in result:
        output.append(f"\nError: {result['error']}")
        return "\n".join(output)
    
    # Add results
    if "results" in result and result["results"]:
        output.append("\nResults:")
        
        # Get the first row to determine columns
        first_row = result["results"][0]
        columns = list(first_row.keys())
        
        # Create a simple table
        # Header
        header = " | ".join(columns)
        separator = "-" * len(header)
        output.append(header)
        output.append(separator)
        
        # Rows
        for row in result["results"][:10]:  # Limit to 10 rows for display
            row_values = [str(row.get(col, "")) for col in columns]
            output.append(" | ".join(row_values))
        
        # Show row count if more than 10
        if len(result["results"]) > 10:
            output.append(f"\n... and {len(result['results']) - 10} more rows")
    else:
        output.append("\nNo results returned")
    
    # Add description if present
    if "description" in result and result["description"]:
        output.append(f"\nAnalysis:\n{result['description']}")
    
    return "\n".join(output)

def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point.
    
    Args:
        argv: Command line arguments (optional)
        
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    parser = setup_parser()
    args = parser.parse_args(argv)
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Get API key from args or environment
    api_key = args.api_key or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        logger.error("No Google API key provided. Please provide it with --api-key or set the GOOGLE_API_KEY environment variable.")
        return 1
    
    try:
        if args.command == "train":
            train_llamaindex(
                api_key=api_key,
                model=args.model,
                temperature=args.temperature
            )
        
        elif args.command == "ask":
            result = ask_question(
                question=args.question,
                company=args.company,
                api_key=api_key,
                model=args.model,
                temperature=args.temperature,
                backend=args.backend
            )
            print(format_result(result))
        
        elif args.command == "compare":
            results = compare_backends(
                question=args.question,
                company=args.company,
                api_key=api_key,
                model=args.model,
                temperature=args.temperature
            )
            
            print("\n" + "=" * 40)
            print("LlamaIndex Result:")
            print("=" * 40)
            print(format_result(results["llamaindex"]))
            
            print("\n" + "=" * 40)
            print("Vanna Result:")
            print("=" * 40)
            print(format_result(results["vanna"]))
            
        elif args.command == "view-training":
            # Show training data
            from app.llamaindex_core import initialize_llamaindex
            llamaindex_instance = initialize_llamaindex(
                api_key=api_key,
                model=args.model,
                temperature=args.temperature
            )
            training_data = llamaindex_instance.get_training_data()
            print(f"\nTraining data ({len(training_data)} tables indexed):")
            for item in training_data:
                print(f"- {item['table']}")
        
        return 0  # Success
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1  # Failure

if __name__ == "__main__":
    sys.exit(main())
