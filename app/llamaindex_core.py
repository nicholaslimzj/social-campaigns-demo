#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Core LlamaIndex integration for the Meta Demo project.
This module provides natural language to SQL capabilities using LlamaIndex.
"""

import os
import logging
import tempfile
import re
from pathlib import Path
import duckdb
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import sqlalchemy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path constants - same as vanna_core.py for consistency
DATA_ROOT_PATH = '/data'
DBT_MODELS_PATH = '/app/dbt/models'
DBT_MACROS_PATH = '/app/dbt/macros'
DB_PATH = '/data/db/meta_analytics.duckdb'
DBT_PROJECT_PATH = '/app/dbt'

# Check if LlamaIndex imports
try:
    from llama_index.core import Settings
    from llama_index.core.retrievers import NLSQLRetriever
    from llama_index.core.query_engine import RetrieverQueryEngine
    from llama_index.core.indices.struct_store.sql_query import SQLDatabase
    from llama_index.llms.google_genai import GoogleGenAI
    
    # Custom DuckDB database adapter for LlamaIndex
    class DuckDBDatabase(SQLDatabase):
        """DuckDB database adapter for LlamaIndex."""
        
        def __init__(self, conn: duckdb.DuckDBPyConnection, include_tables: Optional[List[str]] = None):
            """Initialize with DuckDB connection."""
            self._conn = conn
            self._include_tables = include_tables
            self._tables = self._get_tables()
            self._table_fields = {}
            for table in self._tables:
                self._table_fields[table] = self._get_table_fields(table)
        
        def _get_tables(self) -> List[str]:
            """Get all tables in the database."""
            if self._include_tables:
                return self._include_tables
            
            result = self._conn.execute("SHOW TABLES").fetchall()
            return [row[0] for row in result]
        
        def _get_table_fields(self, table: str) -> Dict[str, str]:
            """Get all fields for a table."""
            result = self._conn.execute(f"DESCRIBE {table}").fetchall()
            return {row[0]: row[1] for row in result}
        
        def get_table_columns(self, table_name: str) -> Dict[str, str]:
            """Get all columns for a table."""
            return self._table_fields.get(table_name, {})
        
        def get_tables(self) -> List[str]:
            """Get all tables in the database."""
            return self._tables
        
        def run_sql(self, query: str) -> pd.DataFrame:
            """Run a SQL query and return the results as a pandas DataFrame."""
            return self._conn.execute(query).df()
    LLAMAINDEX_AVAILABLE = True
except ImportError as e:
    logger.warning(f"LlamaIndex not installed or import error: {e}. Install with 'pip install llama-index llama-index-llms-gemini llama-index-embeddings-gemini'")
    LLAMAINDEX_AVAILABLE = False

# Check if google-generativeai is installed
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    logger.warning("Google Generative AI not installed. Install with 'pip install google-generativeai'")
    GEMINI_AVAILABLE = False

class MetaLlamaIndexCore:
    """
    Core LlamaIndex integration for the Meta Demo project.
    Provides natural language to SQL capabilities.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None):
        """
        Initialize the LlamaIndex integration.
        
        Args:
            api_key: Google API key. If None, will try to get from environment variable.
            model: Gemini model to use. If None, will try to get from environment variable.
            temperature: Temperature for the model. If None, will try to get from environment variable.
        """
        if not LLAMAINDEX_AVAILABLE:
            raise ImportError("LlamaIndex not installed. Install with 'pip install llama-index llama-index-llms-gemini llama-index-embeddings-gemini'")
        
        if not GEMINI_AVAILABLE:
            raise ImportError("Google Generative AI not installed. Install with 'pip install google-generativeai'")
        
        # Get API key from parameter or environment variable
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("Google API key not provided and not found in environment variables")
        
        # Get model name from parameter or environment variable with fallback
        self.model = model or os.environ.get("GEMINI_MODEL", "gemini-1.5-pro")
        
        # Get temperature from parameter or environment variable with fallback
        self.temperature = temperature
        if self.temperature is None:
            temp_str = os.environ.get("GEMINI_TEMPERATURE", "0.2")
            try:
                self.temperature = float(temp_str)
            except ValueError:
                logger.warning(f"Invalid temperature value '{temp_str}' in environment variables, using default 0.2")
                self.temperature = 0.2
        
        logger.info(f"Initializing LlamaIndex with model={self.model}, temperature={self.temperature}")
        
        self.db_path = DB_PATH
        
        # Create SQLAlchemy engine for DuckDB instead of direct connection
        self.engine = sqlalchemy.create_engine(f"duckdb:///{self.db_path}")
        
        # Set up LlamaIndex components
        self._setup_llamaindex()
        
        # Initialize query engine
        self.query_engine = None
        
        # Initialize NLSQLRetriever
        self.nl_sql_retriever = None
        
        # Initialize SQLDatabase
        self.sql_database = None
    
    def _setup_llamaindex(self):
        """Set up LlamaIndex components."""
        # Initialize Google Generative AI
        try:
            # Configure the model name
            model_name = self.model
            if model_name and not model_name.startswith('gemini-'):
                if model_name.startswith('models/'):
                    # Extract the model name from the models/ prefix
                    model_name = model_name.split('models/')[1]
                # Add gemini- prefix if needed
                if not model_name.startswith('gemini-'):
                    model_name = f"gemini-{model_name}"
            
            logger.info(f"Initializing GoogleGenAI with model={model_name}")
            
            # Initialize the LLM
            llm = GoogleGenAI(
                model=model_name,
                api_key=self.api_key,
                temperature=self.temperature
            )
            logger.info("Successfully initialized GoogleGenAI LLM")
            
            # No embedding model needed since we're not using vector storage
            
        except Exception as e:
            logger.warning(f"Error initializing Google Generative AI: {e}")
            
            # Fall back to HuggingFace models if Google AI fails
            try:
                from llama_index.llms.huggingface import HuggingFaceLLM
                
                logger.info("Falling back to HuggingFace models")
                
                # Initialize HuggingFace LLM
                llm = HuggingFaceLLM(model_name="google/flan-t5-small", device_map="auto")
                logger.info("Successfully initialized HuggingFaceLLM")
                
            except Exception as e2:
                logger.warning(f"Error initializing HuggingFace models: {e2}")
                
                # Final fallback to mock models for testing
                from llama_index.llms.mock import MockLLM
                
                logger.warning("All model initializations failed. Using mock models for testing only.")
                llm = MockLLM()
        
        # Set up global settings
        Settings.llm = llm
        Settings.chunk_size = 1024
    
    def _get_table_schema(self, table_name):
        """Get schema for a specific table."""
        try:
            # Query the information schema
            query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            """
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            return df
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {e}")
            return pd.DataFrame()
    
    def _get_table_sample(self, table_name, limit=5):
        """Get a sample of data from the table."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            return df
        except Exception as e:
            logger.error(f"Error getting sample data for table {table_name}: {e}")
            return pd.DataFrame()
    
    def ask(self, question: str) -> Dict[str, Any]:
        """
        Ask a natural language question and get SQL and results.
        
        Args:
            question: The natural language question to ask.
            
        Returns:
            Dictionary with question, SQL, and results.
        """
        try:
            # Preprocess the question to add table guidance based on question type
            enhanced_question = self._enhance_question_with_table_guidance(question)
            logger.info(f"Asking LlamaIndex: {enhanced_question}")
            
            # Step 1: Initialize the SQLDatabase and NLSQLRetriever if not already done
            if self.sql_database is None or self.nl_sql_retriever is None:
                # Get all tables from the database
                from sqlalchemy import inspect
                inspector = inspect(self.engine)
                tables = inspector.get_table_names()
                logger.info(f"Found {len(tables)} tables in the database")
                
                # Create a list of key tables to prioritize
                key_tables = []
                for table in tables:
                    if table in ["stg_campaigns", "campaign_monthly_metrics", "metrics_monthly_anomalies", "campaign_month_performance_rankings"]:
                        key_tables.append(table)
                
                # If we have our key tables, use only those; otherwise use all tables
                tables_to_use = key_tables if key_tables else tables
                logger.info(f"Using tables: {', '.join(tables_to_use)}")
                
                # Create SQLDatabase
                self.sql_database = SQLDatabase(self.engine, include_tables=tables_to_use)
                logger.info(f"Created SQLDatabase with tables: {', '.join(tables_to_use)}")
                
                # Disable embeddings
                Settings.embed_model = None
                
                # Create NLSQLRetriever
                try:
                    self.nl_sql_retriever = NLSQLRetriever(
                        sql_database=self.sql_database
                    )
                    logger.info("Created NLSQLRetriever")
                except Exception as e:
                    logger.error(f"Error creating NLSQLRetriever: {e}")
                    raise
            
            # Step 2: Get SQL directly from the retriever (following the LlamaIndex documentation)
            retriever_results = self.nl_sql_retriever.retrieve(enhanced_question)
            
            # Extract SQL from the retriever results
            sql = ""
            for node in retriever_results:
                if 'sql_query' in node.metadata:
                    sql = node.metadata['sql_query']
                    logger.info(f"Retrieved SQL: {sql}")
                    break
            
            # If no SQL found, use LLM to generate a direct response
            if not sql:
                logger.warning("No SQL found in the retriever results")
                
                # Use the LLM to generate a direct response with an optional joke
                # Data scope explanation to include in the prompt
                data_scope = "The database only contains campaign performance, audience segments, and marketing metrics data."
                
                response_prompt = f"""The user asked: '{question}'
                
                Generate a helpful response with these components:
                
                1. Start with a statement beginning with 'Unable to' that explains we can't answer this from the marketing data. Express a subtle hint of disappointment that we can't be more helpful.
                
                2. Include this exact explanation: "{data_scope}"
                
                3. End with a brief, subtle comment that shows you wish you could help more while adding just a touch of wit. Don't overdo the humor - it should be understated.
                
                Example responses:
                - "Unable to tell you what color the sky is based on our marketing data. {data_scope} Wish I could be more helpful with this one."
                - "Unable to determine your name from marketing data. {data_scope} I'm much better with campaign metrics than personal details, unfortunately."
                - "Unable to recommend dinner options based on our marketing data. {data_scope} I can analyze conversion rates all day, but restaurant recommendations are sadly outside my scope."
                
                Make sure the response flows naturally as a single paragraph. Balance being helpful and professional with just a touch of personality.
                
                For parsing purposes, add 'Joke:' on a separate line before your subtle comment, but I'll remove this marker later.
                """
                
                try:
                    # Get the LLM's response
                    full_response = Settings.llm.complete(response_prompt).text.strip()
                    
                    # Split the response into the main response and joke
                    response_parts = full_response.split('Joke:', 1)
                    
                    # If we have a joke, combine them naturally
                    if len(response_parts) > 1 and response_parts[1].strip():
                        main_response = response_parts[0].strip()
                        joke = response_parts[1].strip()
                        friendly_message = f"{main_response} {joke}"
                    else:
                        # If no joke or parsing failed, just use the full response
                        friendly_message = response_parts[0].strip()
                except Exception as e:
                    logger.error(f"Error generating response with LLM: {e}")
                    friendly_message = f"Unable to answer about '{question}' based on marketing data. The database only contains campaign metrics and performance data."
                
                return {
                    "question": question,
                    "sql": "",
                    "results": [],
                    "description": friendly_message
                }
            
            # Step 3: Execute the SQL query
            results_data = []
            try:
                # Use SQLAlchemy to execute the query
                with self.engine.connect() as connection:
                    results_df = pd.read_sql(sql, connection)
                    # Convert DataFrame to list of dicts
                    results_data = results_df.to_dict(orient='records')
                    logger.info(f"SQL execution returned {len(results_data)} rows")
            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                results_data = [{"error": str(e)}]
            
            # Step 4: Generate a descriptive analysis of the results using the LLM
            description = ""
            if results_data and len(results_data) > 0:
                try:
                    # Convert results to a readable format for the LLM
                    results_str = "\n".join([str(row) for row in results_data[:10]])
                    if len(results_data) > 10:
                        results_str += f"\n... and {len(results_data) - 10} more rows"
                    
                    analysis_prompt = f"""Based on the following SQL query and results, provide a clear answer to the question:
                    
                    Question: {question}
                    
                    SQL Query:
                    {sql}
                    
                    Results:
                    {results_str}
                    
                    Your response must do TWO things:
                    1. State what the data shows, using specific numbers/metrics from the results AND mentioning key parameters (like filters, time periods, or groupings).
                    2. Directly connect this to the original question, noting any limitations if the data doesn't fully answer what was asked.
                    
                    Important guidelines:
                    - Keep your response VERY CONCISE (max 2-3 sentences)
                    - Don't quote exact table or column names from the SQL - translate these to business concepts
                    - Focus on what the data means, not how it was queried
                    - Don't add recommendations or inferences beyond what the data shows
                    
                    Example good answers:
                    - "Technology is the target audience with highest ROI (4.2) for Cyber Circuit when ranked by performance."
                    - "Based on the total profit of $430M across all campaigns in the last 30 days, your marketing shows strong returns. Whether you'd be 'rich' depends on factors beyond just campaign profits."
                    - "The Summer Promotion had the highest conversion rate (8.2%) among all TechNova campaigns in Q2 2024, making it their best performer for that period."
                    """
                    
                    description = Settings.llm.complete(analysis_prompt).text
                    logger.info("Generated analysis for the results")
                except Exception as e:
                    logger.error(f"Error generating description: {e}")
                    description = "Error generating description"
            
            # Create the response object
            response = {
                "question": question,
                "sql": sql,
                "results": results_data,
                "description": description
            }
            
            # Dispose of the engine to free up the connection
            # This is important to avoid conflicts with other parts of the API
            self.engine.dispose()
            logger.info("Disposed of SQLAlchemy engine")
            
            return response
        except Exception as e:
            logger.error(f"Error in ask: {e}")
            # Make sure to dispose of the engine even in case of error
            try:
                self.engine.dispose()
                logger.info("Disposed of SQLAlchemy engine after error")
            except Exception as dispose_error:
                logger.error(f"Error disposing of engine: {dispose_error}")
                
            return {
                "question": question,
                "sql": "",
                "results": [],
                "description": f"Error: {str(e)}"
            }
    
    def _enhance_question_with_table_guidance(self, question: str) -> str:
        """
        Enhance the question with guidance about which tables to use based on the question type.
        
        Args:
            question: The original natural language question
            
        Returns:
            Enhanced question with table guidance
        """
        question_lower = question.lower()
        
        # Time-based analysis questions
        if any(term in question_lower for term in ['month', 'trend', 'over time', 'change', 'growth']):
            return f"{question} Use the campaign_monthly_metrics table for time-based analysis."
        
        # Anomaly detection questions
        elif any(term in question_lower for term in ['anomaly', 'unusual', 'outlier', 'deviation', 'abnormal']):
            return f"{question} Use the metrics_monthly_anomalies table to identify statistical anomalies."
        
        # Ranking and performance comparison questions
        elif any(term in question_lower for term in ['rank', 'ranking', 'top', 'best', 'worst', 'compare', 'comparison', 'performance']):
            return f"{question} Use the campaign_month_performance_rankings table for aggregating campaigns and their performance."
        
        # Campaign-level detailed questions
        elif any(term in question_lower for term in ['campaign', 'specific', 'individual', 'detail']):
            return f"{question} Use the stg_campaigns table for detailed campaign-level data."
        
        # Default - no specific guidance
        return question


def initialize_llamaindex(api_key: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None) -> MetaLlamaIndexCore:
    """
    Initialize the LlamaIndex integration.
    
    Args:
        api_key: Google API key. If None, will try to get from environment variable.
        model: Gemini model to use. If None, will try to get from environment variable.
        temperature: Temperature for the model. If None, will try to get from environment variable.
        
    Returns:
        Initialized LlamaIndex integration.
    """
    try:
        llamaindex = MetaLlamaIndexCore(api_key, model, temperature)
        # Query engine will be initialized on-demand when ask() is called
        return llamaindex
    except Exception as e:
        logger.error(f"Error initializing LlamaIndex: {e}")
        raise
