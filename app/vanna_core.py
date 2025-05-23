#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Core Vanna integration for the Meta Demo project.
This module provides natural language to SQL capabilities using Vanna.ai.
"""

import os
import logging
from pathlib import Path
import duckdb
from typing import List, Dict, Any, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_ROOT_PATH = '/data'
DBT_MODELS_PATH = '/app/dbt/models'
DBT_MACROS_PATH = '/app/dbt/macros'
DB_PATH = '/data/db/meta_analytics.duckdb'
DBT_PROJECT_PATH = '/app/dbt'  # Added for vanna_json path resolution

try:
    import vanna
    VANNA_AVAILABLE = True
except ImportError:
    logger.warning("Vanna not installed. Install with 'pip install vanna'")
    VANNA_AVAILABLE = False

# Check if google-generativeai is installed
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    logger.warning("Google Generative AI not installed. Install with 'pip install google-generativeai'")
    GEMINI_AVAILABLE = False

class MetaVannaCore:
    """
    Core Vanna integration for the Meta Demo project.
    Provides natural language to SQL capabilities.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None):
        """
        Initialize the Vanna integration.
        
        Args:
            api_key: Google API key. If None, will try to get from environment variable.
            model: Gemini model to use. If None, will try to get from environment variable.
            temperature: Temperature for the model. If None, will try to get from environment variable.
        """
        if not VANNA_AVAILABLE:
            raise ImportError("Vanna not installed. Install with 'pip install vanna'")
        
        if not GEMINI_AVAILABLE:
            raise ImportError("Google Generative AI not installed. Install with 'pip install google-generativeai'")
        
        # Get API key from parameter or environment variable
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("Google API key not provided and not found in environment variables")
        
        # Get model name from parameter or environment variable with fallback
        self.model = model or os.environ.get("VANNA_MODEL", "gemini-2.5-pro-preview-03-25")
        
        # Get temperature from parameter or environment variable with fallback
        self.temperature = temperature
        if self.temperature is None:
            temp_str = os.environ.get("VANNA_TEMPERATURE", "0.2")
            try:
                self.temperature = float(temp_str)
            except ValueError:
                logger.warning(f"Invalid temperature value '{temp_str}' in environment variables, using default 0.2")
                self.temperature = 0.2
        
        logger.info(f"Initializing Vanna with model={self.model}, temperature={self.temperature}")
        
        self.db_path = DB_PATH
        
        # Import specific Vanna components
        from vanna.chromadb import ChromaDB_VectorStore
        from vanna.google import GoogleGeminiChat
        import os
        
        # Create a dedicated directory for ChromaDB in the data folder
        chroma_path = os.path.join(DATA_ROOT_PATH, 'vanna_db')
        os.makedirs(chroma_path, exist_ok=True)
        logger.info(f"Using ChromaDB path: {chroma_path}")
        
        # Function to preload the embedding model
        def preload_embedding_model():
            try:
                logger.info("Preloading Chroma embedding model...")
                from chromadb.utils import embedding_functions
                
                # Path to the pre-unpacked model directory
                model_cache_dir = os.path.join(DATA_ROOT_PATH, 'chroma_models')
                model_name = "all-MiniLM-L6-v2"
                
                # Check if we have the pre-unpacked model directory
                pre_unpacked_path = os.path.join(model_cache_dir, 'models--sentence-transformers--all-MiniLM-L6-v2')
                if os.path.exists(pre_unpacked_path):
                    logger.info(f"Using pre-unpacked model at: {pre_unpacked_path}")
                    # Use the pre-unpacked model by setting the cache_folder to its parent directory
                    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                        model_name=model_name,
                        cache_folder=model_cache_dir
                    )
                else:
                    logger.info(f"Pre-unpacked model not found, downloading to: {model_cache_dir}")
                    # Download the model if the pre-unpacked version isn't available
                    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                        model_name=model_name,
                        cache_folder=model_cache_dir
                    )
                
                # Test the embedding function to ensure it's loaded
                _ = sentence_transformer_ef(["Test sentence to ensure model is loaded"])
                logger.info("Chroma embedding model successfully preloaded")
                return sentence_transformer_ef
            except Exception as e:
                logger.warning(f"Failed to preload embedding model: {e}")
                return None
        
        # Preload the embedding model
        embedding_function = preload_embedding_model()
        
        # Initialize Vanna with ChromaDB and GoogleGeminiChat
        class MyVanna(ChromaDB_VectorStore, GoogleGeminiChat):
            def __init__(self, config=None):
                ChromaDB_VectorStore.__init__(self, config=config)
                GoogleGeminiChat.__init__(self, config=config)
            
            # Patch the str_to_approx_token_count method to handle None values
            def str_to_approx_token_count(self, string):
                if string is None:
                    logger.warning("str_to_approx_token_count received None instead of a string")
                    return 0
                return super().str_to_approx_token_count(string)
                
        # Configure Vanna with the preloaded embedding function if available
        config = {
            'api_key': self.api_key, 
            'model': self.model,
            'temperature': self.temperature,
            'path': chroma_path  # Set ChromaDB path to data folder
        }
        
        # Add the embedding function to the config if it was successfully preloaded
        if embedding_function:
            config['embedding_function'] = embedding_function
        
        self.vn = MyVanna(config=config)
        
        # Connect to DuckDB for direct queries
        self.conn = duckdb.connect(self.db_path)
        
        # Define a function that takes a SQL query and returns a pandas DataFrame
        def run_sql(sql: str):
            return self.conn.query(sql).to_df()
        
        # Assign the function to vn.run_sql
        self.vn.run_sql = run_sql
        self.vn.run_sql_is_set = True
        self.vn.dialect = "DuckDB SQL"
        
        # Set DATA_ROOT macro
        self.conn.execute(f"CREATE OR REPLACE MACRO DATA_ROOT() AS '{DATA_ROOT_PATH}'")
        
        logger.info(f"Vanna initialized with DuckDB at {self.db_path}")
    
    def train_on_dbt_models(self):
        """
        Train Vanna on the dbt models.
        This will help Vanna understand the data structure and generate better SQL.
        """
        # Configure ChromaDB logging to reduce warnings
        import logging
        logging.getLogger('chromadb').setLevel(logging.ERROR)
        
        # Always clear existing training data before training
        try:
            logger.info("Clearing existing training data...")
            # Get existing training data (returns DataFrame or None)
            import pandas as pd
            training_data = self.vn.get_training_data()
            
            # Handle DataFrame case
            if isinstance(training_data, pd.DataFrame) and not training_data.empty:
                logger.info(f"Found {len(training_data)} existing training items")
                
                # Check if DataFrame has an 'id' column
                if 'id' in training_data.columns:
                    # Get list of IDs to remove
                    ids_to_remove = training_data['id'].tolist()
                    removed_count = 0
                    
                    # Remove each ID
                    for item_id in ids_to_remove:
                        try:
                            self.vn.remove_training_data(id=item_id)
                            removed_count += 1
                        except Exception as e:
                            logger.warning(f"Could not remove training item {item_id}: {e}")
                    
                    logger.info(f"Removed {removed_count} of {len(ids_to_remove)} training items")
                else:
                    logger.warning("Training data DataFrame does not contain 'id' column, cannot remove items")
            else:
                logger.info("No existing training data to clear")
        except Exception as e:
            logger.warning(f"Could not clear existing training data: {e}")
        
        logger.info("Training Vanna on dbt models (using Vanna JSON files)...")

        import os
        import json

        vanna_json_root = os.path.join(DBT_PROJECT_PATH, "vanna_json")
        all_models = []
        for root, dirs, files in os.walk(vanna_json_root):
            for file in files:
                if file.endswith('.json'):
                    with open(os.path.join(root, file), 'r') as f:
                        model = json.load(f)
                        all_models.append(model)

        if not all_models:
            logger.warning(f"No Vanna JSON files found in {vanna_json_root}. Nothing to train.")
            return

        # 1. Train on information schema
        try:
            logger.info("Training on information schema...")
            df_information_schema = self.vn.run_sql("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
            plan = self.vn.get_training_plan_generic(df_information_schema)
            self.vn.train(plan=plan)
            logger.info("Information schema training completed")
        except Exception as e:
            logger.warning(f"Could not train on information schema: {e}")

        # 2. Train on each Vanna JSON file individually, using only table name and description
        # trained_count = 0
        # for root, dirs, files in os.walk(vanna_json_root):
        #     for file in files:
        #         if file.endswith('.json'):
        #             json_path = os.path.join(root, file)
        #             try:
        #                 import json
        #                 with open(json_path, 'r') as f:
        #                     json_data = json.load(f)
                            
        #                 # Extract only the table name and description
        #                 if 'name' in json_data and 'description' in json_data:
        #                     table_name = json_data['name']
        #                     description = json_data['description']
                            
        #                     # Create a simplified training string
        #                     simplified_doc = f"The table {table_name} has this description: {description}"
                            
        #                     # Train Vanna with just the simplified information
        #                     self.vn.train(documentation=simplified_doc)
        #                     logger.info(f"Trained on simplified Vanna JSON documentation: {json_path}")
        #                     trained_count += 1
        #                 else:
        #                     logger.warning(f"JSON file {json_path} missing required name or description fields")
        #             except Exception as e:
        #                 logger.warning(f"Could not train on Vanna JSON file {json_path}: {e}")
        # logger.info(f"Vanna training completed ({trained_count} JSON files trained)")
    
    def ask(self, question: str) -> Dict[str, Any]:
        """
        Ask a natural language question and get SQL and results.
        
        Args:
            question: Natural language question about the data
            
        Returns:
            Dictionary with SQL query, results, and visualization
        """
        # Configure ChromaDB logging to reduce warnings
        import logging
        logging.getLogger('chromadb').setLevel(logging.ERROR)
        
        logger.info(f"Question: {question}")
        
        # Generate SQL from the question
        sql = self.vn.generate_sql(question)
        logger.info(f"Generated SQL: {sql}")
        
        # Execute the SQL
        try:
            results = self.vn.run_sql(sql)
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            
            # Convert DataFrame to a JSON-serializable format
            if hasattr(results, 'to_dict'):
                # It's a pandas DataFrame, convert to records
                serializable_results = results.to_dict(orient='records')
            else:
                # It's already a list or other serializable type
                serializable_results = results
            
            return {
                "question": question,
                "sql": sql,
                "results": serializable_results
            }
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            return {
                "question": question,
                "sql": sql,
                "error": str(e)
            }
        
    def get_training_data(self) -> List[Dict[str, Any]]:
        """
        Get the current training data in Vanna.
        
        Returns:
            List of training data items
        """
        try:
            # Get training data from Vanna
            return self.vn.get_training_data()
        except Exception as e:
            logger.error(f"Error getting training data: {str(e)}")
            return []


def initialize_vanna(api_key: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None, train: bool = False) -> MetaVannaCore:
    """
    Initialize the Vanna integration.
    
    Args:
        api_key: Google API key. If None, will try to get from environment variable.
        model: Gemini model to use. If None, will try to get from environment variable.
        temperature: Temperature for the model. If None, will try to get from environment variable.
        train: Whether to train Vanna on dbt models after initialization.
            When training, existing training data will always be cleared first.
        
    Returns:
        Initialized MetaVannaCore instance
    """
    vanna_instance = MetaVannaCore(api_key=api_key, model=model, temperature=temperature)
    
    if train:
        vanna_instance.train_on_dbt_models()
        
    return vanna_instance
