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

# Check if we're running in a container
if Path('/data').exists():
    # Container environment
    DATA_ROOT_PATH = '/data'
    DBT_MODELS_PATH = '/app/dbt/models'
    DBT_MACROS_PATH = '/app/dbt/macros'
    DB_PATH = '/data/db/meta_analytics.duckdb'
else:
    # Local environment
    DATA_ROOT_PATH = 'c:/Users/wolve/code/meta-demo/data'
    DBT_MODELS_PATH = 'c:/Users/wolve/code/meta-demo/app/dbt/models'
    DBT_MACROS_PATH = 'c:/Users/wolve/code/meta-demo/app/dbt/macros'
    DB_PATH = 'c:/Users/wolve/code/meta-demo/data/db/meta_demo.duckdb'

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
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.5-pro-exp-03-25"):
        """
        Initialize the Vanna integration.
        
        Args:
            api_key: Google API key. If None, will try to get from environment variable.
            model: Gemini model to use.
        """
        if not VANNA_AVAILABLE:
            raise ImportError("Vanna not installed. Install with 'pip install vanna'")
        
        if not GEMINI_AVAILABLE:
            raise ImportError("Google Generative AI not installed. Install with 'pip install google-generativeai'")
        
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("Google API key not provided and not found in environment variables")
        
        self.model = model
        self.db_path = DB_PATH
        
        # Import specific Vanna components
        from vanna.chromadb import ChromaDB_VectorStore
        from vanna.google import GoogleGeminiChat
        import os
        
        # Create a dedicated directory for ChromaDB in the data folder
        chroma_path = os.path.join(DATA_ROOT_PATH, 'vanna_db')
        os.makedirs(chroma_path, exist_ok=True)
        logger.info(f"Using ChromaDB path: {chroma_path}")
        
        # Initialize Vanna with ChromaDB and GoogleGeminiChat
        class MyVanna(ChromaDB_VectorStore, GoogleGeminiChat):
            def __init__(self, config=None):
                ChromaDB_VectorStore.__init__(self, config=config)
                GoogleGeminiChat.__init__(self, config=config)
                
        self.vn = MyVanna(config={
            'api_key': self.api_key, 
            'model': self.model,
            'path': chroma_path  # Set ChromaDB path to data folder
        })
        
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
        
        logger.info("Training Vanna on dbt models...")
        
        # First, train on information schema to understand the database structure
        try:
            logger.info("Training on information schema...")
            df_information_schema = self.vn.run_sql("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
            plan = self.vn.get_training_plan_generic(df_information_schema)
            self.vn.train(plan=plan)
            logger.info("Information schema training completed")
        except Exception as e:
            logger.warning(f"Could not train on information schema: {e}")
        
        # Note: We're not training on the raw dbt SQL files because they contain Jinja templating
        # and macros that might confuse the LLM. Instead, we rely on the information schema
        # and documentation to provide the necessary context.
        
        # Add documentation about the data structure
        self.vn.train(
            documentation="""
            The Meta Demo project contains social media advertising data with the following structure:
            
            Base columns:
            - Campaign_ID: Unique identifier for each campaign (primary key, use for counting unique campaigns)
            - Target_Audience: Demographic target (e.g., "Men 35-44") (dimension for grouping/filtering)
            - Campaign_Goal: Purpose of campaign (e.g., "Product Launch") (dimension for grouping/filtering)
            - Duration: Length of campaign (e.g., "15 Days") (can be used for time-based analysis)
            - Channel_Used: Social media platform (e.g., "Instagram", "Facebook") (dimension for grouping/filtering)
            - Conversion_Rate: Rate of conversions (float) (metric for averaging)
            - Acquisition_Cost: Cost per acquisition (float) (metric for averaging/summing)
            - ROI: Return on investment (float) (metric for averaging)
            - Location: Geographic location (e.g., "Las Vegas") (dimension for grouping/filtering)
            - Language: Campaign language (e.g., "Spanish") (dimension for grouping/filtering)
            - Clicks: Number of clicks (integer) (metric for summing)
            - Impressions: Number of impressions (integer) (metric for summing)
            - Engagement_Score: Engagement level (integer) (metric for averaging)
            - Customer_Segment: Target segment (e.g., "Health") (dimension for grouping/filtering)
            - Date: Campaign date (date) (dimension for time-based grouping/filtering)
            - Company: Company running the campaign (e.g., "Aura Align") (dimension for grouping/filtering)
            
            Derived metrics:
            - CTR: Click-through rate (Clicks/Impressions) (calculated ratio, useful for comparing performance)
            - CPC: Cost per click (Acquisition_Cost/Clicks) (calculated ratio, useful for cost analysis)
            - ROAS: Return on ad spend (ROI + 1) (calculated ratio, useful for ROI analysis)
            - StandardizedDate: Standardized date format (useful for consistent date filtering/grouping)
            
            Aggregated metrics (available in mart models):
            - campaign_count: Count of campaigns (COUNT aggregation, useful for volume analysis)
            - avg_conversion_rate: Average conversion rate (AVG aggregation, useful for performance analysis)
            - avg_roi: Average ROI (AVG aggregation, useful for investment analysis)
            - avg_acquisition_cost: Average acquisition cost (AVG aggregation, useful for cost analysis)
            - total_clicks: Sum of clicks (SUM aggregation, useful for engagement analysis)
            - total_impressions: Sum of impressions (SUM aggregation, useful for reach analysis)
            - overall_ctr: Overall CTR (total_clicks/total_impressions) (calculated from aggregates, useful for overall performance)
            
            Common aggregation patterns:
            - GROUP BY dimensions (Target_Audience, Channel_Used, Customer_Segment, Company, etc.)
            - Filter by dimensions using WHERE clauses
            - Extract time components from Date for time-based analysis (EXTRACT(MONTH FROM Date))
            - Use window functions for comparative analysis (RANK, DENSE_RANK, ROW_NUMBER)
            """
        )
        
        logger.info("Vanna training completed")
    
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
            
            return {
                "question": question,
                "sql": sql,
                "results": results
            }
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            return {
                "question": question,
                "sql": sql,
                "error": str(e)
            }
    
    def explain_sql(self, sql: str) -> str:
        """
        Explain a SQL query in natural language.
        
        Args:
            sql: SQL query to explain
            
        Returns:
            Natural language explanation of the SQL query
        """
        return self.vn.explain_sql(sql)
    
    def suggest_questions(self) -> List[str]:
        """
        Suggest questions that can be asked about the data.
        
        Returns:
            List of suggested questions
        """
        return [
            "What are the top 5 companies by ROI?",
            "Which channel has the highest conversion rate?",
            "How does CTR vary across different target audiences?",
            "What is the average acquisition cost by month?",
            "Which campaigns had the highest engagement score?"
        ]
        
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


def initialize_vanna(api_key: Optional[str] = None, train: bool = False) -> MetaVannaCore:
    """
    Initialize the Vanna integration.
    
    Args:
        api_key: Google API key. If None, will try to get from environment variable.
        train: Whether to train Vanna on dbt models after initialization.
            When training, existing training data will always be cleared first.
        
    Returns:
        Initialized MetaVannaCore instance
    """
    vanna_instance = MetaVannaCore(api_key=api_key)
    
    if train:
        vanna_instance.train_on_dbt_models()
        
    return vanna_instance
