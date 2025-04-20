"""
Insights Generator for the Meta Demo project.

This module provides the core functionality for generating AI-powered insights
about social media advertising data. It handles caching, LLM initialization,
and orchestration of different insight types.
"""

import os
import logging
import json
import duckdb
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Callable
from datetime import datetime, date
from decimal import Decimal

# LangChain imports
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path for the insights cache database (separate from analytics DB)
INSIGHTS_DB_PATH = '/data/db/insights_cache.duckdb'

# Custom JSON encoder to handle datetime and Decimal objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def get_insights_connection() -> duckdb.DuckDBPyConnection:
    """
    Get a connection to the insights cache DuckDB database.
    
    Returns:
        duckdb.DuckDBPyConnection: A connection to the insights cache database
    """
    try:
        # Create the directory if it doesn't exist
        db_dir = os.path.dirname(INSIGHTS_DB_PATH)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Connect to the database
        conn = duckdb.connect(INSIGHTS_DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to insights cache DB: {str(e)}")
        raise

def get_analytics_connection() -> duckdb.DuckDBPyConnection:
    """
    Get a connection to the analytics DuckDB database.
    
    Returns:
        duckdb.DuckDBPyConnection: A connection to the analytics database
    """
    try:
        # Connect to the analytics database
        conn = duckdb.connect('/data/db/meta_analytics.duckdb')
        
        # Set up DATA_ROOT macro
        conn.execute("CREATE OR REPLACE MACRO DATA_ROOT() AS '/data'")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to analytics DB: {str(e)}")
        raise

def setup_insights_cache():
    """
    Set up the insights cache table in the insights cache database if it doesn't exist.
    """
    try:
        conn = get_insights_connection()
        
        # Create insights cache table if it doesn't exist
        conn.execute("""
        CREATE TABLE IF NOT EXISTS insights_cache (
            company_name VARCHAR,
            insight_type VARCHAR,  -- 'company', 'segment', 'channel', 'campaign'
            insight_text TEXT,
            generated_at TIMESTAMP,
            PRIMARY KEY (company_name, insight_type)
        )
        """)
        
        logger.info("Insights cache table created or already exists")
    except Exception as e:
        logger.error(f"Error setting up insights cache: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def get_cached_insight(company_name: str, insight_type: str) -> Optional[str]:
    """
    Get a cached insight from the database if it exists and is not too old.
    
    Args:
        company_name: The name of the company
        insight_type: The type of insight ('company', 'segment', 'channel', 'campaign')
        
    Returns:
        Optional[str]: The cached insight text if found and not expired, None otherwise
    """
    try:
        conn = get_insights_connection()
        
        # Get the cached insight if it's less than 24 hours old
        result = conn.execute("""
        SELECT insight_text, generated_at
        FROM insights_cache
        WHERE company_name = ?
        AND insight_type = ?
        AND generated_at > CURRENT_TIMESTAMP - INTERVAL 24 HOUR
        """, [company_name, insight_type]).fetchone()
        
        if result:
            logger.info(f"Found cached {insight_type} insight for {company_name}")
            return result[0]
        
        return None
    except Exception as e:
        logger.error(f"Error getting cached insight: {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def cache_insight(company_name: str, insight_type: str, insight_text: str):
    """
    Cache an insight in the database.
    
    Args:
        company_name: The name of the company
        insight_type: The type of insight ('company', 'segment', 'channel', 'campaign')
        insight_text: The insight text to cache
    """
    # Validate inputs before attempting to cache
    if not company_name or not insight_type or not insight_text:
        logger.warning("Cannot cache insight: missing required parameters")
        return
        
    # Validate insight type
    valid_types = ['company', 'segment', 'channel', 'campaign']
    if insight_type not in valid_types:
        logger.warning(f"Cannot cache insight: invalid insight type '{insight_type}'")
        return
    
    try:
        conn = get_insights_connection()
        
        # Insert or replace the insight
        conn.execute("""
        INSERT OR REPLACE INTO insights_cache (company_name, insight_type, insight_text, generated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, [company_name, insight_type, insight_text])
        
        logger.info(f"Cached {insight_type} insight for {company_name}")
    except Exception as e:
        logger.error(f"Error caching insight: {str(e)}")
        # Do not propagate the exception - we don't want caching failures to break the app
    finally:
        if 'conn' in locals():
            conn.close()

class InsightsGenerator:
    """Generate AI-powered insights for social media advertising data."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.5-pro-preview-03-25", temperature: float = 0.2):
        """Initialize the insights generator."""
        # Use environment variable if API key not provided
        if not api_key:
            api_key = os.environ.get("GOOGLE_API_KEY")
            if not api_key:
                raise ValueError("Google API key is required. Set GOOGLE_API_KEY environment variable or pass it directly.")
        
        # Initialize LangChain components
        self.llm = ChatGoogleGenerativeAI(
            google_api_key=api_key,
            model=model,
            temperature=temperature,
            convert_system_message_to_human=True
        )
        
        # Set up the insights cache
        setup_insights_cache()
        
        logger.info(f"Initialized InsightsGenerator with Gemini model {model}")
    
    def generate_insight(self, company_name: str, insight_type: str, force_refresh: bool = False) -> str:
        """
        Generate an insight for a company.
        
        Args:
            company_name: The name of the company
            insight_type: The type of insight ('company', 'segment', 'channel', 'campaign')
            force_refresh: Whether to force a refresh of the insight
            
        Returns:
            str: The generated insight
        """
        # Check cache first unless force refresh is requested
        if not force_refresh:
            cached_insight = get_cached_insight(company_name, insight_type)
            if cached_insight:
                return cached_insight
        
        # Import the appropriate insight module dynamically
        try:
            if insight_type == 'company':
                from app.scripts.company_insights import generate_company_insight
                insight_text = generate_company_insight(self.llm, company_name)
            elif insight_type == 'segment':
                from app.scripts.segment_insights import generate_segment_insight
                insight_text = generate_segment_insight(self.llm, company_name)
            elif insight_type == 'channel':
                from app.scripts.channel_insights import generate_channel_insight
                insight_text = generate_channel_insight(self.llm, company_name)
            elif insight_type == 'campaign':
                from app.scripts.campaign_insights import generate_campaign_insight
                insight_text = generate_campaign_insight(self.llm, company_name)
            else:
                raise ValueError(f"Invalid insight type: {insight_type}")
            
            # Only cache the insight if generation was successful
            if insight_text:  # Make sure we have valid insight text
                cache_insight(company_name, insight_type, insight_text)
            
            return insight_text
        except Exception as e:
            logger.error(f"Error generating {insight_type} insight for {company_name}: {str(e)}")
            # Do not cache anything when an error occurs
            raise

# Command-line interface function
def generate_insight_cli(company_name: str = None, insight_type: str = None, force_refresh: bool = False):
    """
    Command-line interface for generating insights.
    
    Args:
        company_name: The name of the company (optional, if None will list available companies)
        insight_type: The type of insight ('company', 'segment', 'channel', 'campaign')
                     (optional, defaults to 'company')
        force_refresh: Whether to force a refresh of the insight
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # If no company name is provided, list available companies
        if not company_name:
            conn = get_analytics_connection()
            companies = conn.execute("SELECT DISTINCT Company FROM stg_campaigns ORDER BY Company").fetchall()
            conn.close()
            
            print("\nAvailable companies:")
            for i, (company,) in enumerate(companies, 1):
                print(f"{i}. {company}")
            
            print("\nUsage: python -m app.main insights [company_name] [insight_type] [--force]")
            print("Insight types: company, segment, channel, campaign")
            return True
            
        # If no insight type is provided, default to company
        if not insight_type:
            insight_type = "company"
            
        # Validate insight type
        valid_types = ["company", "segment", "channel", "campaign"]
        if insight_type not in valid_types:
            print(f"Error: Invalid insight type: {insight_type}")
            print(f"Valid insight types: {', '.join(valid_types)}")
            return False
            
        # Validate company exists
        conn = get_analytics_connection()
        company_exists = conn.execute(
            "SELECT COUNT(*) FROM stg_campaigns WHERE Company = ?", 
            [company_name]
        ).fetchone()[0] > 0
        conn.close()
        
        if not company_exists:
            print(f"Error: Company '{company_name}' not found in the database.")
            return False
        
        # Initialize the insights generator
        generator = InsightsGenerator()
        
        # Generate the insight
        insight = generator.generate_insight(company_name, insight_type, force_refresh)
        
        # Print the insight
        print(f"\n--- {insight_type.upper()} INSIGHT FOR {company_name.upper()} ---\n")
        print(insight)
        print("\n" + "-" * 80)
        
        return True
    except Exception as e:
        logger.error(f"Error in generate_insight_cli: {str(e)}")
        print(f"Error generating insight: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate insights for social media advertising data")
    parser.add_argument("company", help="Company name to generate insights for")
    parser.add_argument("type", choices=["company", "segment", "channel", "campaign"], help="Type of insight to generate")
    parser.add_argument("--force", action="store_true", help="Force refresh the insight")
    
    args = parser.parse_args()
    
    generate_insight_cli(args.company, args.type, args.force)
