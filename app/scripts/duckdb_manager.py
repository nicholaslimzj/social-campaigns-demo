#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DuckDB Manager for the Meta Demo application.
This module handles the integration with DuckDB for analytics.
"""

import os
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Union, Optional

import duckdb
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
# Determine base directory - assume /data exists in container
BASE_DIR = Path('/')
DATA_ROOT = Path('/data')
PROCESSED_DIR = DATA_ROOT / "processed"
DB_DIR = DATA_ROOT / "db"
DB_FILE = DB_DIR / "meta_analytics.duckdb"

# SQL fragments for reuse
SQL_FRAGMENTS = {
    # Column definitions
    "base_columns": """
        Campaign_ID,
        Target_Audience,
        Campaign_Goal,
        Duration,
        Channel_Used,
        Conversion_Rate,
        Acquisition_Cost,
        ROI,
        Location,
        Language,
        Clicks,
        Impressions,
        Engagement_Score,
        Customer_Segment,
        Date,
        Company
    """,
    
    # Derived metrics
    "derived_metrics": """
        CAST(Clicks AS FLOAT) / NULLIF(CAST(Impressions AS FLOAT), 0) AS CTR,
        Acquisition_Cost / NULLIF(CAST(Clicks AS FLOAT), 0) AS CPC,
        ROI + 1 AS ROAS,
        Date AS StandardizedDate
    """,
    
    # Aggregation metrics
    "agg_metrics": """
        COUNT(*) as campaign_count,
        AVG(Conversion_Rate) as avg_conversion_rate,
        AVG(ROI) as avg_roi,
        AVG(Acquisition_Cost) as avg_acquisition_cost,
        SUM(Clicks) as total_clicks,
        SUM(Impressions) as total_impressions,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr
    """
}


class DuckDBManager:
    """
    Manager class for DuckDB operations.
    """
    
    def __init__(self, db_file: Optional[Path] = None):
        """
        Initialize the DuckDB manager.
        
        Args:
            db_file (Path, optional): Path to the DuckDB database file
        """
        self.db_file = db_file or DB_FILE
        self.connection = None
        
        # Ensure the database directory exists
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"DuckDB manager initialized with database: {self.db_file}")
    
    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Connect to the DuckDB database.
        
        Returns:
            duckdb.DuckDBPyConnection: DuckDB connection
        """
        if self.connection is None:
            logger.info(f"Connecting to DuckDB database: {self.db_file}")
            self.connection = duckdb.connect(database=str(self.db_file), read_only=False)
            
            # Enable automatic loading of extensions
            self.connection.execute("INSTALL httpfs;")
            self.connection.execute("LOAD httpfs;")
            
            # Enable parquet scanning
            self.connection.execute("PRAGMA enable_object_cache;")
            
            # Store the data root path in the database for portability
            self.connection.execute(f"CREATE OR REPLACE MACRO DATA_ROOT() AS '{DATA_ROOT}'")
            
            logger.info("Connected to DuckDB database")
        
        return self.connection
    
    def close(self):
        """
        Close the DuckDB connection.
        """
        if self.connection is not None:
            logger.info("Closing DuckDB connection")
            self.connection.close()
            self.connection = None
    
    def __enter__(self) -> 'DuckDBManager':
        """
        Context manager entry.
        
        Returns:
            DuckDBManager: Self
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit.
        
        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        self.close()
    
    def execute(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            duckdb.DuckDBPyRelation: Query result
        """
        conn = self.connect()
        logger.debug(f"Executing query: {query}")
        return conn.execute(query)
    
    def query_to_df(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pd.DataFrame: Query result as a pandas DataFrame
        """
        result = self.execute(query)
        return result.fetchdf()
    
    def run_dbt(self, models: Optional[List[str]] = None) -> bool:
        """
        Run dbt models to create views and tables.
        
        Args:
            models (List[str], optional): Specific models to run. Defaults to None (all models).
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if the processed directory exists
            if not PROCESSED_DIR.exists():
                logger.error(f"Processed directory not found: {PROCESSED_DIR}")
                return False
            
            # Set up environment variables for dbt
            env = os.environ.copy()
            env["DBT_PROFILES_DIR"] = str(Path(__file__).parent.parent / "dbt")
            
            # Build the dbt command
            dbt_project_dir = Path(__file__).parent.parent / "dbt"
            cmd = ["dbt", "run", "--project-dir", str(dbt_project_dir)]
            
            # Add specific models if provided
            if models:
                cmd.extend(["--select"] + models)
            
            # Run the dbt command
            logger.info(f"Running dbt command: {' '.join(cmd)}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"dbt command failed: {result.stderr}")
                return False
            
            logger.info(f"dbt command succeeded: {result.stdout}")
            
            # Verify the view
            count_query = "SELECT COUNT(*) FROM stg_campaigns;"
            count_result = self.query_to_df(count_query)
            count = count_result.iloc[0, 0]
            
            logger.info(f"Campaigns view contains {count} rows")
            return True
            
        except Exception as e:
            logger.error(f"Error running dbt: {e}")
            return False
            
    def create_campaign_view(self) -> bool:
        """
        Create a view of all campaign data from parquet files using dbt.
        
        Returns:
            bool: True if successful, False otherwise
        """
        return self.run_dbt(["stg_campaigns"])
    
    def create_materialized_views(self) -> bool:
        """
        Create materialized views for analytics.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create a materialized view for company metrics
            company_metrics_query = f"""
            CREATE OR REPLACE TABLE company_metrics AS
            SELECT 
                Company,
                {SQL_FRAGMENTS['agg_metrics']}
            FROM campaigns
            GROUP BY Company;
            """
            
            self.execute(company_metrics_query)
            logger.info("Created company_metrics materialized view")
            
            # Create a materialized view for channel metrics
            channel_metrics_query = f"""
            CREATE OR REPLACE TABLE channel_metrics AS
            SELECT 
                Channel_Used,
                {SQL_FRAGMENTS['agg_metrics']}
            FROM campaigns
            GROUP BY Channel_Used;
            """
            
            self.execute(channel_metrics_query)
            logger.info("Created channel_metrics materialized view")
            
            # Create a materialized view for audience metrics
            audience_metrics_query = f"""
            CREATE OR REPLACE TABLE audience_metrics AS
            SELECT 
                Target_Audience,
                {SQL_FRAGMENTS['agg_metrics']}
            FROM campaigns
            GROUP BY Target_Audience;
            """
            
            self.execute(audience_metrics_query)
            logger.info("Created audience_metrics materialized view")
            
            # Create a materialized view for time-based metrics
            time_metrics_query = f"""
            CREATE OR REPLACE TABLE time_metrics AS
            SELECT 
                EXTRACT(MONTH FROM CAST(Date AS DATE)) as month,
                {SQL_FRAGMENTS['agg_metrics']}
            FROM campaigns
            GROUP BY month
            ORDER BY month;
            """
            
            self.execute(time_metrics_query)
            logger.info("Created time_metrics materialized view")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating materialized views: {e}")
            return False
    
    def get_top_performing_campaigns(self, metric: str = 'ROI', limit: int = 10) -> pd.DataFrame:
        """
        Get the top performing campaigns based on a specific metric.
        
        Args:
            metric (str): Metric to sort by (ROI, Conversion_Rate, etc.)
            limit (int): Number of campaigns to return
            
        Returns:
            pd.DataFrame: Top performing campaigns
        """
        valid_metrics = ['ROI', 'Conversion_Rate', 'Clicks', 'Impressions', 'Engagement_Score']
        
        if metric not in valid_metrics:
            logger.warning(f"Invalid metric: {metric}. Using ROI instead.")
            metric = 'ROI'
        
        query = f"""
        SELECT 
            Campaign_ID,
            Company,
            Channel_Used,
            Target_Audience,
            Campaign_Goal,
            Conversion_Rate,
            ROI,
            Clicks,
            Impressions,
            Engagement_Score,
            Date
        FROM campaigns
        ORDER BY {metric} DESC
        LIMIT {limit};
        """
        
        return self.query_to_df(query)
    
    def get_channel_comparison(self) -> pd.DataFrame:
        """
        Get a comparison of different channels.
        
        Returns:
            pd.DataFrame: Channel comparison
        """
        query = """
        SELECT * FROM channel_metrics
        ORDER BY avg_roi DESC;
        """
        
        return self.query_to_df(query)
    
    def get_audience_insights(self) -> pd.DataFrame:
        """
        Get insights about different target audiences.
        
        Returns:
            pd.DataFrame: Audience insights
        """
        query = """
        SELECT * FROM audience_metrics
        ORDER BY avg_roi DESC;
        """
        
        return self.query_to_df(query)
    
    def get_company_performance(self) -> pd.DataFrame:
        """
        Get performance metrics for each company.
        
        Returns:
            pd.DataFrame: Company performance
        """
        query = """
        SELECT * FROM company_metrics
        ORDER BY avg_roi DESC;
        """
        
        return self.query_to_df(query)
    
    def get_monthly_trends(self) -> pd.DataFrame:
        """
        Get monthly trends.
        
        Returns:
            pd.DataFrame: Monthly trends
        """
        query = """
        SELECT * FROM time_metrics
        ORDER BY month;
        """
        
        return self.query_to_df(query)
    
    def get_campaign_details(self, campaign_id: int) -> pd.DataFrame:
        """
        Get details for a specific campaign.
        
        Args:
            campaign_id (int): Campaign ID
            
        Returns:
            pd.DataFrame: Campaign details
        """
        query = f"""
        SELECT * FROM campaigns
        WHERE Campaign_ID = {campaign_id};
        """
        
        return self.query_to_df(query)


def initialize_duckdb():
    """
    Initialize DuckDB with views and tables.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with DuckDBManager() as db:
            # Create the campaigns view
            db.create_campaign_view()
            
            # Create materialized views
            # Commented out to test just the campaign view
            # db.create_materialized_views()
            
            # Test a query
            top_campaigns = db.get_top_performing_campaigns(limit=5)
            logger.info(f"Top 5 campaigns by ROI:\n{top_campaigns}")
            
            return True
    
    except Exception as e:
        logger.error(f"Error initializing DuckDB: {e}")
        return False


if __name__ == "__main__":
    # When run directly, initialize DuckDB
    initialize_duckdb()
