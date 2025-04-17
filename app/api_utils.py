"""
Utility functions for the API module.

This module provides helper functions for the API endpoints to interact with DuckDB
and retrieve data from the dbt models.
"""

import os
import logging
import duckdb
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Always use the container path as specified
DB_PATH = '/data/db/meta_analytics.duckdb'

def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Get a connection to the DuckDB database.
    
    Returns:
        duckdb.DuckDBPyConnection: A connection to the DuckDB database
    """
    try:
        # Create the directory if it doesn't exist
        db_dir = os.path.dirname(DB_PATH)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Connect to the database
        conn = duckdb.connect(DB_PATH)
        
        # Set up DATA_ROOT macro
        conn.execute("CREATE OR REPLACE MACRO DATA_ROOT() AS '/data'")
            
        return conn
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {str(e)}")
        raise

def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a SQL query and return the results as a list of dictionaries.
    
    Args:
        query: The SQL query to execute
        params: Optional parameters for the query
        
    Returns:
        List[Dict[str, Any]]: The query results
    """
    try:
        conn = get_connection()
        
        # Execute the query
        if params:
            result = conn.execute(query, params).fetchdf()
        else:
            result = conn.execute(query).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Query: {query}")
        if params:
            logger.error(f"Params: {params}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def get_companies() -> List[str]:
    """
    Get a list of all companies in the dataset.
    
    Returns:
        List[str]: List of company names
    """
    query = """
    SELECT DISTINCT Company 
    FROM stg_campaigns
    ORDER BY Company
    """
    
    try:
        results = execute_query(query)
        return [row['Company'] for row in results]
    except Exception as e:
        logger.error(f"Error getting companies: {str(e)}")
        return []

def get_company_metrics(company: Optional[str] = None) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """
    Get KPI metrics for a specific company or all companies.
    
    Args:
        company: Optional company name to filter metrics
        
    Returns:
        Union[Dict[str, Any], Dict[str, Dict[str, Any]]]: Metrics for one or all companies
    """
    if company:
        query = """
        SELECT 
            AVG(ROI) as roi,
            AVG(Conversion_Rate) as conversion_rate,
            AVG(Acquisition_Cost) as acquisition_cost,
            CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr,
            -- Calculate a simple engagement score (example formula)
            (AVG(ROI) * 0.4 + AVG(Conversion_Rate) * 100 * 0.3 + 
             CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) * 100 * 0.3) as engagement_score
        FROM stg_campaigns
        WHERE Company = ?
        """
        results = execute_query(query, [company])
        return results[0] if results else {}
    else:
        # Get metrics for all companies
        query = """
        SELECT 
            Company,
            AVG(ROI) as roi,
            AVG(Conversion_Rate) as conversion_rate,
            AVG(Acquisition_Cost) as acquisition_cost,
            CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr,
            -- Calculate a simple engagement score (example formula)
            (AVG(ROI) * 0.4 + AVG(Conversion_Rate) * 100 * 0.3 + 
             CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) * 100 * 0.3) as engagement_score
        FROM stg_campaigns
        GROUP BY Company
        ORDER BY Company
        """
        results = execute_query(query)
        
        # Convert to dictionary with company names as keys
        metrics_by_company = {}
        for row in results:
            company_name = row.pop('Company')
            metrics_by_company[company_name] = row
            
        return metrics_by_company

def get_time_series_data(company: str) -> List[Dict[str, Any]]:
    """
    Get time series data for a specific company.
    
    Args:
        company: Company name to get time series data for
        
    Returns:
        List[Dict[str, Any]]: Time series data for the company
    """
    query = """
    SELECT 
        strftime(StandardizedDate, '%Y-%m') as date,
        AVG(ROI) as roi,
        AVG(Conversion_Rate) as conversion_rate,
        AVG(Acquisition_Cost) as acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr
    FROM stg_campaigns
    WHERE Company = ?
    GROUP BY strftime(StandardizedDate, '%Y-%m')
    ORDER BY date
    """
    
    return execute_query(query, [company])

def get_segment_performance(company: str) -> List[Dict[str, Any]]:
    """
    Get segment performance for a specific company.
    
    Args:
        company: Company name to get segment performance for
        
    Returns:
        List[Dict[str, Any]]: Segment performance data for the company
    """
    query = """
    SELECT 
        Customer_Segment as segment,
        AVG(ROI) as roi,
        AVG(Conversion_Rate) as conversion_rate,
        AVG(Acquisition_Cost) as acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr,
        COUNT(*) as campaign_count
    FROM stg_campaigns
    WHERE Company = ?
    GROUP BY Customer_Segment
    ORDER BY roi DESC
    """
    
    return execute_query(query, [company])

def get_channel_performance(company: str) -> List[Dict[str, Any]]:
    """
    Get channel performance for a specific company.
    
    Args:
        company: Company name to get channel performance for
        
    Returns:
        List[Dict[str, Any]]: Channel performance data for the company
    """
    query = """
    SELECT 
        Channel as channel,
        AVG(ROI) as roi,
        AVG(Conversion_Rate) as conversion_rate,
        AVG(Acquisition_Cost) as acquisition_cost,
        CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as ctr,
        COUNT(*) as campaign_count
    FROM stg_campaigns
    WHERE Company = ?
    GROUP BY Channel
    ORDER BY roi DESC
    """
    
    return execute_query(query, [company])

def get_campaign_details(company: str) -> List[Dict[str, Any]]:
    """
    Get campaign details for a specific company.
    
    Args:
        company: Company name to get campaign details for
        
    Returns:
        List[Dict[str, Any]]: Campaign details for the company
    """
    query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost,
        CAST(Clicks AS FLOAT) / NULLIF(Impressions, 0) as ctr,
        Duration as duration
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY StandardizedDate DESC
    """
    
    return execute_query(query, [company])

def get_campaign_clusters(company: str) -> List[Dict[str, Any]]:
    """
    Get high-performing campaign combinations for a specific company.
    
    Args:
        company: Company name to get campaign clusters for
        
    Returns:
        List[Dict[str, Any]]: Campaign clusters for the company
    """
    query = """
    SELECT *
    FROM campaign_historical_clusters
    WHERE Company = ?
    ORDER BY composite_score DESC
    """
    
    return execute_query(query, [company])

def get_campaign_duration_analysis(company: str) -> Dict[str, Any]:
    """
    Get optimal duration analysis for a specific company.
    
    Args:
        company: Company name to get campaign duration analysis for
        
    Returns:
        Dict[str, Any]: Campaign duration analysis for the company
    """
    # Get overall optimal duration
    overall_query = """
    SELECT 
        optimal_duration_range as overall_optimal,
        avg_roi as overall_roi
    FROM campaign_duration_historical_analysis
    WHERE Company = ? AND dimension_type = 'overall'
    LIMIT 1
    """
    
    overall_result = execute_query(overall_query, [company])
    
    # Get segment-specific durations
    segment_query = """
    SELECT 
        dimension_value as segment,
        optimal_duration_range,
        avg_roi,
        potential_roi_improvement
    FROM campaign_duration_historical_analysis
    WHERE Company = ? AND dimension_type = 'segment'
    ORDER BY avg_roi DESC
    """
    
    segment_results = execute_query(segment_query, [company])
    
    # Get channel-specific durations
    channel_query = """
    SELECT 
        dimension_value as channel,
        optimal_duration_range,
        avg_roi,
        potential_roi_improvement
    FROM campaign_duration_historical_analysis
    WHERE Company = ? AND dimension_type = 'channel'
    ORDER BY avg_roi DESC
    """
    
    channel_results = execute_query(channel_query, [company])
    
    # Get goal-specific durations
    goal_query = """
    SELECT 
        dimension_value as goal,
        optimal_duration_range,
        avg_roi,
        potential_roi_improvement
    FROM campaign_duration_historical_analysis
    WHERE Company = ? AND dimension_type = 'goal'
    ORDER BY avg_roi DESC
    """
    
    goal_results = execute_query(goal_query, [company])
    
    # Combine results
    return {
        "overall_optimal": overall_result[0]["overall_optimal"] if overall_result else None,
        "overall_roi": overall_result[0]["overall_roi"] if overall_result else None,
        "by_segment": segment_results,
        "by_channel": channel_results,
        "by_goal": goal_results
    }

def get_performance_matrix(company: str) -> List[Dict[str, Any]]:
    """
    Get goal vs segment performance matrix for a specific company.
    
    Args:
        company: Company name to get performance matrix for
        
    Returns:
        List[Dict[str, Any]]: Performance matrix for the company
    """
    query = """
    SELECT *
    FROM campaign_historical_performance_matrix
    WHERE Company = ?
    ORDER BY composite_score DESC
    """
    
    return execute_query(query, [company])

def get_forecasting(company: str) -> List[Dict[str, Any]]:
    """
    Get campaign performance forecasts for a specific company.
    
    Args:
        company: Company name to get forecasts for
        
    Returns:
        List[Dict[str, Any]]: Forecast data for the company
    """
    query = """
    SELECT *
    FROM campaign_future_forecast
    WHERE Company = ?
    ORDER BY forecast_date
    """
    
    return execute_query(query, [company])

def get_top_bottom_performers(company: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get top and bottom performing campaigns for a specific company.
    
    Args:
        company: Company name to get top/bottom performers for
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Top and bottom performers for the company
    """
    # Get top ROI performers
    top_roi_query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY ROI DESC
    LIMIT 5
    """
    
    top_roi = execute_query(top_roi_query, [company])
    
    # Get bottom ROI performers
    bottom_roi_query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY ROI ASC
    LIMIT 5
    """
    
    bottom_roi = execute_query(bottom_roi_query, [company])
    
    # Get top conversion rate performers
    top_conversion_query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY Conversion_Rate DESC
    LIMIT 5
    """
    
    top_conversion = execute_query(top_conversion_query, [company])
    
    # Get bottom conversion rate performers
    bottom_conversion_query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY Conversion_Rate ASC
    LIMIT 5
    """
    
    bottom_conversion = execute_query(bottom_conversion_query, [company])
    
    # Get top acquisition cost performers (lowest cost)
    top_acquisition_cost_query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY Acquisition_Cost ASC
    LIMIT 5
    """
    
    top_acquisition_cost = execute_query(top_acquisition_cost_query, [company])
    
    # Get bottom acquisition cost performers (highest cost)
    bottom_acquisition_cost_query = """
    SELECT 
        Campaign_ID as campaign_id,
        Campaign_Goal as goal,
        Channel as channel,
        Customer_Segment as segment,
        ROI as roi,
        Conversion_Rate as conversion_rate,
        Acquisition_Cost as acquisition_cost
    FROM stg_campaigns
    WHERE Company = ?
    ORDER BY Acquisition_Cost DESC
    LIMIT 5
    """
    
    bottom_acquisition_cost = execute_query(bottom_acquisition_cost_query, [company])
    
    # Combine results
    return {
        "top_roi": top_roi,
        "bottom_roi": bottom_roi,
        "top_conversion": top_conversion,
        "bottom_conversion": bottom_conversion,
        "top_acquisition_cost": top_acquisition_cost,
        "bottom_acquisition_cost": bottom_acquisition_cost
    }

def get_anomalies(company: str) -> List[Dict[str, Any]]:
    """
    Get anomaly detection with context for a specific company.
    
    Args:
        company: Company name to get anomalies for
        
    Returns:
        List[Dict[str, Any]]: Anomaly data for the company
    """
    query = """
    SELECT *
    FROM metrics_historical_anomalies
    WHERE Company = ?
    ORDER BY date DESC, abs(z_score) DESC
    """
    
    return execute_query(query, [company])
