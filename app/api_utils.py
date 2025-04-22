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
from datetime import datetime, timedelta

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

def execute_query(query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
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

def get_companies() -> List[Dict[str, Any]]:
    """
    Get a list of all companies in the dataset.
    
    Returns:
        List[Dict[str, Any]]: List of companies
    """
    query = """
    SELECT DISTINCT
        Company as company
    FROM meta_analytics.main.segment_company_quarter_rankings
    ORDER BY company
    """
    
    try:
        results = execute_query(query)
        return results
    except Exception as e:
        logger.error(f"Error getting companies: {str(e)}")
        return []

def get_monthly_company_metrics(company_id: str, include_anomalies: bool = False) -> Dict[str, Any]:
    """
    Get monthly metrics for a specific company.
    
    Args:
        company_id: Company name to get monthly metrics for
        include_anomalies: Whether to include anomaly detection
        
    Returns:
        Dict[str, Any]: Monthly metrics for the company
    """
    query = """
    SELECT 
        month,
        avg_conversion_rate as conversion_rate,
        avg_roi as roi,
        avg_acquisition_cost as acquisition_cost,
        avg_ctr as ctr,
        total_spend as spend,
        total_revenue as revenue,
        total_clicks as clicks,
        total_impressions as impressions
    FROM campaign_monthly_metrics
    WHERE Company = ?
    ORDER BY month
    """
    
    try:
        results = execute_query(query, [company_id])
        
        if not results:
            return {"metrics": {}}
            
        # Calculate CAC (Customer Acquisition Cost) for each result
        for result in results:
            # CAC = Total Spend / (Clicks * Conversion Rate)
            if result.get('clicks', 0) > 0 and result.get('conversion_rate', 0) > 0:
                conversions = result['clicks'] * result['conversion_rate']
                result['cac'] = result['spend'] / conversions if conversions > 0 else 0
            else:
                result['cac'] = 0
                
        # Format the results into the expected structure
        metrics = {
            "conversion_rate": [],
            "roi": [],
            "acquisition_cost": [],
            "ctr": [],
            "spend": [],
            "revenue": [],
            "cac": []
        }
        
        for row in results:
            month_str = f"2022-{int(row['month']):02d}"
            
            metrics["conversion_rate"].append({
                "month": month_str,
                "value": row["conversion_rate"],
                "is_anomaly": False  # Default value, will be updated if include_anomalies is True
            })
            
            metrics["roi"].append({
                "month": month_str,
                "value": row["roi"]
            })
            
            metrics["acquisition_cost"].append({
                "month": month_str,
                "value": row["acquisition_cost"]
            })
            
            # Add CAC to metrics
            metrics["cac"].append({
                "month": month_str,
                "value": row["cac"]
            })
            
            metrics["ctr"].append({
                "month": month_str,
                "value": row["ctr"]
            })
            
            metrics["spend"].append({
                "month": month_str,
                "value": row["spend"]
            })
            
            metrics["revenue"].append({
                "month": month_str,
                "value": row["revenue"]
            })
        
        response = {"metrics": metrics}
        
        # If anomalies are requested, get them from the anomalies table
        if include_anomalies:
            # Query for anomalies using individual anomaly flags for each metric
            anomaly_query = """
            WITH anomaly_data AS (
                -- Conversion rate anomalies
                SELECT
                    'conversion_rate' as metric,
                    month,
                    avg_conversion_rate as value,
                    conversion_rate_z as z_score,
                    'Conversion rate ' || CASE WHEN conversion_rate_z > 0 THEN 'higher' ELSE 'lower' END as explanation
                FROM metrics_monthly_anomalies
                WHERE Company = ? AND conversion_rate_anomaly = 'anomaly'
                
                UNION ALL
                
                -- ROI anomalies
                SELECT
                    'roi' as metric,
                    month,
                    avg_roi as value,
                    roi_z as z_score,
                    'ROI ' || CASE WHEN roi_z > 0 THEN 'higher' ELSE 'lower' END as explanation
                FROM metrics_monthly_anomalies
                WHERE Company = ? AND roi_anomaly = 'anomaly'
                
                UNION ALL
                
                -- Acquisition cost anomalies
                SELECT
                    'acquisition_cost' as metric,
                    month,
                    avg_acquisition_cost as value,
                    acquisition_cost_z as z_score,
                    'Acquisition cost ' || CASE WHEN acquisition_cost_z > 0 THEN 'higher' ELSE 'lower' END as explanation
                FROM metrics_monthly_anomalies
                WHERE Company = ? AND acquisition_cost_anomaly = 'anomaly'
                
                UNION ALL
                
                -- CTR anomalies
                SELECT
                    'ctr' as metric,
                    month,
                    monthly_ctr as value,
                    ctr_z as z_score,
                    'CTR ' || CASE WHEN ctr_z > 0 THEN 'higher' ELSE 'lower' END as explanation
                FROM metrics_monthly_anomalies
                WHERE Company = ? AND ctr_anomaly = 'anomaly'
                
                UNION ALL
                
                -- Spend anomalies
                SELECT
                    'spend' as metric,
                    month,
                    total_spend as value,
                    spend_z as z_score,
                    'Spend ' || CASE WHEN spend_z > 0 THEN 'higher' ELSE 'lower' END as explanation
                FROM metrics_monthly_anomalies
                WHERE Company = ? AND spend_anomaly = 'anomaly'
                
                UNION ALL
                
                -- Revenue anomalies
                SELECT
                    'revenue' as metric,
                    month,
                    total_revenue as value,
                    revenue_z as z_score,
                    'Revenue ' || CASE WHEN revenue_z > 0 THEN 'higher' ELSE 'lower' END as explanation
                FROM metrics_monthly_anomalies
                WHERE Company = ? AND revenue_anomaly = 'anomaly'
            )
            SELECT * FROM anomaly_data
            ORDER BY ABS(z_score) DESC
            """
            
            # Pass company_id six times, once for each subquery in the UNION ALL
            anomaly_results = execute_query(anomaly_query, [company_id, company_id, company_id, company_id, company_id, company_id])
            
            if anomaly_results:
                # Mark metrics as anomalies
                for anomaly in anomaly_results:
                    month_str = f"2022-{int(anomaly['month']):02d}"
                    metric = anomaly["metric"].lower()
                    
                    if metric in metrics:
                        for i, data_point in enumerate(metrics[metric]):
                            if data_point["month"] == month_str:
                                metrics[metric][i]["is_anomaly"] = True
                                break
                
                # Add anomalies to the response
                response["anomalies"] = [
                    {
                        "metric": anomaly["metric"].lower(),
                        "month": f"2022-{int(anomaly['month']):02d}",
                        "value": anomaly["value"],
                        "z_score": anomaly["z_score"],
                        "explanation": anomaly["explanation"]
                    }
                    for anomaly in anomaly_results
                ]
        
        return response
    except Exception as e:
        logger.error(f"Error getting monthly company metrics: {str(e)}")
        return {"metrics": {}}
