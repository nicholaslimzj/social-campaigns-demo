"""
Campaign API utilities for the Meta Demo project.

This module contains utility functions for campaign-related API endpoints.
These functions interact with the DuckDB database to retrieve campaign data.
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

def execute_query(query: str, params: List = None) -> List[Dict[str, Any]]:
    """
    Execute a DuckDB query and return the results as a list of dictionaries.
    
    Args:
        query: SQL query to execute
        params: Parameters to pass to the query
        
    Returns:
        List[Dict[str, Any]]: Query results as a list of dictionaries
    """
    try:
        # Ensure the database file exists
        db_path = Path(DB_PATH)
        if not db_path.exists():
            logger.error(f"Database file not found at {db_path}")
            return []
        
        # Connect to the database
        conn = duckdb.connect(str(db_path))
        
        # Execute the query with parameters if provided
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        
        # Fetch the results and convert to dictionaries
        column_names = [col[0] for col in result.description]
        rows = result.fetchall()
        
        # Close the connection
        conn.close()
        
        # Convert rows to dictionaries
        return [dict(zip(column_names, row)) for row in rows]
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return []

def get_company_goals(company_id: str, include_metrics: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get list of campaign goals for a specific company.
    
    Args:
        company_id: Company name to get goals for
        include_metrics: Whether to include metrics with each goal
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: List of goals for the company
    """
    if include_metrics:
        query = """
        SELECT 
            goal as goal_id,
            campaign_count,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            ctr as avg_ctr,
            total_clicks,
            total_impressions,
            total_spend,
            total_revenue,
            roi_vs_prev_quarter,
            conversion_rate_vs_prev_quarter,
            roi_rank,
            conversion_rate_rank,
            composite_performance_score as performance_score,
            performance_tier
        FROM goal_quarter_metrics
        WHERE Company = ?
        ORDER BY avg_roi DESC
        """
    else:
        query = """
        SELECT 
            goal as goal_id,
            campaign_count
        FROM goal_quarter_metrics
        WHERE Company = ?
        ORDER BY goal
        """
    
    try:
        results = execute_query(query, [company_id])
        return {"goals": results}
    except Exception as e:
        logger.error(f"Error getting company goals: {str(e)}")
        return {"goals": []}

def get_monthly_campaign_metrics(company_id: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Get monthly campaign metrics for a specific company.
    
    Args:
        company_id: Company name to get monthly campaign metrics for
        
    Returns:
        Dict[str, Dict[str, List[Dict[str, Any]]]]: Monthly campaign metrics for the company
    """
    query = """
    SELECT 
        month,
        avg_conversion_rate as conversion_rate,
        avg_roi as roi,
        avg_acquisition_cost as acquisition_cost,
        avg_ctr as ctr,
        campaign_count,
        total_clicks,
        total_impressions,
        total_spend,
        total_revenue,
        roi_vs_prev_month,
        conversion_rate_vs_prev_month,
        acquisition_cost_vs_prev_month,
        ctr_vs_prev_month
    FROM campaign_monthly_metrics
    WHERE Company = ?
    ORDER BY month
    """
    
    try:
        results = execute_query(query, [company_id])
        
        if not results:
            return {"metrics": {}}
            
        # Format the results into the expected structure
        metrics = {
            "conversion_rate": [],
            "roi": [],
            "acquisition_cost": [],
            "ctr": [],
            "spend": [],
            "revenue": [],
            "changes": []
        }
        
        for row in results:
            month_str = f"2022-{int(row['month']):02d}"
            
            # Add core metrics
            metrics["conversion_rate"].append({
                "month": month_str,
                "value": row["conversion_rate"],
                "change": row["conversion_rate_vs_prev_month"]
            })
            
            metrics["roi"].append({
                "month": month_str,
                "value": row["roi"],
                "change": row["roi_vs_prev_month"]
            })
            
            metrics["acquisition_cost"].append({
                "month": month_str,
                "value": row["acquisition_cost"],
                "change": row["acquisition_cost_vs_prev_month"]
            })
            
            metrics["ctr"].append({
                "month": month_str,
                "value": row["ctr"],
                "change": row["ctr_vs_prev_month"]
            })
            
            # Add spend and revenue metrics
            metrics["spend"].append({
                "month": month_str,
                "value": row["total_spend"]
            })
            
            metrics["revenue"].append({
                "month": month_str,
                "value": row["total_revenue"]
            })
            
            # Add consolidated changes for easier trend analysis
            metrics["changes"].append({
                "month": month_str,
                "conversion_rate": row["conversion_rate_vs_prev_month"],
                "roi": row["roi_vs_prev_month"],
                "acquisition_cost": row["acquisition_cost_vs_prev_month"],
                "ctr": row["ctr_vs_prev_month"]
            })
        
        return {"metrics": metrics}
    except Exception as e:
        logger.error(f"Error getting monthly campaign metrics: {str(e)}")
        return {"metrics": {}}

def get_campaign_duration_analysis(company_id: str, dimension: str = "channel") -> Dict[str, Any]:
    """
    Get campaign duration impact analysis with heatmap data.
    
    Args:
        company_id: Company name to get campaign duration analysis for
        dimension: Dimension to analyze (channel, goal, company)
        
    Returns:
        Dict[str, Any]: Campaign duration analysis for the company with heatmap data
    """
    # Map dimension parameter to dimension value in the model
    dimension_value = {
        "audience": "Target_Audience",  # For raw campaign data
        "channel": "Channel",
        "goal": "Goal",
        "company": "Company"
    }.get(dimension.lower(), "Channel")
    
    # Get optimal durations
    optimal_query = """
    SELECT 
        dimension,
        category,
        optimal_duration_bucket,
        optimal_min_duration,
        optimal_max_duration,
        optimal_roi,
        optimal_conversion_rate,
        optimal_roi_per_day,
        optimal_duration_range
    FROM campaign_duration_quarter_analysis
    WHERE analysis_type = 'optimal_durations'
    AND Company = ?
    AND dimension = ?
    ORDER BY optimal_roi DESC
    """
    
    # Get heatmap data
    heatmap_query = """
    SELECT 
        analysis_type,
        dimension,
        category,
        duration_bucket_num,
        optimal_min_duration as min_duration,
        optimal_max_duration as max_duration,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        avg_ctr,
        avg_roi_per_day,
        roi_impact,
        roi_impact_pct
    FROM campaign_duration_quarter_analysis
    WHERE analysis_type LIKE ?
    AND Company = ?
    AND dimension = ?
    ORDER BY category, duration_bucket_num
    """
    
    
    try:
        # Get optimal durations
        optimal_results = execute_query(optimal_query, [company_id, dimension_value])
        
        # Get heatmap data - use the appropriate analysis_type based on dimension
        heatmap_type = f"{dimension.lower()}_duration_heatmap"
        heatmap_results = execute_query(heatmap_query, [heatmap_type, company_id, dimension_value])
        
        # Format heatmap data for visualization
        categories = set()
        duration_buckets = set()
        
        for row in heatmap_results:
            categories.add(row.get('category'))
            # Create a duration bucket label based on min and max duration
            duration_bucket = f"{row.get('min_duration', 0)}-{row.get('max_duration', 0)} days"
            row['duration_bucket'] = duration_bucket
            duration_buckets.add(duration_bucket)
        
        # Create a comprehensive analysis response without raw data
        analysis = {
            "optimal": optimal_results,
            "heatmap": heatmap_results,
            "summary": {
                "categories": list(categories),
                "duration_buckets": sorted(list(duration_buckets), 
                                          key=lambda x: next((r['duration_bucket_num'] for r in heatmap_results if r.get('duration_bucket') == x), 0)),
                "count": 0,
                "avg_duration": 0,
                "min_duration": 0,
                "max_duration": 0
            }
        }
        
        return analysis
    except Exception as e:
        logger.error(f"Error getting campaign duration analysis: {str(e)}")
        return {"data": []}


