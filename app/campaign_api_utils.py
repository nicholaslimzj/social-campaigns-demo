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

def get_campaign_duration_analysis(company_id: str, dimension: str = "company") -> Dict[str, Any]:
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
        "channel": "Channel",
        "goal": "Goal",
        "company": "Company"
    }.get(dimension.lower(), "Company")
    
    try:
        # First, check if the company exists in the table
        company_check_query = "SELECT DISTINCT Company FROM campaign_duration_quarter_analysis"
        companies = execute_query(company_check_query, [])
        company_list = [c.get('Company') for c in companies]
        
        # Use fallback company if needed
        if company_id not in company_list and company_list:
            company_id = company_list[0]
        
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
        
        # Execute queries
        optimal_results = execute_query(optimal_query, [company_id, dimension_value])
        heatmap_type = f"{dimension.lower()}_duration_heatmap"
        heatmap_results = execute_query(heatmap_query, [heatmap_type, company_id, dimension_value])
        
        # If no results, try with an alternative dimension
        if not optimal_results and not heatmap_results:
            available_dimensions_query = """
            SELECT DISTINCT dimension FROM campaign_duration_quarter_analysis
            WHERE Company = ? AND analysis_type = 'optimal_durations'
            """
            available_dimensions = execute_query(available_dimensions_query, [company_id])
            dimension_list = [d.get('dimension') for d in available_dimensions]
            
            if dimension_list:
                alt_dimension = dimension_list[0]
                optimal_results = execute_query(optimal_query, [company_id, alt_dimension])
                alt_heatmap_type = f"{alt_dimension.lower()}_duration_heatmap"
                heatmap_results = execute_query(heatmap_query, [alt_heatmap_type, company_id, alt_dimension])
        
        # Format heatmap data for visualization
        categories = set()
        duration_buckets = set()
        
        for row in heatmap_results:
            categories.add(row.get('category'))
            duration_bucket = f"{row.get('min_duration', 0)}-{row.get('max_duration', 0)} days"
            row['duration_bucket'] = duration_bucket
            duration_buckets.add(duration_bucket)
        
        # Transform data into the format expected by the frontend
        dimension_values = []
        
        # Group by category (dimension value)
        category_groups = {}
        for row in heatmap_results:
            category = row.get('category')
            if category not in category_groups:
                category_groups[category] = []
            category_groups[category].append(row)
        
        # Format each dimension value with its metrics
        for category, rows in category_groups.items():
            metrics = []
            for row in rows:
                metrics.append({
                    "duration_bucket": row.get('duration_bucket'),
                    "avg_roi": row.get('avg_roi'),
                    "avg_conversion_rate": row.get('avg_conversion_rate'),
                    "avg_acquisition_cost": row.get('avg_acquisition_cost'),
                    "avg_ctr": row.get('avg_ctr'),
                    "campaign_count": row.get('campaign_count'),
                    # Determine if this is the optimal duration for this category
                    "optimal_flag": False,  # Will be set below
                    "performance_index": row.get('roi_impact', 0) + 1  # Use ROI impact as performance index
                })
            
            # Find the optimal duration for this category from optimal_results
            optimal_duration = ""
            roi_impact = 0
            for opt in optimal_results:
                if opt.get('category') == category:
                    optimal_min = opt.get('optimal_min_duration')
                    optimal_max = opt.get('optimal_max_duration')
                    optimal_duration = f"{optimal_min}-{optimal_max} days"
                    roi_impact = opt.get('roi_impact', 0)
                    
                    # Mark the optimal duration in metrics
                    for metric in metrics:
                        if metric['duration_bucket'] == optimal_duration:
                            metric['optimal_flag'] = True
            
            dimension_values.append({
                "dimension_value": category,
                "metrics": metrics,
                "optimal_duration": optimal_duration,
                "roi_impact": roi_impact
            })
        
        # Calculate overall optimal duration and ROI impact
        overall_optimal_duration = ""
        overall_roi_impact = 0
        if optimal_results:
            # Use the first result with the highest ROI as overall optimal
            sorted_optimal = sorted(optimal_results, key=lambda x: x.get('optimal_roi', 0), reverse=True)
            if sorted_optimal:
                opt = sorted_optimal[0]
                overall_optimal_duration = f"{opt.get('optimal_min_duration')}-{opt.get('optimal_max_duration')} days"
                overall_roi_impact = opt.get('roi_impact', 0)
        
        # Create response in the format expected by the frontend
        return {
            "company": company_id,
            "dimension": dimension_value,
            "dimension_values": dimension_values,
            "overall_optimal_duration": overall_optimal_duration,
            "overall_roi_impact": overall_roi_impact
        }
    except Exception as e:
        # Return empty results with error information
        return {
            "optimal": [],
            "heatmap": [],
            "summary": {
                "categories": [],
                "duration_buckets": [],
                "count": 0,
                "avg_duration": 0,
                "min_duration": 0,
                "max_duration": 0,
                "error": str(e)
            }
        }


def get_campaign_clusters(company_id: str, limit: int = 5) -> Dict[str, Any]:
    """
    Get high-performing campaign clusters for a company, separated by ROI and conversion rate.
    
    Args:
        company_id: Company ID to get clusters for
        limit: Maximum number of clusters to return for each category (default: 5)
        
    Returns:
        Dict with high_roi and high_conversion campaign clusters
    """
    
    # Query to get high ROI campaign clusters
    high_roi_query = """
    SELECT 
        goal,
        segment,
        channel,
        duration_bucket,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        avg_ctr,
        min_duration,
        max_duration,
        avg_duration,
        roi_vs_company,
        conversion_rate_vs_company,
        composite_score,
        is_optimal_duration,
        optimal_duration_range
    FROM campaign_quarter_clusters
    WHERE Company = ?
    AND is_winning_combination = TRUE
    ORDER BY avg_roi DESC
    LIMIT ?
    """
    
    # Query to get high conversion rate campaign clusters
    high_conversion_query = """
    SELECT 
        goal,
        segment,
        channel,
        duration_bucket,
        campaign_count,
        avg_conversion_rate,
        avg_roi,
        avg_acquisition_cost,
        avg_ctr,
        min_duration,
        max_duration,
        avg_duration,
        roi_vs_company,
        conversion_rate_vs_company,
        composite_score,
        is_optimal_duration,
        optimal_duration_range
    FROM campaign_quarter_clusters
    WHERE Company = ?
    AND is_winning_combination = TRUE
    ORDER BY avg_conversion_rate DESC
    LIMIT ?
    """
    
    try:
        # Get high ROI campaign clusters
        high_roi_clusters = execute_query(high_roi_query, [company_id, limit])
        
        # Get high conversion rate campaign clusters
        high_conversion_clusters = execute_query(high_conversion_query, [company_id, limit])
        
        # Process the clusters to create a simplified response format
        high_roi_results = []
        high_conversion_results = []
        
        # Process high ROI clusters
        for cluster in high_roi_clusters:
            # Determine recommended action
            recommended_action = 'Increase budget allocation' if cluster.get('roi_vs_company', 0) > 0.2 else 'Maintain current strategy'
            
            high_roi_results.append({
                'goal': cluster.get('goal'),
                'segment': cluster.get('segment'),
                'channel': cluster.get('channel'),
                'duration_bucket': cluster.get('duration_bucket'),
                'campaign_count': cluster.get('campaign_count'),
                'conversion_rate': cluster.get('avg_conversion_rate'),
                'roi': cluster.get('avg_roi'),
                'acquisition_cost': cluster.get('avg_acquisition_cost'),
                'ctr': cluster.get('avg_ctr'),
                'min_duration': cluster.get('min_duration'),
                'max_duration': cluster.get('max_duration'),
                'avg_duration': cluster.get('avg_duration'),
                'roi_vs_company': cluster.get('roi_vs_company'),
                'conversion_rate_vs_company': cluster.get('conversion_rate_vs_company'),
                'performance_score': cluster.get('composite_score'),
                'is_optimal_duration': cluster.get('is_optimal_duration'),
                'optimal_duration_range': cluster.get('optimal_duration_range'),
                'recommended_action': recommended_action
            })
        
        # Process high conversion rate clusters
        for cluster in high_conversion_clusters:
            # Determine recommended action
            recommended_action = 'Optimize for conversions' if cluster.get('conversion_rate_vs_company', 0) > 0.2 else 'Maintain current strategy'
            
            high_conversion_results.append({
                'goal': cluster.get('goal'),
                'segment': cluster.get('segment'),
                'channel': cluster.get('channel'),
                'duration_bucket': cluster.get('duration_bucket'),
                'campaign_count': cluster.get('campaign_count'),
                'conversion_rate': cluster.get('avg_conversion_rate'),
                'roi': cluster.get('avg_roi'),
                'acquisition_cost': cluster.get('avg_acquisition_cost'),
                'ctr': cluster.get('avg_ctr'),
                'min_duration': cluster.get('min_duration'),
                'max_duration': cluster.get('max_duration'),
                'avg_duration': cluster.get('avg_duration'),
                'roi_vs_company': cluster.get('roi_vs_company'),
                'conversion_rate_vs_company': cluster.get('conversion_rate_vs_company'),
                'performance_score': cluster.get('composite_score'),
                'is_optimal_duration': cluster.get('is_optimal_duration'),
                'optimal_duration_range': cluster.get('optimal_duration_range'),
                'recommended_action': recommended_action
            })
        
        return {
            'company': company_id,
            'high_roi': high_roi_results,
            'high_conversion': high_conversion_results
        }
    
    except Exception as e:
        logger.error(f"Error getting campaign clusters: {str(e)}")
        return {
            'company': company_id,
            'high_roi': [],
            'high_conversion': [],
            'error': str(e)
        }

def get_campaign_future_forecast(company_id: str, metric: str = 'revenue') -> Dict[str, Any]:
    """
    Get campaign future forecast data for a specific company.
    
    Args:
        company_id: Company name to get forecast data for
        metric: Metric to forecast (revenue, roi, conversion_rate, acquisition_cost, ctr)
                 'revenue' is calculated as ROI * Acquisition Cost
        
    Returns:
        Dict[str, Any]: Campaign forecast data for the company
    """
    # Map metric parameter to column name in the model
    metric_column = {
        "roi": "forecasted_roi",
        "conversion_rate": "forecasted_conversion_rate",
        "acquisition_cost": "forecasted_acquisition_cost",
        "ctr": "forecasted_ctr"
    }.get(metric.lower(), "forecasted_roi")
    
    # Base query to get historical and forecasted data
    query = """
    SELECT 
        month_id,
        month,
        year,
        is_forecast,
        conversion_rate,
        conversion_rate_forecast,
        conversion_rate_lower_bound,
        conversion_rate_upper_bound,
        roi,
        roi_forecast,
        roi_lower_bound,
        roi_upper_bound,
        acquisition_cost,
        acquisition_cost_forecast,
        acquisition_cost_lower_bound,
        acquisition_cost_upper_bound,
        ctr,
        ctr_forecast,
        ctr_lower_bound,
        ctr_upper_bound
    FROM campaign_future_forecast
    WHERE Company = ?
    ORDER BY month_id
    """
    
    try:
        # Execute query
        results = execute_query(query, [company_id])
        
        if not results:
            # Try with a fallback company if no results
            company_check_query = "SELECT DISTINCT Company FROM campaign_future_forecast LIMIT 1"
            companies = execute_query(company_check_query, [])
            
            if companies and companies[0].get('Company'):
                fallback_company = companies[0].get('Company')
                results = execute_query(query, [fallback_company])
        
        # Format the results
        historical_data = []
        forecast_data = []
        confidence_intervals = []
        
        for row in results:
            date_str = f"{int(row['year'])}-{int(row['month']):02d}"
            
            # Get the is_forecast flag from the row
            is_forecast_value = row.get('is_forecast')
            
            # Get the appropriate values based on the metric
            if metric == 'revenue':
                # Calculate revenue as ROI * Acquisition Cost
                roi_hist = row.get('roi')
                acq_cost_hist = row.get('acquisition_cost')
                roi_forecast = row.get('roi_forecast')
                acq_cost_forecast = row.get('acquisition_cost_forecast')
                
                # Calculate historical and forecasted revenue
                historical_value = None
                forecast_value = None
                if roi_hist is not None and acq_cost_hist is not None:
                    historical_value = roi_hist * acq_cost_hist
                if roi_forecast is not None and acq_cost_forecast is not None:
                    forecast_value = roi_forecast * acq_cost_forecast
                    
                # Calculate lower and upper bounds for revenue
                roi_lower = row.get('roi_lower_bound')
                roi_upper = row.get('roi_upper_bound')
                acq_cost_lower = row.get('acquisition_cost_lower_bound')
                acq_cost_upper = row.get('acquisition_cost_upper_bound')
                
                # Use conservative estimates for bounds (lower ROI * lower cost, upper ROI * upper cost)
                lower_bound = None
                upper_bound = None
                if roi_lower is not None and acq_cost_lower is not None:
                    lower_bound = roi_lower * acq_cost_lower
                if roi_upper is not None and acq_cost_upper is not None:
                    upper_bound = roi_upper * acq_cost_upper
            elif metric == 'roi':
                historical_value = row.get('roi')
                forecast_value = row.get('roi_forecast')
                lower_bound = row.get('roi_lower_bound')
                upper_bound = row.get('roi_upper_bound')
            elif metric == 'conversion_rate':
                historical_value = row.get('conversion_rate')
                forecast_value = row.get('conversion_rate_forecast')
                lower_bound = row.get('conversion_rate_lower_bound')
                upper_bound = row.get('conversion_rate_upper_bound')
            elif metric == 'acquisition_cost':
                historical_value = row.get('acquisition_cost')
                forecast_value = row.get('acquisition_cost_forecast')
                lower_bound = row.get('acquisition_cost_lower_bound')
                upper_bound = row.get('acquisition_cost_upper_bound')
            elif metric == 'ctr':
                historical_value = row.get('ctr')
                forecast_value = row.get('ctr_forecast')
                lower_bound = row.get('ctr_lower_bound')
                upper_bound = row.get('ctr_upper_bound')
            else:
                historical_value = None
                forecast_value = None
                lower_bound = None
                upper_bound = None
            
            # Add to appropriate arrays based on is_forecast flag
            if is_forecast_value == 0 and historical_value is not None:
                historical_data.append({
                    "date": date_str,
                    "value": historical_value
                })
            elif is_forecast_value == 1 and forecast_value is not None:
                forecast_data.append({
                    "date": date_str,
                    "value": forecast_value
                })
                
                # Add confidence intervals if available
                if lower_bound is not None and upper_bound is not None:
                    confidence_intervals.append({
                        "date": date_str,
                        "lower": lower_bound,
                        "upper": upper_bound,
                        "confidence_level": 0.9  # Default confidence level
                    })
        
        return {
            "company": company_id,
            "metric": metric,
            "historical_data": historical_data,
            "forecast_data": forecast_data,
            "confidence_intervals": confidence_intervals,
            "metadata": {
                "forecast_periods": len(forecast_data),
                "historical_periods": len(historical_data),
                "last_historical_date": historical_data[-1]['date'] if historical_data else None,
                "first_forecast_date": forecast_data[0]['date'] if forecast_data else None
            }
        }
    except Exception as e:
        logger.error(f"Error getting campaign future forecast: {str(e)}")
        return {
            "company": company_id,
            "metric": metric,
            "historical_data": [],
            "forecast_data": [],
            "confidence_intervals": [],
            "metadata": {
                "forecast_periods": 0,
                "historical_periods": 0,
                "error": str(e)
            }
        }
