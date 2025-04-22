"""
Audience API utilities for the Meta Demo project.

This module contains utility functions for audience-related API endpoints.
These functions interact with the DuckDB database to retrieve audience data.
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

def get_company_audiences(company_id: str, include_metrics: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get list of target audiences for a specific company.
    
    Args:
        company_id: Company name to get audiences for
        include_metrics: Whether to include metrics with each audience
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: List of audiences for the company
    """
    # Use audience_quarter_anomalies table which has the latest quarterly data
    # and maintains the hierarchical dimension structure with Company as primary dimension
    if include_metrics:
        query = """
        WITH company_metrics AS (
            SELECT 
                Target_Audience as audience_id,
                campaign_count,
                avg_conversion_rate,
                avg_roi,
                avg_acquisition_cost,
                quarterly_ctr as avg_ctr,
                has_anomaly,
                CASE
                    WHEN has_anomaly = TRUE THEN anomaly_description
                    ELSE NULL
                END as anomaly_description
            FROM audience_quarter_anomalies
            WHERE Company = ?
        ),
        -- Get industry benchmarks for all metrics in one CTE
        industry_benchmarks AS (
            SELECT 
                dimension,
                metric,
                entity as audience_id,
                metric_value,
                metric_rank,
                total_entities,
                CAST(metric_rank AS FLOAT) / total_entities as percentile_rank
            FROM dimensions_quarter_performance_rankings
            WHERE dimension = 'audience'
            AND metric IN ('roi', 'conversion_rate', 'acquisition_cost', 'ctr')
        ),
        -- Pivot the industry benchmarks to get one row per audience
        industry_metrics AS (
            SELECT
                audience_id,
                MAX(CASE WHEN metric = 'roi' THEN metric_value END) as industry_roi,
                MAX(CASE WHEN metric = 'roi' THEN percentile_rank END) as roi_percentile,
                MAX(CASE WHEN metric = 'conversion_rate' THEN metric_value END) as industry_conversion_rate,
                MAX(CASE WHEN metric = 'conversion_rate' THEN percentile_rank END) as conversion_percentile,
                MAX(CASE WHEN metric = 'acquisition_cost' THEN metric_value END) as industry_acquisition_cost,
                MAX(CASE WHEN metric = 'acquisition_cost' THEN percentile_rank END) as acquisition_percentile,
                MAX(CASE WHEN metric = 'ctr' THEN metric_value END) as industry_ctr,
                MAX(CASE WHEN metric = 'ctr' THEN percentile_rank END) as ctr_percentile
            FROM industry_benchmarks
            GROUP BY audience_id
        )
        -- Join company metrics with industry benchmarks
        SELECT 
            c.audience_id,
            c.campaign_count,
            c.avg_conversion_rate,
            c.avg_roi,
            c.avg_acquisition_cost,
            c.avg_ctr,
            c.has_anomaly,
            c.anomaly_description,
            -- Industry benchmarks
            i.industry_conversion_rate,
            i.industry_roi,
            i.industry_acquisition_cost,
            i.industry_ctr,
            -- Percentile rankings
            i.conversion_percentile,
            i.roi_percentile,
            i.acquisition_percentile,
            i.ctr_percentile,
            -- Performance comparisons
            CASE 
                WHEN c.avg_conversion_rate > i.industry_conversion_rate THEN 'above_average'
                WHEN c.avg_conversion_rate < i.industry_conversion_rate THEN 'below_average'
                ELSE 'average'
            END as conversion_performance,
            CASE 
                WHEN c.avg_roi > i.industry_roi THEN 'above_average'
                WHEN c.avg_roi < i.industry_roi THEN 'below_average'
                ELSE 'average'
            END as roi_performance,
            CASE 
                WHEN c.avg_acquisition_cost < i.industry_acquisition_cost THEN 'above_average'
                WHEN c.avg_acquisition_cost > i.industry_acquisition_cost THEN 'below_average'
                ELSE 'average'
            END as acquisition_performance,
            CASE 
                WHEN c.avg_ctr > i.industry_ctr THEN 'above_average'
                WHEN c.avg_ctr < i.industry_ctr THEN 'below_average'
                ELSE 'average'
            END as ctr_performance
        FROM company_metrics c
        LEFT JOIN industry_metrics i ON c.audience_id = i.audience_id
        ORDER BY c.avg_roi DESC
        """
    else:
        query = """
        SELECT 
            Target_Audience as audience_id,
            campaign_count
        FROM audience_quarter_anomalies
        WHERE Company = ?
        ORDER BY Target_Audience
        """
    
    try:
        results = execute_query(query, [company_id])
        
        # If include_metrics is True, enhance the results with performance tier
        if include_metrics:
            for result in results:
                # Calculate overall performance tier based on benchmark comparisons
                above_average_count = sum(1 for perf in [
                    result.get('conversion_performance', ''),
                    result.get('roi_performance', ''),
                    result.get('acquisition_performance', ''),
                    result.get('ctr_performance', '')
                ] if perf == 'above_average')
                
                if above_average_count >= 3:
                    result['overall_performance'] = 'excellent'
                elif above_average_count == 2:
                    result['overall_performance'] = 'good'
                elif above_average_count == 1:
                    result['overall_performance'] = 'average'
                else:
                    result['overall_performance'] = 'below_average'
                
                # Add structured benchmark data
                result['industry_benchmarks'] = {
                    'conversion_rate': result.pop('industry_conversion_rate', None),
                    'roi': result.pop('industry_roi', None),
                    'acquisition_cost': result.pop('industry_acquisition_cost', None),
                    'ctr': result.pop('industry_ctr', None)
                }
                
                result['percentiles'] = {
                    'conversion_rate': result.pop('conversion_percentile', None),
                    'roi': result.pop('roi_percentile', None),
                    'acquisition_cost': result.pop('acquisition_percentile', None),
                    'ctr': result.pop('ctr_percentile', None)
                }
                
                result['performance'] = {
                    'conversion_rate': result.pop('conversion_performance', None),
                    'roi': result.pop('roi_performance', None),
                    'acquisition_cost': result.pop('acquisition_performance', None),
                    'ctr': result.pop('ctr_performance', None),
                    'overall': result.pop('overall_performance', None)
                }
                
                # Add anomaly information
                result['anomaly'] = {
                    'has_anomaly': result.get('has_anomaly', False),
                    'description': result.pop('anomaly_description', None)
                }
        
        return {"audiences": results}
    except Exception as e:
        logger.error(f"Error getting company audiences: {str(e)}")
        return {"audiences": []}

def get_monthly_audience_metrics(company_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get monthly metrics for audiences of a specific company.
    
    Args:
        company_id: Company name to get monthly audience metrics for
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Monthly audience metrics for the company
    """
    query = """
    SELECT 
        Target_Audience as audience_id,
        month,
        avg_conversion_rate as conversion_rate,
        avg_roi as roi,
        avg_acquisition_cost as acquisition_cost,
        monthly_ctr as ctr,
        total_clicks as clicks,
        total_impressions as impressions,
        campaign_count,
        total_spend,
        total_revenue,
        roi_vs_prev_month,
        conversion_rate_vs_prev_month,
        acquisition_cost_vs_prev_month,
        ctr_vs_prev_month,
        audience_share_clicks,
        response_rate,
        efficiency_ratio
    FROM audience_monthly_metrics
    WHERE Company = ?
    ORDER BY month ASC, avg_roi DESC
    """
    
    try:
        results = execute_query(query, [company_id])
        
        # Group by audience_id
        audiences = {}
        for row in results:
            audience_id = row["audience_id"]
            if audience_id not in audiences:
                audiences[audience_id] = {
                    "audience_id": audience_id,
                    "monthly_metrics": []
                }
            
            # Add monthly metrics with enhanced data from the pre-computed model
            audiences[audience_id]["monthly_metrics"].append({
                "month": row["month"],
                "conversion_rate": row["conversion_rate"],
                "roi": row["roi"],
                "acquisition_cost": row["acquisition_cost"],
                "ctr": row["ctr"],
                "clicks": row["clicks"],
                "impressions": row["impressions"],
                "campaign_count": row["campaign_count"],
                "total_spend": row["total_spend"],
                "total_revenue": row["total_revenue"],
                "changes": {
                    "roi": row["roi_vs_prev_month"],
                    "conversion_rate": row["conversion_rate_vs_prev_month"],
                    "acquisition_cost": row["acquisition_cost_vs_prev_month"],
                    "ctr": row["ctr_vs_prev_month"]
                },
                "audience_share": row["audience_share_clicks"],
                "response_rate": row["response_rate"],
                "efficiency_ratio": row["efficiency_ratio"]
            })
        
        return {"audiences": list(audiences.values())}
    except Exception as e:
        logger.error(f"Error getting monthly audience metrics: {str(e)}")
        return {"audiences": []}

def get_audience_performance_matrix(company_id: str, dimension_type: str = "goal") -> Dict[str, List[Dict[str, Any]]]:
    """
    Get audience performance matrix data.
    
    Args:
        company_id: Company name to get performance matrix for
        dimension_type: Type of dimension to analyze (goal, location, language)
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Performance matrix for the company
    """
    # Use the audience_quarter_performance_matrix table which has pre-calculated performance metrics
    query = """
    SELECT 
        Target_Audience as audience_id,
        dimension_type,
        dimension_value,
        avg_roi,
        avg_conversion_rate,
        avg_acquisition_cost,
        avg_ctr
    FROM audience_quarter_performance_matrix
    WHERE Company = ?
    AND dimension_type = ?
    ORDER BY Target_Audience, dimension_value
    """
    
    try:
        results = execute_query(query, [company_id, dimension_type])
        
        if not results:
            return {"matrix": []}
            
        # Group by audience_id
        matrix = {}
        for row in results:
            audience_id = row["audience_id"]
            
            if audience_id not in matrix:
                matrix[audience_id] = {
                    "audience_id": audience_id,
                    "dimensions": []
                }
            
            matrix[audience_id]["dimensions"].append({
                "dimension_value": row["dimension_value"],
                "metrics": {
                    "roi": row["avg_roi"],
                    "conversion_rate": row["avg_conversion_rate"],
                    "acquisition_cost": row["avg_acquisition_cost"],
                    "ctr": row["avg_ctr"]
                }
            })
        
        return {"matrix": list(matrix.values())}
    except Exception as e:
        logger.error(f"Error getting audience performance matrix: {str(e)}")
        return {"matrix": []}

def get_audience_clusters(company_id: str, limit: int = 5) -> Dict[str, Any]:
    """
    Get high-performing audience clusters for a specific company, separated by ROI and conversion rate.
    
    Args:
        company_id: Company name to get audience clusters for
        limit: Maximum number of clusters to return for each category (default: 5)
        
    Returns:
        Dict with high_roi and high_conversion audience lists
    """
    
    # Query to get high ROI audience clusters
    high_roi_query = """
    SELECT 
        Target_Audience as audience_id,
        Location as location,
        channel,
        goal,
        campaign_count,
        avg_conversion_rate as conversion_rate,
        avg_roi as roi,
        avg_acquisition_cost as acquisition_cost,
        avg_ctr as ctr,
        total_spend,
        total_revenue,
        composite_performance_score as performance_score,
        performance_tier,
        recommended_action,
        is_high_performing_cluster,
        avg_audience_conversion_rate,
        avg_audience_roi,
        avg_audience_ctr,
        avg_audience_acquisition_cost
    FROM audience_quarter_clusters
    WHERE Company = ?
    AND is_high_performing_cluster = TRUE
    ORDER BY avg_roi DESC
    LIMIT ?
    """
    
    # Query to get high conversion rate audience clusters
    high_conversion_query = """
    SELECT 
        Target_Audience as audience_id,
        Location as location,
        channel,
        goal,
        campaign_count,
        avg_conversion_rate as conversion_rate,
        avg_roi as roi,
        avg_acquisition_cost as acquisition_cost,
        avg_ctr as ctr,
        total_spend,
        total_revenue,
        composite_performance_score as performance_score,
        performance_tier,
        recommended_action,
        is_high_performing_cluster,
        avg_audience_conversion_rate,
        avg_audience_roi,
        avg_audience_ctr,
        avg_audience_acquisition_cost
    FROM audience_quarter_clusters
    WHERE Company = ?
    AND is_high_performing_cluster = TRUE
    ORDER BY avg_conversion_rate DESC
    LIMIT ?
    """
    
    
    try:
        
        # Get high ROI audience clusters
        high_roi_clusters = execute_query(high_roi_query, [company_id, limit])
        
        # Get high conversion rate audience clusters
        high_conversion_clusters = execute_query(high_conversion_query, [company_id, limit])
        
        # Process the clusters to create a simplified response format
        high_roi_results = []
        high_conversion_results = []
        
        # Process high ROI clusters
        for cluster in high_roi_clusters:
            high_roi_results.append({
                'audience_id': cluster.get('audience_id'),
                'location': cluster.get('location'),
                'channel': cluster.get('channel'),
                'goal': cluster.get('goal'),
                'campaign_count': cluster.get('campaign_count'),
                'conversion_rate': cluster.get('conversion_rate'),
                'roi': cluster.get('roi'),
                'acquisition_cost': cluster.get('acquisition_cost'),
                'ctr': cluster.get('ctr'),
                'total_spend': cluster.get('total_spend'),
                'total_revenue': cluster.get('total_revenue'),
                'performance_score': cluster.get('performance_score'),
                'performance_tier': cluster.get('performance_tier'),
                'recommended_action': cluster.get('recommended_action'),
                'avg_audience_conversion_rate': cluster.get('avg_audience_conversion_rate'),
                'avg_audience_roi': cluster.get('avg_audience_roi'),
                'avg_audience_ctr': cluster.get('avg_audience_ctr'),
                'avg_audience_acquisition_cost': cluster.get('avg_audience_acquisition_cost')
            })
        
        # Process high conversion rate clusters
        for cluster in high_conversion_clusters:
            high_conversion_results.append({
                'audience_id': cluster.get('audience_id'),
                'location': cluster.get('location'),
                'channel': cluster.get('channel'),
                'goal': cluster.get('goal'),
                'campaign_count': cluster.get('campaign_count'),
                'conversion_rate': cluster.get('conversion_rate'),
                'roi': cluster.get('roi'),
                'acquisition_cost': cluster.get('acquisition_cost'),
                'ctr': cluster.get('ctr'),
                'total_spend': cluster.get('total_spend'),
                'total_revenue': cluster.get('total_revenue'),
                'performance_score': cluster.get('performance_score'),
                'performance_tier': cluster.get('performance_tier'),
                'recommended_action': cluster.get('recommended_action'),
                'avg_audience_conversion_rate': cluster.get('avg_audience_conversion_rate'),
                'avg_audience_roi': cluster.get('avg_audience_roi'),
                'avg_audience_ctr': cluster.get('avg_audience_ctr'),
                'avg_audience_acquisition_cost': cluster.get('avg_audience_acquisition_cost')
            })
        
        return {
            "high_roi": high_roi_results,
            "high_conversion": high_conversion_results
        }
    except Exception as e:
        logger.error(f"Error getting audience clusters: {str(e)}")
        return {"high_roi": [], "high_conversion": []}

def get_audience_benchmarks(company_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get audience industry benchmarks.
    
    Args:
        company_id: Company name to get audience benchmarks for
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Audience benchmarks for the company
    """
    # Query that combines company-specific metrics with industry benchmarks
    query = """
    WITH company_metrics AS (
        -- Get company-specific metrics from audience_quarter_anomalies
        SELECT 
            Target_Audience as audience_id,
            avg_conversion_rate as company_conversion_rate,
            avg_roi as company_roi,
            avg_acquisition_cost as company_acquisition_cost,
            quarterly_ctr as company_ctr,
            has_anomaly,
            anomaly_description
        FROM audience_quarter_anomalies
        WHERE Company = ?
    ),
    -- Get industry benchmarks for all metrics in one CTE
    industry_benchmarks AS (
        SELECT 
            dimension,
            metric,
            entity as audience_id,
            metric_value,
            metric_rank,
            total_entities,
            CAST(metric_rank AS FLOAT) / total_entities as percentile_rank
        FROM dimensions_quarter_performance_rankings
        WHERE dimension = 'audience'
        AND metric IN ('roi', 'conversion_rate', 'acquisition_cost', 'ctr')
    ),
    -- Pivot the industry benchmarks to get one row per audience
    industry_metrics AS (
        SELECT
            audience_id,
            MAX(CASE WHEN metric = 'roi' THEN metric_value END) as industry_roi,
            MAX(CASE WHEN metric = 'roi' THEN percentile_rank END) as roi_percentile,
            MAX(CASE WHEN metric = 'conversion_rate' THEN metric_value END) as industry_conversion_rate,
            MAX(CASE WHEN metric = 'conversion_rate' THEN percentile_rank END) as conversion_percentile,
            MAX(CASE WHEN metric = 'acquisition_cost' THEN metric_value END) as industry_acquisition_cost,
            MAX(CASE WHEN metric = 'acquisition_cost' THEN percentile_rank END) as acquisition_percentile,
            MAX(CASE WHEN metric = 'ctr' THEN metric_value END) as industry_ctr,
            MAX(CASE WHEN metric = 'ctr' THEN percentile_rank END) as ctr_percentile
        FROM industry_benchmarks
        GROUP BY audience_id
    )
    -- Join company metrics with industry benchmarks
    SELECT 
        c.audience_id,
        -- Company metrics
        c.company_conversion_rate,
        c.company_roi,
        c.company_acquisition_cost,
        c.company_ctr,
        c.has_anomaly,
        c.anomaly_description,
        -- Industry benchmarks
        i.industry_conversion_rate,
        i.industry_roi,
        i.industry_acquisition_cost,
        i.industry_ctr,
        -- Percentile rankings
        i.conversion_percentile,
        i.roi_percentile,
        i.acquisition_percentile,
        i.ctr_percentile,
        -- Performance comparisons
        CASE 
            WHEN c.company_conversion_rate > i.industry_conversion_rate THEN 'above_average'
            WHEN c.company_conversion_rate < i.industry_conversion_rate THEN 'below_average'
            ELSE 'average'
        END as conversion_performance,
        CASE 
            WHEN c.company_roi > i.industry_roi THEN 'above_average'
            WHEN c.company_roi < i.industry_roi THEN 'below_average'
            ELSE 'average'
        END as roi_performance,
        CASE 
            WHEN c.company_acquisition_cost < i.industry_acquisition_cost THEN 'above_average'
            WHEN c.company_acquisition_cost > i.industry_acquisition_cost THEN 'below_average'
            ELSE 'average'
        END as acquisition_performance,
        CASE 
            WHEN c.company_ctr > i.industry_ctr THEN 'above_average'
            WHEN c.company_ctr < i.industry_ctr THEN 'below_average'
            ELSE 'average'
        END as ctr_performance
    FROM company_metrics c
    LEFT JOIN industry_metrics i ON c.audience_id = i.audience_id
    ORDER BY c.audience_id
    """
    
    try:
        results = execute_query(query, [company_id])
        
        # Just add a simple overall performance indicator
        for result in results:
            # Count the number of above_average performances
            above_average_count = sum(1 for perf in [
                result.get('conversion_performance', ''),
                result.get('roi_performance', ''),
                result.get('acquisition_performance', ''),
                result.get('ctr_performance', '')
            ] if perf == 'above_average')
            
            # Determine overall performance tier
            if above_average_count >= 3:
                result['overall_performance'] = 'excellent'
            elif above_average_count == 2:
                result['overall_performance'] = 'good'
            elif above_average_count == 1:
                result['overall_performance'] = 'average'
            else:
                result['overall_performance'] = 'needs_improvement'
        
        return {"audiences": results}
    except Exception as e:
        logger.error(f"Error getting audience benchmarks: {str(e)}")
        return {"audiences": []}

def get_audience_anomalies(company_id: str, threshold: float = 2.0) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get anomalies for target audiences of a specific company.
    
    Args:
        company_id: Company name to get audience anomalies for
        threshold: Z-score threshold for anomaly detection
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Audience anomalies for the company
    """
    # Query to get anomalies from the audience_quarter_anomalies table
    query = """
    SELECT 
        Target_Audience as audience_id,
        CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN 'conversion_rate'
            WHEN roi_anomaly = 'anomaly' THEN 'roi'
            WHEN acquisition_cost_anomaly = 'anomaly' THEN 'acquisition_cost'
            WHEN ctr_anomaly = 'anomaly' THEN 'ctr'
            WHEN clicks_anomaly = 'anomaly' THEN 'clicks'
            WHEN impressions_anomaly = 'anomaly' THEN 'impressions'
            WHEN spend_anomaly = 'anomaly' THEN 'spend'
            WHEN revenue_anomaly = 'anomaly' THEN 'revenue'
            WHEN campaign_count_anomaly = 'anomaly' THEN 'campaign_count'
        END as metric,
        CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN avg_conversion_rate
            WHEN roi_anomaly = 'anomaly' THEN avg_roi
            WHEN acquisition_cost_anomaly = 'anomaly' THEN avg_acquisition_cost
            WHEN ctr_anomaly = 'anomaly' THEN quarterly_ctr
            WHEN clicks_anomaly = 'anomaly' THEN total_clicks
            WHEN impressions_anomaly = 'anomaly' THEN total_impressions
            WHEN spend_anomaly = 'anomaly' THEN total_spend
            WHEN revenue_anomaly = 'anomaly' THEN total_revenue
            WHEN campaign_count_anomaly = 'anomaly' THEN campaign_count
        END as actual_value,
        CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN conversion_rate_mean
            WHEN roi_anomaly = 'anomaly' THEN roi_mean
            WHEN acquisition_cost_anomaly = 'anomaly' THEN acquisition_cost_mean
            WHEN ctr_anomaly = 'anomaly' THEN ctr_mean
            WHEN clicks_anomaly = 'anomaly' THEN clicks_mean
            WHEN impressions_anomaly = 'anomaly' THEN impressions_mean
            WHEN spend_anomaly = 'anomaly' THEN spend_mean
            WHEN revenue_anomaly = 'anomaly' THEN revenue_mean
            WHEN campaign_count_anomaly = 'anomaly' THEN campaign_count_mean
        END as expected_value,
        CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN conversion_rate_z
            WHEN roi_anomaly = 'anomaly' THEN roi_z
            WHEN acquisition_cost_anomaly = 'anomaly' THEN acquisition_cost_z
            WHEN ctr_anomaly = 'anomaly' THEN ctr_z
            WHEN clicks_anomaly = 'anomaly' THEN clicks_z
            WHEN impressions_anomaly = 'anomaly' THEN impressions_z
            WHEN spend_anomaly = 'anomaly' THEN spend_z
            WHEN revenue_anomaly = 'anomaly' THEN revenue_z
            WHEN campaign_count_anomaly = 'anomaly' THEN campaign_count_z
        END as z_score,
        anomaly_impact,
        anomaly_count,
        anomaly_description as explanation
    FROM audience_quarter_anomalies
    WHERE Company = ?
    AND ABS(CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN conversion_rate_z
            WHEN roi_anomaly = 'anomaly' THEN roi_z
            WHEN acquisition_cost_anomaly = 'anomaly' THEN acquisition_cost_z
            WHEN ctr_anomaly = 'anomaly' THEN ctr_z
            WHEN clicks_anomaly = 'anomaly' THEN clicks_z
            WHEN impressions_anomaly = 'anomaly' THEN impressions_z
            WHEN spend_anomaly = 'anomaly' THEN spend_z
            WHEN revenue_anomaly = 'anomaly' THEN revenue_z
            WHEN campaign_count_anomaly = 'anomaly' THEN campaign_count_z
        END) >= ?
    ORDER BY anomaly_count DESC, ABS(CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN conversion_rate_z
            WHEN roi_anomaly = 'anomaly' THEN roi_z
            WHEN acquisition_cost_anomaly = 'anomaly' THEN acquisition_cost_z
            WHEN ctr_anomaly = 'anomaly' THEN ctr_z
            WHEN clicks_anomaly = 'anomaly' THEN clicks_z
            WHEN impressions_anomaly = 'anomaly' THEN impressions_z
            WHEN spend_anomaly = 'anomaly' THEN spend_z
            WHEN revenue_anomaly = 'anomaly' THEN revenue_z
            WHEN campaign_count_anomaly = 'anomaly' THEN campaign_count_z
        END) DESC
    """
    
    try:
        results = execute_query(query, [company_id, threshold])
        
        # Format the date for each anomaly (using current quarter date)
        current_date = datetime.now()
        current_quarter_end = datetime(current_date.year, ((current_date.month - 1) // 3) * 3 + 3, 1) - timedelta(days=1)
        date_str = current_quarter_end.strftime("%Y-%m-%d")
        
        # Add the date to each anomaly
        for result in results:
            result["date"] = date_str
        
        return {"anomalies": results}
    except Exception as e:
        logger.error(f"Error getting audience anomalies: {str(e)}")
        return {"anomalies": []}
