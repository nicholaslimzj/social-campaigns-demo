"""
Channel API utilities for the Meta Demo project.

This module contains utility functions for channel-related API endpoints.
These functions interact with the DuckDB database to retrieve channel data.
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

def get_company_channels(company_id: str, include_metrics: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get channels for a specific company.
    
    Args:
        company_id: Company name to get channels for
        include_metrics: Whether to include metrics with each channel
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Channels for the company
    """
    # Use channel_quarter_anomalies table which has the latest quarterly data
    # and maintains the hierarchical dimension structure with Company as primary dimension
    if include_metrics:
        query = """
        WITH company_metrics AS (
            SELECT 
                Channel_Used as channel_id,
                campaign_count,
                avg_conversion_rate,
                avg_roi,
                avg_acquisition_cost,
                quarterly_ctr as avg_ctr,
                has_anomaly,
                anomaly_impact,
                anomaly_count
            FROM channel_quarter_anomalies
            WHERE Company = ?
        ),
        -- Get industry benchmarks for all metrics in one CTE
        industry_benchmarks AS (
            SELECT 
                dimension,
                metric,
                entity as channel_id,
                metric_value,
                metric_rank,
                total_entities,
                CAST(metric_rank AS FLOAT) / total_entities as percentile_rank
            FROM dimensions_quarter_performance_rankings
            WHERE dimension = 'channel'
            AND metric IN ('roi', 'conversion_rate', 'acquisition_cost', 'ctr')
        ),
        -- Pivot the industry benchmarks to get one row per channel
        industry_metrics AS (
            SELECT
                channel_id,
                MAX(CASE WHEN metric = 'roi' THEN metric_value END) as industry_roi,
                MAX(CASE WHEN metric = 'roi' THEN percentile_rank END) as roi_percentile,
                MAX(CASE WHEN metric = 'conversion_rate' THEN metric_value END) as industry_conversion_rate,
                MAX(CASE WHEN metric = 'conversion_rate' THEN percentile_rank END) as conversion_percentile,
                MAX(CASE WHEN metric = 'acquisition_cost' THEN metric_value END) as industry_acquisition_cost,
                MAX(CASE WHEN metric = 'acquisition_cost' THEN percentile_rank END) as acquisition_percentile,
                MAX(CASE WHEN metric = 'ctr' THEN metric_value END) as industry_ctr,
                MAX(CASE WHEN metric = 'ctr' THEN percentile_rank END) as ctr_percentile
            FROM industry_benchmarks
            GROUP BY channel_id
        )
        -- Join company metrics with industry benchmarks
        SELECT 
            c.channel_id,
            c.campaign_count,
            c.avg_conversion_rate,
            c.avg_roi,
            c.avg_acquisition_cost,
            c.avg_ctr,
            c.has_anomaly,
            c.anomaly_impact,
            c.anomaly_count,
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
        LEFT JOIN industry_metrics i ON c.channel_id = i.channel_id
        ORDER BY c.avg_roi DESC
        """
    else:
        query = """
        SELECT 
            Channel_Used as channel_id,
            campaign_count
        FROM channel_quarter_anomalies
        WHERE Company = ?
        ORDER BY Channel_Used
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
                    'impact': result.pop('anomaly_impact', None),
                    'count': result.pop('anomaly_count', 0)
                }
        
        return {"channels": results}
    except Exception as e:
        logger.error(f"Error getting company channels: {str(e)}")
        return {"channels": []}

def get_monthly_channel_metrics(company_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get monthly metrics for channels of a specific company.
    
    Args:
        company_id: Company name to get monthly channel metrics for
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Monthly channel metrics for the company
    """
    query = """
    SELECT 
        Channel_Used as channel_id,
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
        channel_share_clicks,
        channel_count,
        efficiency_ratio
    FROM channel_monthly_metrics
    WHERE Company = ?
    ORDER BY month ASC, avg_roi DESC
    """
    
    try:
        results = execute_query(query, [company_id])
        
        # Calculate CAC (Customer Acquisition Cost) for each result
        for result in results:
            # CAC = Acquisition Cost / (Clicks * Conversion Rate)
            if result.get('clicks', 0) > 0 and result.get('conversion_rate', 0) > 0:
                conversions = result['clicks'] * result['conversion_rate']
                result['cac'] = result['acquisition_cost'] / conversions if conversions > 0 else 0
            else:
                result['cac'] = 0
        
        # Group by channel_id
        channels = {}
        for row in results:
            channel_id = row["channel_id"]
            if channel_id not in channels:
                channels[channel_id] = {
                    "channel_id": channel_id,
                    "monthly_metrics": []
                }
            
            # Add monthly metrics with enhanced data from the pre-computed model
            channels[channel_id]["monthly_metrics"].append({
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
                "channel_share": row["channel_share_clicks"],
                "channel_count": row["channel_count"],
                "efficiency_ratio": row["efficiency_ratio"]
            })
        
        return {"channels": list(channels.values())}
    except Exception as e:
        logger.error(f"Error getting monthly channel metrics: {str(e)}")
        return {"channels": []}

def get_channel_performance_matrix(company_id: str, dimension_type: str = "goal") -> Dict[str, List[Dict[str, Any]]]:
    """
    Get performance matrix for channels of a specific company.
    
    Args:
        company_id: Company name to get channel performance matrix for
        dimension_type: Type of dimension to analyze (goal, target_audience)
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Channel performance matrix for the company
    """
    # Use the channel_quarter_performance_matrix table which has pre-calculated performance metrics
    query = """
    SELECT 
        Channel_Used as channel_id,
        dimension_type,
        dimension_value,
        avg_roi,
        avg_conversion_rate,
        avg_acquisition_cost,
        avg_ctr
    FROM channel_quarter_performance_matrix
    WHERE Company = ?
    AND dimension_type = ?
    ORDER BY Channel_Used, dimension_value
    """
    
    try:
        results = execute_query(query, [company_id, dimension_type])
        
        if not results:
            return {"matrix": []}
            
        # Group by channel_id
        matrix = {}
        for row in results:
            channel_id = row["channel_id"]
            
            if channel_id not in matrix:
                matrix[channel_id] = {
                    "channel_id": channel_id,
                    "dimensions": []
                }
            
            matrix[channel_id]["dimensions"].append({
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
        logger.error(f"Error getting channel performance matrix: {str(e)}")
        return {"matrix": []}

def get_channel_clusters(company_id: str, min_roi: float = 0, min_conversion_rate: float = 0) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get channel clustering data for a specific company.
    
    Args:
        company_id: Company name to get channel clusters for
        min_roi: Minimum ROI threshold for filtering
        min_conversion_rate: Minimum conversion rate threshold for filtering
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Channel clusters for the company
    """
    # Query to get channel performance data from the pre-computed model
    performance_query = """
    SELECT 
        Channel_Used as channel_id,
        dimension_value,
        dimension_type,
        campaign_count,
        avg_conversion_rate as conversion_rate,
        avg_roi as roi,
        avg_acquisition_cost as acquisition_cost,
        avg_ctr as ctr,
        total_clicks,
        total_impressions,
        total_spend,
        total_revenue,
        composite_score as performance_score,
        performance_tier,
        is_top_performer
    FROM channel_quarter_performance_matrix
    WHERE Company = ?
    AND avg_roi >= ?
    AND avg_conversion_rate >= ?
    ORDER BY composite_score DESC
    """
    
    # Query to get channel summary metrics
    summary_query = """
    SELECT 
        Channel_Used as channel_id,
        COUNT(*) as dimension_count,
        AVG(avg_conversion_rate) as conversion_rate,
        AVG(avg_roi) as roi,
        AVG(avg_acquisition_cost) as acquisition_cost,
        AVG(avg_ctr) as ctr,
        SUM(total_clicks) as total_clicks,
        SUM(total_impressions) as total_impressions,
        SUM(total_spend) as total_spend,
        SUM(total_revenue) as total_revenue,
        AVG(composite_score) as avg_performance_score,
        COUNT(CASE WHEN performance_tier = 'high_performer' THEN 1 END) as high_performer_count,
        COUNT(CASE WHEN performance_tier = 'average_performer' THEN 1 END) as avg_performer_count,
        COUNT(CASE WHEN performance_tier = 'low_performer' THEN 1 END) as low_performer_count
    FROM channel_quarter_performance_matrix
    WHERE Company = ?
    AND avg_roi >= ?
    AND avg_conversion_rate >= ?
    GROUP BY Channel_Used
    ORDER BY AVG(composite_score) DESC
    """
    
    try:
        # Get detailed performance data
        performance_results = execute_query(performance_query, [company_id, min_roi, min_conversion_rate])
        
        # Get summary metrics
        summary_results = execute_query(summary_query, [company_id, min_roi, min_conversion_rate])
        
        # Process the data to create channel clusters
        channel_data = {}
        
        # First, add base metrics for each channel
        for summary in summary_results:
            channel_id = summary.get('channel_id')
            channel_data[channel_id] = {
                'channel_id': channel_id,
                'metrics': summary,
                'dimensions': [],
                'performance_score': summary.get('avg_performance_score', 0)
            }
        
        # Add dimension performance data for each channel
        for perf in performance_results:
            channel_id = perf.get('channel_id')
            if channel_id in channel_data:
                channel_data[channel_id]['dimensions'].append({
                    'dimension_type': perf.get('dimension_type'),
                    'dimension_value': perf.get('dimension_value'),
                    'campaign_count': perf.get('campaign_count'),
                    'conversion_rate': perf.get('conversion_rate'),
                    'roi': perf.get('roi'),
                    'acquisition_cost': perf.get('acquisition_cost'),
                    'ctr': perf.get('ctr'),
                    'total_spend': perf.get('total_spend'),
                    'total_revenue': perf.get('total_revenue'),
                    'performance_score': perf.get('performance_score'),
                    'performance_tier': perf.get('performance_tier'),
                    'is_top_performer': perf.get('is_top_performer')
                })
        
        # Define clusters based on performance tiers from the data
        high_performers = []
        mid_performers = []
        low_performers = []
        
        for channel in channel_data.values():
            # Determine cluster based on high performer count and average score
            metrics = channel.get('metrics', {})
            high_count = metrics.get('high_performer_count', 0)
            avg_score = channel.get('performance_score', 0)
            
            if high_count > 0 or avg_score > 1.0:
                high_performers.append(channel)
            elif avg_score > 0.8:
                mid_performers.append(channel)
            else:
                low_performers.append(channel)
        
        # Sort clusters by performance score
        high_performers.sort(key=lambda x: x.get('performance_score', 0), reverse=True)
        mid_performers.sort(key=lambda x: x.get('performance_score', 0), reverse=True)
        low_performers.sort(key=lambda x: x.get('performance_score', 0), reverse=True)
        
        return {
            "clusters": [
                {"name": "High Performers", "channels": high_performers},
                {"name": "Mid Performers", "channels": mid_performers},
                {"name": "Low Performers", "channels": low_performers}
            ]
        }
    except Exception as e:
        logger.error(f"Error getting channel clusters: {str(e)}")
        return {"clusters": []}

def get_channel_benchmarks(company_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get channel industry benchmarks.
    
    Args:
        company_id: Company name to get channel benchmarks for
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Channel benchmarks for the company
    """
    # Query that combines company-specific metrics with industry benchmarks
    query = """
    WITH company_metrics AS (
        -- Get company-specific metrics from channel_quarter_anomalies
        SELECT 
            Channel_Used as channel_id,
            avg_conversion_rate as company_conversion_rate,
            avg_roi as company_roi,
            avg_acquisition_cost as company_acquisition_cost,
            quarterly_ctr as company_ctr,
            has_anomaly,
            anomaly_impact,
            anomaly_count
        FROM channel_quarter_anomalies
        WHERE Company = ?
    ),
    -- Get industry benchmarks for all metrics in one CTE
    industry_benchmarks AS (
        SELECT 
            dimension,
            metric,
            entity as channel_id,
            metric_value,
            metric_rank,
            total_entities,
            CAST(metric_rank AS FLOAT) / total_entities as percentile_rank
        FROM dimensions_quarter_performance_rankings
        WHERE dimension = 'channel'
        AND metric IN ('roi', 'conversion_rate', 'acquisition_cost', 'ctr')
    ),
    -- Pivot the industry benchmarks to get one row per channel
    industry_metrics AS (
        SELECT
            channel_id,
            MAX(CASE WHEN metric = 'roi' THEN metric_value END) as industry_roi,
            MAX(CASE WHEN metric = 'roi' THEN percentile_rank END) as roi_percentile,
            MAX(CASE WHEN metric = 'conversion_rate' THEN metric_value END) as industry_conversion_rate,
            MAX(CASE WHEN metric = 'conversion_rate' THEN percentile_rank END) as conversion_percentile,
            MAX(CASE WHEN metric = 'acquisition_cost' THEN metric_value END) as industry_acquisition_cost,
            MAX(CASE WHEN metric = 'acquisition_cost' THEN percentile_rank END) as acquisition_percentile,
            MAX(CASE WHEN metric = 'ctr' THEN metric_value END) as industry_ctr,
            MAX(CASE WHEN metric = 'ctr' THEN percentile_rank END) as ctr_percentile
        FROM industry_benchmarks
        GROUP BY channel_id
    )
    -- Join company metrics with industry benchmarks
    SELECT 
        c.channel_id,
        -- Company metrics
        c.company_conversion_rate,
        c.company_roi,
        c.company_acquisition_cost,
        c.company_ctr,
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
    LEFT JOIN industry_metrics i ON c.channel_id = i.channel_id
    ORDER BY c.channel_id
    """
    
    try:
        results = execute_query(query, [company_id])
        
        # Enhance results with anomaly data and overall performance indicator
        for result in results:
            # Count the number of above_average performances
            above_average_count = sum(1 for perf in [
                result.get('conversion_performance', ''),
                result.get('roi_performance', ''),
                result.get('acquisition_performance', ''),
                result.get('ctr_performance', '')
            ] if perf == 'above_average')
            
            # Add anomaly information
            has_anomaly = result.get('has_anomaly', False)
            anomaly_impact = result.get('anomaly_impact', 'normal')
            anomaly_count = result.get('anomaly_count', 0)
            
            # Determine overall performance tier considering both benchmarks and anomalies
            if above_average_count >= 3 and (anomaly_impact == 'positive' or anomaly_impact == 'normal'):
                result['overall_performance'] = 'excellent'
            elif above_average_count >= 2 or (above_average_count >= 1 and anomaly_impact == 'positive'):
                result['overall_performance'] = 'good'
            elif above_average_count >= 1 or anomaly_impact == 'normal':
                result['overall_performance'] = 'average'
            else:
                result['overall_performance'] = 'needs_improvement'
            
            # Add anomaly status to the result
            result['has_anomaly'] = has_anomaly
            result['anomaly_impact'] = anomaly_impact
            result['anomaly_count'] = anomaly_count
            
            # Count the number of below_average performances
            below_average_count = sum(1 for perf in [
                result.get('conversion_performance', ''),
                result.get('roi_performance', ''),
                result.get('acquisition_performance', ''),
                result.get('ctr_performance', '')
            ] if perf == 'below_average')
            
            # Add risk assessment based on anomalies and benchmark comparison
            if anomaly_impact == 'negative' and anomaly_count >= 2:
                result['risk_level'] = 'high'
            elif anomaly_impact == 'negative' or below_average_count >= 3:
                result['risk_level'] = 'medium'
            else:
                result['risk_level'] = 'low'
        
        return {"channels": results}
    except Exception as e:
        logger.error(f"Error getting channel benchmarks: {str(e)}")
        return {"channels": []}

def get_channel_anomalies(company_id: str, threshold: float = 2.0) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get anomalies for channels of a specific company.
    
    Args:
        company_id: Company name to get channel anomalies for
        threshold: Z-score threshold for anomaly detection
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Channel anomalies for the company
    """
    # Query to get anomalies from the channel_quarter_anomalies table
    query = """
    SELECT 
        Channel_Used as channel_id,
        CASE
            WHEN conversion_rate_anomaly = 'anomaly' THEN 'conversion_rate'
            WHEN roi_anomaly = 'anomaly' THEN 'roi'
            WHEN acquisition_cost_anomaly = 'anomaly' THEN 'acquisition_cost'
            WHEN ctr_anomaly = 'anomaly' THEN 'ctr'
            WHEN clicks_anomaly = 'anomaly' THEN 'clicks'
            WHEN impressions_anomaly = 'anomaly' THEN 'impressions'
            WHEN spend_anomaly = 'anomaly' THEN 'spend'
            WHEN revenue_anomaly = 'anomaly' THEN 'revenue'
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
        END as z_score,
        anomaly_impact,
        anomaly_count
    FROM channel_quarter_anomalies
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
        END) DESC
    """
    
    try:
        results = execute_query(query, [company_id, threshold])
        
        # Format the date for each anomaly (using current quarter date)
        current_date = datetime.now()
        current_quarter_end = datetime(current_date.year, ((current_date.month - 1) // 3) * 3 + 3, 1) - timedelta(days=1)
        date_str = current_quarter_end.strftime("%Y-%m-%d")
        
        # Add the date and generate explanations for each anomaly
        for result in results:
            result["date"] = date_str
            
            # Generate explanation based on metric, z-score, and impact
            metric = result.get('metric', '')
            z_score = result.get('z_score', 0)
            direction = 'high' if z_score > 0 else 'low'
            
            # Create a human-readable explanation
            if metric == 'conversion_rate':
                result['explanation'] = f"Unusually {direction} conversion rate"
            elif metric == 'roi':
                result['explanation'] = f"Unusually {direction} ROI"
            elif metric == 'acquisition_cost':
                result['explanation'] = f"Unusually {direction} acquisition cost"
            elif metric == 'ctr':
                result['explanation'] = f"Unusually {direction} click-through rate"
            elif metric == 'clicks':
                result['explanation'] = f"Unusually {direction} click volume"
            elif metric == 'impressions':
                result['explanation'] = f"Unusually {direction} impression volume"
            elif metric == 'spend':
                result['explanation'] = f"Unusually {direction} spend"
            elif metric == 'revenue':
                result['explanation'] = f"Unusually {direction} revenue"
            else:
                result['explanation'] = f"{result.get('anomaly_impact', 'Unknown')} anomaly detected"
        
        return {"anomalies": results}
    except Exception as e:
        logger.error(f"Error getting channel anomalies: {str(e)}")
        return {"anomalies": []}

def get_channel_budget_optimizer(company_id: str, total_budget: float, optimization_goal: str = "roi") -> Dict[str, Any]:
    """
    Get budget allocation optimization recommendations for channels of a specific company.
    
    Args:
        company_id: Company name to get budget optimization for
        total_budget: Total budget to allocate
        optimization_goal: Metric to optimize for (currently only supports 'roi')
        
    Returns:
        Dict[str, Any]: Budget allocation recommendations for the company
    """
    query = """
    SELECT 
        Channel_Used as channel_id,
        current_spend,
        current_spend_share,
        optimal_spend_share,
        spend_share_change,
        avg_roi as current_roi,
        projected_roi,
        recommendation_direction,
        recommendation_strength,
        projected_improvement_pct
    FROM channel_quarter_budget_optimizer
    WHERE Company = ?
    ORDER BY efficiency_rank
    """
    
    try:
        results = execute_query(query, [company_id])
        
        if not results:
            return {
                "current_allocation": [],
                "optimized_allocation": [],
                "optimization_metrics": {
                    "total_budget": total_budget,
                    "optimization_goal": optimization_goal,
                    "projected_improvement": 0
                }
            }
        
        # Calculate current and optimized allocations
        current_allocation = []
        optimized_allocation = []
        total_current_spend = sum(r["current_spend"] for r in results)
        total_improvement = 0
        
        for result in results:
            # Calculate absolute budget amounts
            current_amount = result["current_spend"]
            optimized_amount = total_budget * result["optimal_spend_share"]
            
            # Add to current allocation
            current_allocation.append({
                "channel_id": result["channel_id"],
                "amount": current_amount,
                "percentage": result["current_spend_share"] * 100,
                "roi": result["current_roi"]
            })
            
            # Add to optimized allocation
            optimized_allocation.append({
                "channel_id": result["channel_id"],
                "amount": optimized_amount,
                "percentage": result["optimal_spend_share"] * 100,
                "roi": result["projected_roi"],
                "change_direction": result["recommendation_direction"],
                "change_strength": result["recommendation_strength"]
            })
            
            # Track improvement
            total_improvement += result["projected_improvement_pct"]
        
        # Calculate average improvement
        avg_improvement = total_improvement / len(results) if results else 0
        
        return {
            "current_allocation": current_allocation,
            "optimized_allocation": optimized_allocation,
            "optimization_metrics": {
                "total_budget": total_budget,
                "optimization_goal": optimization_goal,
                "projected_improvement": avg_improvement
            }
        }
    except Exception as e:
        logger.error(f"Error getting channel budget optimizer: {str(e)}")
        return {
            "current_allocation": [],
            "optimized_allocation": [],
            "optimization_metrics": {
                "total_budget": total_budget,
                "optimization_goal": optimization_goal,
                "projected_improvement": 0
            }
        }
