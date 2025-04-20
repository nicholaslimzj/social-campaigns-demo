"""
Campaign Performance Insights Generator for the Meta Demo project.

This module generates AI-powered insights about campaign performance
based on social media advertising data, leveraging the campaign clustering
and performance matrix models.
"""

import logging
import json
from typing import Dict, Any, List, Optional
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain_core.output_parsers import StrOutputParser

from app.scripts.insights_generator import get_analytics_connection, CustomJSONEncoder

# Configure logging
logger = logging.getLogger(__name__)

# System prompt template for campaign performance insights
CAMPAIGN_SYSTEM_TEMPLATE = """
You are an expert marketing analyst specializing in social media advertising campaign optimization.
Your task is to analyze campaign performance data and provide clear, actionable insights.

Follow these guidelines:
1. Identify winning campaign combinations (goal-segment-channel-duration)
2. Highlight optimal campaign durations for different goals and segments
3. Analyze performance patterns across campaign goals
4. Identify underperforming campaigns and suggest optimization strategies
5. Provide 2-3 specific, actionable recommendations for campaign optimization
6. Use a professional, data-driven tone
7. Keep your insights concise (3-5 sentences)

DO NOT:
- Make vague or generic statements that could apply to any campaign
- Include raw numbers or statistics without context
- Use marketing jargon without explanation
- Make recommendations not supported by the data

Your insights should help the marketing team optimize their campaign strategy and improve performance.
"""

# Human prompt template for campaign performance insights
CAMPAIGN_HUMAN_TEMPLATE = """
Analyze the following campaign performance data for {company_name} and provide a concise summary with actionable insights.

Campaign Clusters (Winning Combinations):
```json
{campaign_clusters}
```

Campaign Performance Matrix:
```json
{performance_matrix}
```

Campaign Duration Analysis:
```json
{duration_analysis}
```

Recent Campaign Performance:
```json
{recent_campaigns}
```

Please provide a concise summary (3-5 sentences) that highlights key campaign performance insights and 2-3 specific recommendations for optimizing campaign strategy.
"""

# Create the prompt template
campaign_prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(CAMPAIGN_SYSTEM_TEMPLATE),
    HumanMessagePromptTemplate.from_template(CAMPAIGN_HUMAN_TEMPLATE)
])

def get_campaign_clusters(company_name: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get winning campaign combinations from the campaign_historical_clusters model.
    
    Args:
        company_name: The name of the company
        limit: Maximum number of clusters to return
        
    Returns:
        List[Dict[str, Any]]: Winning campaign combinations
    """
    try:
        conn = get_analytics_connection()
        
        # Try to get data from campaign_historical_clusters
        result = conn.execute(f"""
        SELECT 
            goal,
            segment,
            channel,
            duration_bucket,
            optimal_duration_range,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            roi_vs_company_avg,
            roi_vs_global_avg,
            conversion_rate_vs_company_avg,
            conversion_rate_vs_global_avg,
            acquisition_cost_vs_company_avg,
            acquisition_cost_vs_global_avg,
            ctr_vs_company_avg,
            ctr_vs_global_avg,
            is_winning_combination,
            composite_score,
            composite_rank
        FROM campaign_historical_clusters
        WHERE Company = ?
        ORDER BY composite_score DESC
        LIMIT {limit}
        """, [company_name]).fetchdf()
        
        # If no data in the mart, try campaign_historical_performance_matrix
        if result.empty:
            result = conn.execute(f"""
            WITH campaign_clusters AS (
                SELECT 
                    Campaign_Goal as goal,
                    Customer_Segment as segment,
                    Channel_Used as channel,
                    NULL as duration_bucket,
                    NULL as optimal_duration_range,
                    avg_roi,
                    avg_conversion_rate,
                    avg_acquisition_cost,
                    avg_ctr,
                    campaign_count,
                    vs_goal_avg as roi_vs_company_avg,
                    vs_global_avg as roi_vs_global_avg,
                    NULL as conversion_rate_vs_company_avg,
                    NULL as conversion_rate_vs_global_avg,
                    NULL as acquisition_cost_vs_company_avg,
                    NULL as acquisition_cost_vs_global_avg,
                    NULL as ctr_vs_company_avg,
                    NULL as ctr_vs_global_avg,
                    is_top_performer as is_winning_combination,
                    composite_score,
                    rank_overall as composite_rank
                FROM campaign_historical_performance_matrix
                WHERE Company = ?
                GROUP BY Campaign_Goal, Customer_Segment, Channel_Used
                HAVING campaign_count >= 3
            )
            SELECT * FROM campaign_clusters
            ORDER BY composite_score DESC
            LIMIT {limit}
            """, [company_name]).fetchdf()
        
        # If still no data, fall back to a simplified query on stg_campaigns
        if result.empty:
            result = conn.execute(f"""
            WITH campaign_stats AS (
                SELECT 
                    Campaign_Goal as goal,
                    Customer_Segment as segment,
                    Channel as channel,
                    CASE 
                        WHEN Duration <= 7 THEN '1-7 days'
                        WHEN Duration <= 14 THEN '8-14 days'
                        WHEN Duration <= 30 THEN '15-30 days'
                        ELSE '30+ days'
                    END as duration_bucket,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY Campaign_Goal, Customer_Segment, Channel, duration_bucket
                HAVING campaign_count >= 3
            ),
            company_avg AS (
                SELECT 
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr
                FROM stg_campaigns
                WHERE Company = ?
            ),
            global_avg AS (
                SELECT 
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr
                FROM stg_campaigns
            )
            SELECT 
                cs.goal,
                cs.segment,
                cs.channel,
                cs.duration_bucket,
                cs.duration_bucket as optimal_duration_range,
                cs.avg_roi,
                cs.avg_conversion_rate,
                cs.avg_acquisition_cost,
                cs.avg_ctr,
                cs.campaign_count,
                (cs.avg_roi / NULLIF(ca.avg_roi, 0)) - 1 as roi_vs_company_avg,
                (cs.avg_roi / NULLIF(ga.avg_roi, 0)) - 1 as roi_vs_global_avg,
                (cs.avg_conversion_rate / NULLIF(ca.avg_conversion_rate, 0)) - 1 as conversion_rate_vs_company_avg,
                (cs.avg_conversion_rate / NULLIF(ga.avg_conversion_rate, 0)) - 1 as conversion_rate_vs_global_avg,
                (ca.avg_acquisition_cost / NULLIF(cs.avg_acquisition_cost, 0)) - 1 as acquisition_cost_vs_company_avg,
                (ga.avg_acquisition_cost / NULLIF(cs.avg_acquisition_cost, 0)) - 1 as acquisition_cost_vs_global_avg,
                (cs.avg_ctr / NULLIF(ca.avg_ctr, 0)) - 1 as ctr_vs_company_avg,
                (cs.avg_ctr / NULLIF(ga.avg_ctr, 0)) - 1 as ctr_vs_global_avg,
                CASE WHEN cs.avg_roi > ca.avg_roi THEN 1 ELSE 0 END as is_winning_combination,
                (cs.avg_roi * 0.4) + (cs.avg_conversion_rate * 0.3) + ((1.0 / NULLIF(cs.avg_acquisition_cost, 0)) * 0.2) + (cs.avg_ctr * 0.1) as composite_score,
                1 as composite_rank
            FROM campaign_stats cs
            CROSS JOIN company_avg ca
            CROSS JOIN global_avg ga
            ORDER BY composite_score DESC
            LIMIT {limit}
            """, [company_name, company_name, company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting campaign clusters: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def get_performance_matrix(company_name: str) -> List[Dict[str, Any]]:
    """
    Get campaign performance matrix data from the campaign_historical_performance_matrix model.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Campaign performance matrix data
    """
    try:
        conn = get_analytics_connection()
        
        # Query for performance matrix from campaign_historical_performance_matrix
        result = conn.execute("""
        SELECT 
            Campaign_Goal as goal,
            Customer_Segment as segment,
            Channel_Used as channel,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            is_tested,
            roi_normalized as normalized_roi,
            conversion_rate_normalized as normalized_conversion_rate,
            acquisition_cost_normalized as normalized_acquisition_cost,
            ctr_normalized as normalized_ctr,
            composite_score,
            vs_goal_avg,
            vs_segment_avg,
            vs_global_avg,
            is_top_performer,
            is_untested,
            is_high_roi,
            is_high_conversion,
            is_cost_efficient,
            is_high_engagement,
            rank_within_goal,
            rank_within_segment,
            rank_overall
        FROM campaign_historical_performance_matrix
        WHERE Company = ?
        ORDER BY composite_score DESC
        """, [company_name]).fetchdf()
        
        # If no data in the mart, try campaign_historical_clusters
        if result.empty:
            result = conn.execute("""
            SELECT 
                goal,
                segment,
                channel,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                1 as is_tested,
                NULL as normalized_roi,
                NULL as normalized_conversion_rate,
                NULL as normalized_acquisition_cost,
                NULL as normalized_ctr,
                composite_score,
                roi_vs_company_avg as vs_goal_avg,
                NULL as vs_segment_avg,
                roi_vs_global_avg as vs_global_avg,
                is_winning_combination as is_top_performer,
                0 as is_untested,
                CASE WHEN roi_vs_global_avg > 0.2 THEN 1 ELSE 0 END as is_high_roi,
                CASE WHEN conversion_rate_vs_global_avg > 0.2 THEN 1 ELSE 0 END as is_high_conversion,
                CASE WHEN acquisition_cost_vs_global_avg < -0.2 THEN 1 ELSE 0 END as is_cost_efficient,
                CASE WHEN ctr_vs_global_avg > 0.2 THEN 1 ELSE 0 END as is_high_engagement,
                NULL as rank_within_goal,
                NULL as rank_within_segment,
                composite_rank as rank_overall
            FROM campaign_historical_clusters
            WHERE Company = ?
            ORDER BY composite_score DESC
            """, [company_name]).fetchdf()
        
        # If still no data, fall back to a simplified query on stg_campaigns
        if result.empty:
            result = conn.execute("""
            WITH campaign_combos AS (
                SELECT 
                    Campaign_Goal as goal,
                    Customer_Segment as segment,
                    Channel as channel,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count,
                    CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as is_tested
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY Campaign_Goal, Customer_Segment, Channel
                HAVING campaign_count >= 2
            ),
            goal_avgs AS (
                SELECT 
                    goal,
                    AVG(avg_roi) as goal_avg_roi,
                    AVG(avg_conversion_rate) as goal_avg_conversion_rate,
                    AVG(avg_acquisition_cost) as goal_avg_acquisition_cost,
                    AVG(avg_ctr) as goal_avg_ctr
                FROM campaign_combos
                GROUP BY goal
            ),
            segment_avgs AS (
                SELECT 
                    segment,
                    AVG(avg_roi) as segment_avg_roi,
                    AVG(avg_conversion_rate) as segment_avg_conversion_rate,
                    AVG(avg_acquisition_cost) as segment_avg_acquisition_cost,
                    AVG(avg_ctr) as segment_avg_ctr
                FROM campaign_combos
                GROUP BY segment
            ),
            global_avgs AS (
                SELECT 
                    AVG(avg_roi) as global_avg_roi,
                    AVG(avg_conversion_rate) as global_avg_conversion_rate,
                    AVG(avg_acquisition_cost) as global_avg_acquisition_cost,
                    AVG(avg_ctr) as global_avg_ctr
                FROM campaign_combos
            ),
            normalized_metrics AS (
                SELECT 
                    cc.goal,
                    cc.segment,
                    cc.channel,
                    cc.avg_roi,
                    cc.avg_conversion_rate,
                    cc.avg_acquisition_cost,
                    cc.avg_ctr,
                    cc.campaign_count,
                    cc.is_tested,
                    ga.goal_avg_roi,
                    sa.segment_avg_roi,
                    gla.global_avg_roi,
                    ga.goal_avg_conversion_rate,
                    sa.segment_avg_conversion_rate,
                    gla.global_avg_conversion_rate,
                    ga.goal_avg_acquisition_cost,
                    sa.segment_avg_acquisition_cost,
                    gla.global_avg_acquisition_cost,
                    ga.goal_avg_ctr,
                    sa.segment_avg_ctr,
                    gla.global_avg_ctr,
                    cc.avg_roi / NULLIF((SELECT MAX(avg_roi) FROM campaign_combos), 0) as normalized_roi,
                    cc.avg_conversion_rate / NULLIF((SELECT MAX(avg_conversion_rate) FROM campaign_combos), 0) as normalized_conversion_rate,
                    (SELECT MIN(avg_acquisition_cost) FROM campaign_combos) / NULLIF(cc.avg_acquisition_cost, 0) as normalized_acquisition_cost,
                    cc.avg_ctr / NULLIF((SELECT MAX(avg_ctr) FROM campaign_combos), 0) as normalized_ctr
                FROM campaign_combos cc
                LEFT JOIN goal_avgs ga ON cc.goal = ga.goal
                LEFT JOIN segment_avgs sa ON cc.segment = sa.segment
                CROSS JOIN global_avgs gla
            )
            SELECT 
                goal,
                segment,
                channel,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                is_tested,
                normalized_roi,
                normalized_conversion_rate,
                normalized_acquisition_cost,
                normalized_ctr,
                (normalized_roi * 0.4) + (normalized_conversion_rate * 0.3) + (normalized_acquisition_cost * 0.2) + (normalized_ctr * 0.1) as composite_score,
                (avg_roi / NULLIF(goal_avg_roi, 0)) - 1 as vs_goal_avg,
                (avg_roi / NULLIF(segment_avg_roi, 0)) - 1 as vs_segment_avg,
                (avg_roi / NULLIF(global_avg_roi, 0)) - 1 as vs_global_avg,
                CASE WHEN avg_roi > global_avg_roi * 1.1 THEN 1 ELSE 0 END as is_top_performer,
                CASE WHEN campaign_count < 3 THEN 1 ELSE 0 END as is_untested,
                CASE WHEN avg_roi > global_avg_roi * 1.2 THEN 1 ELSE 0 END as is_high_roi,
                CASE WHEN avg_conversion_rate > global_avg_conversion_rate * 1.2 THEN 1 ELSE 0 END as is_high_conversion,
                CASE WHEN avg_acquisition_cost < global_avg_acquisition_cost * 0.8 THEN 1 ELSE 0 END as is_cost_efficient,
                CASE WHEN avg_ctr > global_avg_ctr * 1.2 THEN 1 ELSE 0 END as is_high_engagement,
                ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_roi DESC) as rank_within_goal,
                ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_roi DESC) as rank_within_segment,
                ROW_NUMBER() OVER (ORDER BY (normalized_roi * 0.4) + (normalized_conversion_rate * 0.3) + (normalized_acquisition_cost * 0.2) + (normalized_ctr * 0.1) DESC) as rank_overall
            FROM normalized_metrics
            ORDER BY composite_score DESC
            """, [company_name, company_name, company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting performance matrix: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def get_duration_analysis(company_name: str) -> List[Dict[str, Any]]:
    """
    Get campaign duration analysis data from the campaign_duration_historical_analysis model.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Campaign duration analysis data
    """
    try:
        conn = get_analytics_connection()
        
        # Query for duration analysis from campaign_duration_historical_analysis
        result = conn.execute("""
        SELECT 
            dimension_type,
            dimension_value,
            duration_bucket,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            is_optimal_duration,
            potential_roi_improvement,
            roi_improvement_percentage,
            using_optimal_duration_count,
            not_using_optimal_duration_count,
            exact_duration_data,
            optimal_duration_range,
            roi_per_day,
            vs_avg_roi,
            vs_avg_conversion_rate,
            vs_avg_acquisition_cost,
            vs_avg_ctr
        FROM campaign_duration_historical_analysis
        WHERE Company = ?
        ORDER BY dimension_type, dimension_value, avg_roi DESC
        """, [company_name]).fetchdf()
        
        # If no data in the mart, fall back to a simplified query on stg_campaigns
        if result.empty:
            result = conn.execute("""
            WITH duration_buckets AS (
                SELECT 
                    'Overall' as dimension_type,
                    'All Campaigns' as dimension_value,
                    CASE 
                        WHEN Duration <= 7 THEN '1-7 days'
                        WHEN Duration <= 14 THEN '8-14 days'
                        WHEN Duration <= 30 THEN '15-30 days'
                        ELSE '30+ days'
                    END as duration_bucket,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count,
                    AVG(ROI) / NULLIF(AVG(Duration), 0) as roi_per_day
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY dimension_type, dimension_value, duration_bucket
                HAVING campaign_count >= 3
                
                UNION ALL
                
                SELECT 
                    'Campaign Goal' as dimension_type,
                    Campaign_Goal as dimension_value,
                    CASE 
                        WHEN Duration <= 7 THEN '1-7 days'
                        WHEN Duration <= 14 THEN '8-14 days'
                        WHEN Duration <= 30 THEN '15-30 days'
                        ELSE '30+ days'
                    END as duration_bucket,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count,
                    AVG(ROI) / NULLIF(AVG(Duration), 0) as roi_per_day
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY dimension_type, dimension_value, duration_bucket
                HAVING campaign_count >= 3
                
                UNION ALL
                
                SELECT 
                    'Customer Segment' as dimension_type,
                    Customer_Segment as dimension_value,
                    CASE 
                        WHEN Duration <= 7 THEN '1-7 days'
                        WHEN Duration <= 14 THEN '8-14 days'
                        WHEN Duration <= 30 THEN '15-30 days'
                        ELSE '30+ days'
                    END as duration_bucket,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count,
                    AVG(ROI) / NULLIF(AVG(Duration), 0) as roi_per_day
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY dimension_type, dimension_value, duration_bucket
                HAVING campaign_count >= 3
                
                UNION ALL
                
                SELECT 
                    'Channel' as dimension_type,
                    Channel as dimension_value,
                    CASE 
                        WHEN Duration <= 7 THEN '1-7 days'
                        WHEN Duration <= 14 THEN '8-14 days'
                        WHEN Duration <= 30 THEN '15-30 days'
                        ELSE '30+ days'
                    END as duration_bucket,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count,
                    AVG(ROI) / NULLIF(AVG(Duration), 0) as roi_per_day
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY dimension_type, dimension_value, duration_bucket
                HAVING campaign_count >= 3
            ),
            optimal_durations AS (
                SELECT 
                    dimension_type,
                    dimension_value,
                    FIRST_VALUE(duration_bucket) OVER (
                        PARTITION BY dimension_type, dimension_value 
                        ORDER BY avg_roi DESC
                    ) as optimal_duration_bucket,
                    MAX(avg_roi) OVER (
                        PARTITION BY dimension_type, dimension_value
                    ) as max_roi
                FROM duration_buckets
            )
            SELECT 
                db.dimension_type,
                db.dimension_value,
                db.duration_bucket,
                db.avg_roi,
                db.avg_conversion_rate,
                db.avg_acquisition_cost,
                db.avg_ctr,
                db.campaign_count,
                CASE WHEN db.duration_bucket = od.optimal_duration_bucket THEN 1 ELSE 0 END as is_optimal_duration,
                CASE WHEN db.duration_bucket != od.optimal_duration_bucket THEN od.max_roi - db.avg_roi ELSE 0 END as potential_roi_improvement,
                CASE WHEN db.duration_bucket != od.optimal_duration_bucket THEN (od.max_roi / NULLIF(db.avg_roi, 0) - 1) * 100 ELSE 0 END as roi_improvement_percentage,
                CASE WHEN db.duration_bucket = od.optimal_duration_bucket THEN db.campaign_count ELSE 0 END as using_optimal_duration_count,
                CASE WHEN db.duration_bucket != od.optimal_duration_bucket THEN db.campaign_count ELSE 0 END as not_using_optimal_duration_count
            FROM duration_buckets db
            JOIN optimal_durations od ON db.dimension_type = od.dimension_type AND db.dimension_value = od.dimension_value
            ORDER BY db.dimension_type, db.dimension_value, db.avg_roi DESC
            """, [company_name, company_name, company_name, company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting duration analysis: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def get_recent_campaigns(company_name: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get recent campaign performance data.
    
    Args:
        company_name: The name of the company
        limit: Maximum number of campaigns to return
        
    Returns:
        List[Dict[str, Any]]: Recent campaign performance data
    """
    try:
        conn = get_analytics_connection()
        
        # Try to get data from campaign_future_forecast
        result = conn.execute(f"""
        SELECT 
            campaign_name,
            goal,
            segment,
            channel,
            duration,
            actual_roi as roi,
            actual_conversion_rate as conversion_rate,
            actual_acquisition_cost as acquisition_cost,
            actual_ctr as ctr,
            spend,
            revenue,
            campaign_date as date,
            forecasted_roi,
            forecasted_conversion_rate,
            forecasted_acquisition_cost,
            forecasted_ctr,
            roi_vs_forecast,
            conversion_rate_vs_forecast,
            acquisition_cost_vs_forecast,
            ctr_vs_forecast
        FROM campaign_future_forecast
        WHERE Company = ?
        ORDER BY campaign_date DESC
        LIMIT {limit}
        """, [company_name]).fetchdf()
        
        # If no data in the mart, fall back to campaign_historical_analysis
        if result.empty:
            result = conn.execute(f"""
            SELECT 
                campaign_name,
                goal,
                segment,
                channel,
                duration,
                avg_roi as roi,
                avg_conversion_rate as conversion_rate,
                avg_acquisition_cost as acquisition_cost,
                avg_ctr as ctr,
                spend,
                revenue,
                campaign_date as date,
                NULL as forecasted_roi,
                NULL as forecasted_conversion_rate,
                NULL as forecasted_acquisition_cost,
                NULL as forecasted_ctr,
                NULL as roi_vs_forecast,
                NULL as conversion_rate_vs_forecast,
                NULL as acquisition_cost_vs_forecast,
                NULL as ctr_vs_forecast
            FROM campaign_historical_analysis
            WHERE Company = ?
            ORDER BY campaign_date DESC
            LIMIT {limit}
            """, [company_name]).fetchdf()
        
        # If still no data, fall back to stg_campaigns
        if result.empty:
            result = conn.execute(f"""
            SELECT 
                Campaign_ID as campaign_name,
                Campaign_Goal as goal,
                Customer_Segment as segment,
                Channel as channel,
                Duration as duration,
                ROI as roi,
                Conversion_Rate as conversion_rate,
                Acquisition_Cost as acquisition_cost,
                CAST(Clicks AS FLOAT) / NULLIF(Impressions, 0) as ctr,
                Clicks * CPC as spend,
                ROI * (Clicks * CPC) as revenue,
                StandardizedDate as date,
                NULL as forecasted_roi,
                NULL as forecasted_conversion_rate,
                NULL as forecasted_acquisition_cost,
                NULL as forecasted_ctr,
                NULL as roi_vs_forecast,
                NULL as conversion_rate_vs_forecast,
                NULL as acquisition_cost_vs_forecast,
                NULL as ctr_vs_forecast
            FROM stg_campaigns
            WHERE Company = ?
            ORDER BY StandardizedDate DESC
            LIMIT {limit}
            """, [company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting recent campaigns: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def generate_campaign_insight(llm, company_name: str) -> str:
    """
    Generate campaign performance insights.
{{ ... }}
    Args:
        llm: The language model to use
        company_name: The name of the company
        
    Returns:
        str: The generated insight
    """
    try:
        # Get data for the company
        campaign_clusters = get_campaign_clusters(company_name)
        performance_matrix = get_performance_matrix(company_name)
        duration_analysis = get_duration_analysis(company_name)
        recent_campaigns = get_recent_campaigns(company_name)
        
        # Check if we have enough data
        if not campaign_clusters and not performance_matrix:
            return f"Insufficient campaign data available for {company_name}."
        
        # Convert data to JSON strings
        campaign_clusters_json = json.dumps(campaign_clusters, cls=CustomJSONEncoder, indent=2)
        performance_matrix_json = json.dumps(performance_matrix, cls=CustomJSONEncoder, indent=2)
        duration_analysis_json = json.dumps(duration_analysis, cls=CustomJSONEncoder, indent=2)
        recent_campaigns_json = json.dumps(recent_campaigns, cls=CustomJSONEncoder, indent=2)
        
        # Create the chain
        chain = campaign_prompt | llm | StrOutputParser()
        
        # Generate the insight
        insight = chain.invoke({
            "company_name": company_name,
            "campaign_clusters": campaign_clusters_json,
            "performance_matrix": performance_matrix_json,
            "duration_analysis": duration_analysis_json,
            "recent_campaigns": recent_campaigns_json
        })
        
        return insight
    except Exception as e:
        logger.error(f"Error generating campaign insight: {str(e)}")
        raise  # Re-raise the exception for proper error handling
