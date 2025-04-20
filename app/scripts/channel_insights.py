"""
Channel Performance Insights Generator for the Meta Demo project.

This module generates AI-powered insights about channel performance
based on social media advertising data.
"""

import logging
import json
from typing import Dict, Any, List, Optional
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain_core.output_parsers import StrOutputParser

from app.scripts.insights_generator import get_analytics_connection, CustomJSONEncoder

# Configure logging
logger = logging.getLogger(__name__)

# System prompt template for channel performance insights
CHANNEL_SYSTEM_TEMPLATE = """
You are an expert marketing analyst specializing in multi-channel social media advertising.
Your task is to analyze channel performance data and provide clear, actionable insights.

Follow these guidelines:
1. Identify the best and worst performing channels
2. Analyze why certain channels perform better than others
3. Compare channel performance to company and industry averages
4. Highlight channel-specific trends in key metrics (ROI, conversion rate, acquisition cost, CTR)
5. Identify optimal channel-segment combinations
6. Provide 2-3 specific, actionable recommendations for channel optimization
7. Use a professional, data-driven tone
8. Keep your insights concise (3-5 sentences)

DO NOT:
- Make vague or generic statements that could apply to any channel
- Include raw numbers or statistics without context
- Use marketing jargon without explanation
- Make recommendations not supported by the data

Your insights should help the marketing team optimize their channel strategy and budget allocation.
"""

# Human prompt template for channel performance insights
CHANNEL_HUMAN_TEMPLATE = """
Analyze the following channel performance data for {company_name} and provide a concise summary with actionable insights.

Channel Performance Overview:
```json
{channel_performance}
```

Channel-Segment Performance:
```json
{channel_segment_performance}
```

Channel-Goal Performance:
```json
{channel_goal_performance}
```

Industry Channel Benchmarks:
```json
{industry_benchmarks}
```

Please provide a concise summary (3-5 sentences) that highlights key channel performance insights and 2-3 specific recommendations for optimizing channel strategy and budget allocation.
"""

# Create the prompt template
channel_prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(CHANNEL_SYSTEM_TEMPLATE),
    HumanMessagePromptTemplate.from_template(CHANNEL_HUMAN_TEMPLATE)
])

def get_channel_performance(company_name: str) -> List[Dict[str, Any]]:
    """
    Get performance metrics for all channels for a company.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Performance metrics for all channels
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Query for channel performance from channel_historical_metrics
        result = conn.execute("""
        SELECT 
            Channel_Used as channel,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            overall_ctr as avg_ctr,
            campaign_count,
            total_spend,
            total_revenue,
            roi_rank,
            conversion_rate_rank,
            acquisition_cost_rank,
            ctr_rank,
            composite_score,
            vs_company_avg,
            vs_global_avg
        FROM channel_historical_metrics
        ORDER BY composite_score DESC
        """).fetchdf()
        
        # If no data in the mart, fall back to dimensions_quarterly_performance_rankings
        if result.empty:
            result = conn.execute("""
            SELECT 
                dimension_value as channel,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                NULL as total_spend,
                NULL as total_revenue,
                roi_rank,
                conversion_rate_rank,
                acquisition_cost_rank,
                ctr_rank,
                composite_score,
                vs_company_avg,
                vs_global_avg
            FROM dimensions_quarterly_performance_rankings
            WHERE Company = ? AND dimension_type = 'Channel'
            ORDER BY composite_score DESC
            """).fetchdf()
        
        # If still no data, fall back to stg_campaigns
        if result.empty:
            result = conn.execute("""
            SELECT 
                Channel as channel,
                AVG(ROI) as avg_roi,
                AVG(Conversion_Rate) as avg_conversion_rate,
                AVG(Acquisition_Cost) as avg_acquisition_cost,
                CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                COUNT(*) as campaign_count,
                SUM(Clicks * CPC) as total_spend,
                SUM(ROI * (Clicks * CPC)) as total_revenue,
                NULL as roi_rank,
                NULL as conversion_rate_rank,
                NULL as acquisition_cost_rank,
                NULL as ctr_rank,
                NULL as composite_score,
                NULL as vs_company_avg,
                NULL as vs_global_avg
            FROM stg_campaigns
            WHERE Company = ?
            GROUP BY Channel_Used
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting channel performance: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_channel_segment_performance(company_name: str) -> List[Dict[str, Any]]:
    """
    Get performance metrics for channel-segment combinations.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Performance metrics for channel-segment combinations
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Try to get data from campaign_historical_performance_matrix
        result = conn.execute("""
        WITH channel_segment_combos AS (
            SELECT 
                Channel_Used as channel,
                Customer_Segment as segment,
                AVG(avg_roi) as avg_roi,
                AVG(avg_conversion_rate) as avg_conversion_rate,
                AVG(avg_acquisition_cost) as avg_acquisition_cost,
                AVG(avg_ctr) as avg_ctr,
                SUM(campaign_count) as campaign_count,
                AVG(composite_score) as composite_score
            FROM campaign_historical_performance_matrix
            GROUP BY Channel_Used, Customer_Segment
            HAVING campaign_count >= 3
        )
        SELECT 
            channel,
            segment,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            composite_score,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) as rank_within_channel,
            ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_roi DESC) as rank_within_segment
        FROM channel_segment_combos
        ORDER BY avg_roi DESC
        """).fetchdf()
        
        # If no data, fall back to campaign_historical_clusters
        if result.empty:
            result = conn.execute("""
            SELECT 
                channel,
                segment,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                composite_score,
                ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) as rank_within_channel,
                ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_roi DESC) as rank_within_segment
            FROM campaign_historical_clusters
            WHERE Company = ?
            GROUP BY channel, segment
            HAVING campaign_count >= 3
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # If still no data, fall back to stg_campaigns
        if result.empty:
            result = conn.execute("""
            WITH channel_segment_stats AS (
                SELECT 
                    Channel as channel,
                    Customer_Segment as segment,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count
                FROM stg_campaigns
                WHERE company_id = ?
                GROUP BY Channel_Used, Customer_Segment
                HAVING campaign_count >= 3  -- Ensure statistical significance
            )
            SELECT 
                channel,
                segment,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                (avg_roi * 0.4) + (avg_conversion_rate * 0.3) + ((1.0 / NULLIF(avg_acquisition_cost, 0)) * 0.2) + (avg_ctr * 0.1) as composite_score,
                ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) as rank_within_channel,
                ROW_NUMBER() OVER (PARTITION BY segment ORDER BY avg_roi DESC) as rank_within_segment
            FROM channel_segment_stats
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting channel-segment performance: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_channel_goal_performance(company_name: str) -> List[Dict[str, Any]]:
    """
    Get performance metrics for channel-goal combinations.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Performance metrics for channel-goal combinations
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Try to get data from campaign_historical_performance_matrix
        result = conn.execute("""
        WITH channel_goal_combos AS (
            SELECT 
                Channel_Used as channel,
                Campaign_Goal as goal,
                AVG(avg_roi) as avg_roi,
                AVG(avg_conversion_rate) as avg_conversion_rate,
                AVG(avg_acquisition_cost) as avg_acquisition_cost,
                AVG(avg_ctr) as avg_ctr,
                SUM(campaign_count) as campaign_count,
                AVG(composite_score) as composite_score
            FROM campaign_historical_performance_matrix
            GROUP BY Channel_Used, Campaign_Goal
            HAVING campaign_count >= 3
        )
        SELECT 
            channel,
            goal,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            composite_score,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) as rank_within_channel,
            ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_roi DESC) as rank_within_goal
        FROM channel_goal_combos
        ORDER BY avg_roi DESC
        """).fetchdf()
        
        # If no data, fall back to campaign_historical_clusters
        if result.empty:
            result = conn.execute("""
            SELECT 
                channel,
                goal,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                composite_score,
                ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) as rank_within_channel,
                ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_roi DESC) as rank_within_goal
            FROM campaign_historical_clusters
            WHERE company_id = ?
            GROUP BY channel, goal
            HAVING campaign_count >= 3
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # If still no data, fall back to stg_campaigns
        if result.empty:
            result = conn.execute("""
            WITH channel_goal_stats AS (
                SELECT 
                    Channel as channel,
                    Campaign_Goal as goal,
                    AVG(ROI) as avg_roi,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY Channel, Campaign_Goal
                HAVING campaign_count >= 3  -- Ensure statistical significance
            )
            SELECT 
                channel,
                goal,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                avg_ctr,
                campaign_count,
                (avg_roi * 0.4) + (avg_conversion_rate * 0.3) + ((1.0 / NULLIF(avg_acquisition_cost, 0)) * 0.2) + (avg_ctr * 0.1) as composite_score,
                ROW_NUMBER() OVER (PARTITION BY channel ORDER BY avg_roi DESC) as rank_within_channel,
                ROW_NUMBER() OVER (PARTITION BY goal ORDER BY avg_roi DESC) as rank_within_goal
            FROM channel_goal_stats
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting channel-goal performance: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_industry_channel_benchmarks() -> List[Dict[str, Any]]:
    """
    Get industry benchmarks for channels across all companies.
    
    Returns:
        List[Dict[str, Any]]: Industry benchmarks for channels
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Query for industry channel benchmarks from channel_historical_metrics
        result = conn.execute("""
        SELECT 
            Channel_Used as channel,
            AVG(avg_roi) as avg_roi,
            AVG(avg_conversion_rate) as avg_conversion_rate,
            AVG(avg_acquisition_cost) as avg_acquisition_cost,
            AVG(overall_ctr) as avg_ctr,
            SUM(campaign_count) as campaign_count,
            COUNT(DISTINCT Company) as company_count,
            AVG(composite_score) as avg_composite_score
        FROM channel_historical_metrics
        GROUP BY channel
        ORDER BY avg_roi DESC
        """).fetchdf()
        
        # If no data in the mart, fall back to dimensions_quarterly_performance_rankings
        if result.empty:
            result = conn.execute("""
            SELECT 
                dimension_value as channel,
                AVG(avg_roi) as avg_roi,
                AVG(avg_conversion_rate) as avg_conversion_rate,
                AVG(avg_acquisition_cost) as avg_acquisition_cost,
                AVG(avg_ctr) as avg_ctr,
                SUM(campaign_count) as campaign_count,
                COUNT(DISTINCT Company) as company_count,
                AVG(composite_score) as avg_composite_score
            FROM dimensions_quarterly_performance_rankings
            WHERE dimension_type = 'Channel'
            GROUP BY dimension_value
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # If still no data, fall back to stg_campaigns
        if result.empty:
            result = conn.execute("""
            SELECT 
                Channel as channel,
                AVG(ROI) as avg_roi,
                AVG(Conversion_Rate) as avg_conversion_rate,
                AVG(Acquisition_Cost) as avg_acquisition_cost,
                CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                COUNT(*) as campaign_count,
                COUNT(DISTINCT Company) as company_count,
                NULL as avg_composite_score
            FROM stg_campaigns
            GROUP BY Channel
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting industry channel benchmarks: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def generate_channel_insight(llm, company_name: str) -> str:
    """
    Generate channel performance insights.
    
    Args:
        llm: The language model to use
        company_name: The name of the company
        
    Returns:
        str: The generated insight
    """
    try:
        # Get data for the company
        channel_performance = get_channel_performance(company_name)
        channel_segment_performance = get_channel_segment_performance(company_name)
        channel_goal_performance = get_channel_goal_performance(company_name)
        industry_benchmarks = get_industry_channel_benchmarks()
        
        # Check if we have enough data
        if not channel_performance:
            return f"Insufficient channel data available for {company_name}."
        
        # Convert data to JSON strings
        channel_performance_json = json.dumps(channel_performance, cls=CustomJSONEncoder, indent=2)
        channel_segment_performance_json = json.dumps(channel_segment_performance, cls=CustomJSONEncoder, indent=2)
        channel_goal_performance_json = json.dumps(channel_goal_performance, cls=CustomJSONEncoder, indent=2)
        industry_benchmarks_json = json.dumps(industry_benchmarks, cls=CustomJSONEncoder, indent=2)
        
        # Create the chain
        chain = channel_prompt | llm | StrOutputParser()
        
        # Generate the insight
        insight = chain.invoke({
            "company_name": company_name,
            "channel_performance": channel_performance_json,
            "channel_segment_performance": channel_segment_performance_json,
            "channel_goal_performance": channel_goal_performance_json,
            "industry_benchmarks": industry_benchmarks_json
        })
        
        return insight
    except Exception as e:
        logger.error(f"Error generating channel insight: {str(e)}")
        raise  # Re-raise the exception for proper error handling
