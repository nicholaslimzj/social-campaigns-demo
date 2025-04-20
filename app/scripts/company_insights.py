"""
Company Performance Insights Generator for the Meta Demo project.

This module generates AI-powered insights about overall company performance
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

# System prompt template for company performance insights
COMPANY_SYSTEM_TEMPLATE = """
You are an expert marketing analyst specializing in social media advertising campaigns.
Your task is to analyze campaign performance data and provide clear, actionable insights.

Follow these guidelines:
1. Structure your response as 2-3 distinct paragraphs, each focusing on a different key insight
2. Each paragraph should contain ONE key insight followed by ONE specific, actionable recommendation related to that insight
3. Make each paragraph self-contained and independently valuable
4. Highlight performance across key metrics (ROI, conversion rate, acquisition cost, CTR)
5. Compare performance to industry benchmarks where available
6. Use a professional, data-driven tone
7. Keep each paragraph extremely concise (1-3 sentences for the insight + 1 sentence for the recommendation)

DO NOT:
- Use fluff words like "significantly" unless the difference is truly substantial (>10%)
- Make vague or generic statements that could apply to any company
- Include raw numbers or statistics without context
- Use marketing jargon without explanation
- Make recommendations not supported by the data
- Use bullet points or numbered lists
- Include headers or section titles
- Add unnecessary adjectives or adverbs

Your insights should be factual, precise, and actionable, with each paragraph being displayable in a carousel-style UI.
"""

# Human prompt template for company performance insights
COMPANY_HUMAN_TEMPLATE = """
Analyze the following performance data for {company_name} and provide insights on their campaign performance.

Company Metrics:
```json
{company_metrics}
```

Time Series Data:
```json
{time_series_data}
```

Top Performing Segments:
```json
{top_segments}
```

Top Performing Channels:
```json
{top_channels}
```

Format your response as 2-3 distinct paragraphs. Each paragraph should contain ONE key insight followed by ONE specific, actionable recommendation related to that insight. Make each paragraph self-contained so it can be displayed independently in a carousel-style UI. Do not use bullet points, numbered lists, or section headers.
"""

# Create the prompt template
company_prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(COMPANY_SYSTEM_TEMPLATE),
    HumanMessagePromptTemplate.from_template(COMPANY_HUMAN_TEMPLATE)
])

def get_company_metrics(company_name: str) -> Dict[str, Any]:
    """
    Get overall metrics for a company.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict[str, Any]: Overall metrics for the company
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Query for overall company metrics from stg_campaigns for basic metrics
        basic_metrics = conn.execute("""
        SELECT 
            AVG(ROI) as avg_roi,
            AVG(Conversion_Rate) as avg_conversion_rate,
            AVG(Acquisition_Cost) as avg_acquisition_cost,
            CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
            COUNT(*) as campaign_count,
            COUNT(DISTINCT Campaign_Goal) as goal_count,
            COUNT(DISTINCT Customer_Segment) as segment_count,
            COUNT(DISTINCT Channel_Used) as channel_count,
            MIN(StandardizedDate) as first_campaign_date,
            MAX(StandardizedDate) as last_campaign_date
        FROM stg_campaigns
        WHERE Company = ?
        """, [company_name]).fetchone()
        
        if not basic_metrics:
            return {}
        
        # Convert to dictionary
        metrics = {
            "avg_roi": basic_metrics[0],
            "avg_conversion_rate": basic_metrics[1],
            "avg_acquisition_cost": basic_metrics[2],
            "overall_ctr": basic_metrics[3],
            "campaign_count": basic_metrics[4],
            "goal_count": basic_metrics[5],
            "segment_count": basic_metrics[6],
            "channel_count": basic_metrics[7],
            "first_campaign_date": basic_metrics[8],
            "last_campaign_date": basic_metrics[9]
        }
        
        # Get trend information from metrics_monthly_trends
        trend_metrics = conn.execute("""
        SELECT 
            roi_trend_change,
            conversion_rate_trend_change,
            acquisition_cost_trend_change,
            ctr_trend_change,
            has_trend_change
        FROM metrics_monthly_trends
        WHERE Company = ?
        ORDER BY month DESC
        LIMIT 1
        """, [company_name]).fetchone()
        
        if trend_metrics:
            metrics["roi_trend"] = trend_metrics[0]
            metrics["conversion_rate_trend"] = trend_metrics[1]
            metrics["acquisition_cost_trend"] = trend_metrics[2]
            metrics["ctr_trend"] = trend_metrics[3]
            metrics["has_trend_change"] = trend_metrics[4]
            
        # Get quarterly performance rankings from dimensions_quarterly_performance_rankings
        quarterly_rankings = conn.execute("""
        SELECT 
            metric,
            metric_value,
            metric_rank,
            total_entities,
            campaign_count,
            prev_metric_value,
            metric_qoq_change,
            trend,
            current_window_start_month,
            current_window_end_month
        FROM dimensions_quarterly_performance_rankings
        WHERE dimension = 'company' AND entity = ?
        """, [company_name]).fetchdf()
        
        # Add quarterly rankings to metrics
        if not quarterly_rankings.empty:
            metrics["quarterly_rankings"] = {}
            
            # Process each metric type
            for _, row in quarterly_rankings.iterrows():
                metric_name = row["metric"]
                metrics["quarterly_rankings"][metric_name] = {
                    "value": row["metric_value"],
                    "rank": row["metric_rank"],
                    "total_companies": row["total_entities"],
                    "percentile": 100 - (row["metric_rank"] / row["total_entities"] * 100) if row["total_entities"] > 0 else 0,
                    "previous_value": row["prev_metric_value"],
                    "change_percent": row["metric_qoq_change"],
                    "trend": row["trend"],
                    "quarter_months": f"{row['current_window_start_month']}-{row['current_window_end_month']}"
                }
        
        # Get industry averages for comparison
        # Get industry averages from segment_company_historical_rankings
        # A company is in one industry/segment only, so we can just fetch that record
        industry_avg = conn.execute("""
        SELECT 
            segment_avg_roi as industry_avg_roi,
            segment_avg_conversion_rate as industry_avg_conversion_rate,
            segment_avg_acquisition_cost as industry_avg_acquisition_cost,
            segment_avg_ctr as industry_avg_ctr
        FROM segment_company_historical_rankings
        WHERE Company = ?
        LIMIT 1
        """, [company_name]).fetchone()
        
        metrics["industry_avg_roi"] = industry_avg[0]
        metrics["industry_avg_conversion_rate"] = industry_avg[1]
        metrics["industry_avg_acquisition_cost"] = industry_avg[2]
        metrics["industry_avg_ctr"] = industry_avg[3]
        
        # Calculate performance vs industry
        metrics["roi_vs_industry"] = (metrics["avg_roi"] / industry_avg[0]) - 1 if industry_avg[0] else 0
        metrics["conversion_rate_vs_industry"] = (metrics["avg_conversion_rate"] / industry_avg[1]) - 1 if industry_avg[1] else 0
        metrics["acquisition_cost_vs_industry"] = (industry_avg[2] / metrics["avg_acquisition_cost"]) - 1 if metrics["avg_acquisition_cost"] else 0
        metrics["ctr_vs_industry"] = (metrics["overall_ctr"] / industry_avg[3]) - 1 if industry_avg[3] else 0
        
        # Get anomalies from metrics_historical_anomalies
        anomalies = conn.execute("""
        SELECT 
            month,
            CASE 
                WHEN conversion_rate_anomaly = 'anomaly' THEN 'conversion_rate'
                WHEN roi_anomaly = 'anomaly' THEN 'roi'
                WHEN acquisition_cost_anomaly = 'anomaly' THEN 'acquisition_cost'
                WHEN clicks_anomaly = 'anomaly' THEN 'clicks'
                ELSE NULL
            END as metric_name,
            CASE 
                WHEN conversion_rate_z > 0 OR roi_z > 0 OR clicks_z > 0 THEN 'positive'
                WHEN acquisition_cost_z < 0 THEN 'positive' -- lower cost is good
                ELSE 'negative'
            END as anomaly_type,
            COALESCE(avg_conversion_rate, avg_roi, avg_acquisition_cost, total_clicks) as anomaly_value,
            COALESCE(conversion_rate_mean, roi_mean, acquisition_cost_mean, clicks_mean) as expected_value,
            COALESCE(
                ABS(avg_conversion_rate / NULLIF(conversion_rate_mean, 0) - 1),
                ABS(avg_roi / NULLIF(roi_mean, 0) - 1),
                ABS(avg_acquisition_cost / NULLIF(acquisition_cost_mean, 0) - 1),
                ABS(total_clicks / NULLIF(clicks_mean, 0) - 1)
            ) * 100 as deviation_percentage,
            month as detection_date
        FROM metrics_historical_anomalies
        WHERE Company = ? AND (
            conversion_rate_anomaly = 'anomaly' OR 
            roi_anomaly = 'anomaly' OR 
            acquisition_cost_anomaly = 'anomaly' OR 
            clicks_anomaly = 'anomaly'
        )
        ORDER BY month DESC
        LIMIT 5
        """, [company_name]).fetchdf()
        
        if not anomalies.empty:
            metrics["anomalies"] = anomalies.to_dict(orient='records')
        
        return metrics
    except Exception as e:
        logger.error(f"Error getting company metrics: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_time_series_data(company_name: str) -> List[Dict[str, Any]]:
    """
    Get time series data for a company.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Time series data for the company
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Query for time series data from campaign_monthly_metrics
        # Note: We keep the Company filter here as indicated by the user
        result = conn.execute("""
        SELECT 
            month,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            total_spend,
            total_revenue,
            roi_vs_prev_month,
            conversion_rate_vs_prev_month,
            acquisition_cost_vs_prev_month,
            ctr_vs_prev_month
        FROM campaign_monthly_metrics
        WHERE Company = ?
        ORDER BY month
        """, [company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting time series data: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_top_segments(company_name: str, limit: int = 3) -> List[Dict[str, Any]]:
    """
    Get the company's segment information.
    
    Args:
        company_name: The name of the company
        limit: Not used (kept for API compatibility)
        
    Returns:
        List[Dict[str, Any]]: Company's segment information
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # A company is only in ONE segment, so we just get that segment's info
        result = conn.execute("""
        SELECT 
            Customer_Segment as segment,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            overall_ctr as avg_ctr,
            campaign_count,
            roi_rank,
            conversion_rate_rank,
            acquisition_cost_rank,
            ctr_rank,
            roi_vs_segment_avg as vs_company_avg,
            roi_vs_global_avg as vs_global_avg,
            is_top_conversion_company,
            is_top_roi_company,
            is_top_acquisition_cost_company,
            is_top_ctr_company
        FROM segment_company_historical_rankings
        WHERE Company = ?
        LIMIT 1
        """, [company_name]).fetchdf()
        
        # No fallback needed as per user instruction
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting segment information: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_top_channels(company_name: str, limit: int = 3) -> List[Dict[str, Any]]:
    """
    Get top performing channels for a company.
    
    Args:
        company_name: The name of the company
        limit: Number of top channels to return
        
    Returns:
        List[Dict[str, Any]]: Top performing channels for the company
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Get date ranges using the same approach as in dimensions_quarterly_performance_rankings.sql
        date_ranges = conn.execute("""
        SELECT MAX(EXTRACT(MONTH FROM CAST(StandardizedDate AS DATE))) AS current_max_month
        FROM stg_campaigns
        """).fetchone()
        
        current_max_month = date_ranges[0]
        
        # Query for top channels from the last 3 months of available data
        result = conn.execute(f"""
        WITH date_ranges AS (
            SELECT MAX(EXTRACT(MONTH FROM CAST(StandardizedDate AS DATE))) AS current_max_month
            FROM stg_campaigns
        )
        SELECT 
            Channel_Used as channel,
            AVG(ROI) as avg_roi,
            AVG(Conversion_Rate) as avg_conversion_rate,
            AVG(Acquisition_Cost) as avg_acquisition_cost,
            CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
            COUNT(*) as campaign_count,
            NULL as roi_rank,
            NULL as conversion_rate_rank,
            NULL as acquisition_cost_rank,
            NULL as ctr_rank,
            NULL as composite_score,
            NULL as vs_company_avg,
            NULL as vs_global_avg
        FROM stg_campaigns
        WHERE Company = ?
          AND EXTRACT(MONTH FROM CAST(StandardizedDate AS DATE)) >= 
              (SELECT current_max_month - 2 FROM date_ranges)
        GROUP BY Channel_Used
        ORDER BY avg_roi DESC
        LIMIT {limit}
        """, [company_name]).fetchdf()
        
        # No fallback needed as we're directly using stg_campaigns
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting top channels: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def generate_company_insight(llm, company_name: str) -> str:
    """
    Generate company performance insights.
    
    Args:
        llm: The language model to use
        company_name: The name of the company
        
    Returns:
        str: The generated insight
    """
    try:
        # Get data for the company
        company_metrics = get_company_metrics(company_name)
        time_series_data = get_time_series_data(company_name)
        top_segments = get_top_segments(company_name)
        top_channels = get_top_channels(company_name)
        
        # Check if we have enough data
        if not company_metrics or not time_series_data:
            return f"Insufficient data available for {company_name}."
        
        # Convert data to JSON strings
        company_metrics_json = json.dumps(company_metrics, cls=CustomJSONEncoder, indent=2)
        time_series_json = json.dumps(time_series_data, cls=CustomJSONEncoder, indent=2)
        top_segments_json = json.dumps(top_segments, cls=CustomJSONEncoder, indent=2)
        top_channels_json = json.dumps(top_channels, cls=CustomJSONEncoder, indent=2)
        
        # Create the chain
        chain = company_prompt | llm | StrOutputParser()
        
        # Generate the insight
        insight = chain.invoke({
            "company_name": company_name,
            "company_metrics": company_metrics_json,
            "time_series_data": time_series_json,
            "top_segments": top_segments_json,
            "top_channels": top_channels_json
        })
        
        return insight
    except Exception as e:
        logger.error(f"Error generating company insight: {str(e)}")
        raise  # Re-raise the exception for proper error handling
