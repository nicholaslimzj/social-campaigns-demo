"""
Segment Performance Insights Generator for the Meta Demo project.

This module generates AI-powered insights about segment performance
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

# System prompt template for segment performance insights
SEGMENT_SYSTEM_TEMPLATE = """
You are an expert marketing analyst specializing in customer segmentation and audience targeting.
Your task is to analyze segment performance data and provide clear, actionable insights.

Follow these guidelines:
1. Identify the best and worst performing customer segments
2. Analyze why certain segments perform better than others
3. Compare segment performance to company and industry averages
4. Highlight segment-specific trends in key metrics (ROI, conversion rate, acquisition cost, CTR)
5. Provide 2-3 specific, actionable recommendations for segment targeting
6. Use a professional, data-driven tone
7. Keep your insights concise (3-5 sentences)

DO NOT:
- Make vague or generic statements that could apply to any segment
- Include raw numbers or statistics without context
- Use marketing jargon without explanation
- Make recommendations not supported by the data

Your insights should help the marketing team optimize their segment targeting strategy.
"""

# Human prompt template for segment performance insights
SEGMENT_HUMAN_TEMPLATE = """
Analyze the following segment performance data for {company_name} and provide a concise summary with actionable insights.

Segment Performance Overview:
```json
{segment_performance}
```

Segment Rankings:
```json
{segment_rankings}
```

Segment-Goal Performance Matrix:
```json
{segment_goal_matrix}
```

Industry Segment Benchmarks:
```json
{industry_benchmarks}
```

Please provide a concise summary (3-5 sentences) that highlights key segment performance insights and 2-3 specific recommendations for optimizing segment targeting.
"""

# Create the prompt template
segment_prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(SEGMENT_SYSTEM_TEMPLATE),
    HumanMessagePromptTemplate.from_template(SEGMENT_HUMAN_TEMPLATE)
])

def get_segment_performance(company_name: str) -> List[Dict[str, Any]]:
    """
    Get performance metrics for all segments for a company.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Performance metrics for all segments
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Query for segment performance from segment_historical_metrics
        result = conn.execute("""
        SELECT 
            segment,
            avg_roi,
            avg_conversion_rate,
            avg_acquisition_cost,
            avg_ctr,
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
        FROM segment_historical_metrics
        ORDER BY composite_score DESC
        """).fetchdf()
        
        # If no data in the mart, fall back to segment_company_historical_rankings
        if result.empty:
            result = conn.execute("""
            SELECT 
                Customer_Segment as segment,
                avg_roi,
                avg_conversion_rate,
                avg_acquisition_cost,
                overall_ctr as avg_ctr,
                campaign_count,
                NULL as total_spend,
                NULL as total_revenue,
                roi_rank,
                conversion_rate_rank,
                acquisition_cost_rank,
                ctr_rank,
                NULL as composite_score,
                roi_vs_segment_avg as vs_company_avg,
                roi_vs_global_avg as vs_global_avg
            FROM segment_company_historical_rankings
            WHERE company_id = ?
            ORDER BY roi_rank
            """, [company_name]).fetchdf()
        
        # If still no data, fall back to stg_campaigns
        if result.empty:
            result = conn.execute("""
            SELECT 
                Customer_Segment as segment,
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
            GROUP BY Customer_Segment
            ORDER BY avg_roi DESC
            """, [company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting segment performance: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_segment_rankings(company_name: str) -> List[Dict[str, Any]]:
    """
    Get segment rankings based on the segment_company_historical_rankings model.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Segment rankings for the company
    """
    conn = None
    try:
        conn = get_analytics_connection()
        
        # Query for segment rankings from segment_company_historical_rankings
        result = conn.execute("""
        SELECT 
            Customer_Segment as segment,
            avg_conversion_rate,
            avg_roi,
            avg_acquisition_cost,
            overall_ctr,
            campaign_count,
            conversion_rate_rank,
            roi_rank,
            acquisition_cost_rank,
            ctr_rank,
            is_top_conversion_company,
            is_top_roi_company,
            is_top_acquisition_cost_company,
            is_top_ctr_company,
            conversion_rate_vs_segment_avg,
            roi_vs_segment_avg,
            acquisition_cost_vs_segment_avg,
            ctr_vs_segment_avg,
            conversion_rate_vs_global_avg,
            roi_vs_global_avg,
            acquisition_cost_vs_global_avg,
            ctr_vs_global_avg
        FROM segment_company_historical_rankings
        ORDER BY roi_rank
        """).fetchdf()
        
        # If no data in the mart, fall back to dimensions_quarterly_performance_rankings
        if result.empty:
            result = conn.execute("""
            SELECT 
                dimension_value as segment,
                avg_conversion_rate,
                avg_roi,
                avg_acquisition_cost,
                avg_ctr as overall_ctr,
                campaign_count,
                conversion_rate_rank,
                roi_rank,
                acquisition_cost_rank,
                ctr_rank,
                CASE WHEN conversion_rate_rank = 1 THEN 1 ELSE 0 END as is_top_conversion_company,
                CASE WHEN roi_rank = 1 THEN 1 ELSE 0 END as is_top_roi_company,
                CASE WHEN acquisition_cost_rank = 1 THEN 1 ELSE 0 END as is_top_acquisition_cost_company,
                CASE WHEN ctr_rank = 1 THEN 1 ELSE 0 END as is_top_ctr_company,
                vs_company_avg as conversion_rate_vs_segment_avg,
                vs_company_avg as roi_vs_segment_avg,
                vs_company_avg as acquisition_cost_vs_segment_avg,
                vs_company_avg as ctr_vs_segment_avg,
                vs_global_avg as conversion_rate_vs_global_avg,
                vs_global_avg as roi_vs_global_avg,
                vs_global_avg as acquisition_cost_vs_global_avg,
                vs_global_avg as ctr_vs_global_avg
            FROM dimensions_quarterly_performance_rankings
            WHERE dimension_type = 'Customer Segment'
            ORDER BY roi_rank
            """).fetchdf()
            
        # If still no data, fall back to a simplified query on stg_campaigns
        if result.empty:
            result = conn.execute("""
            WITH segment_metrics AS (
                SELECT 
                    Customer_Segment as segment,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(ROI) as avg_roi,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr,
                    COUNT(*) as campaign_count
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY Customer_Segment
            ),
            segment_ranks AS (
                SELECT 
                    segment,
                    avg_conversion_rate,
                    avg_roi,
                    avg_acquisition_cost,
                    overall_ctr,
                    campaign_count,
                    ROW_NUMBER() OVER (ORDER BY avg_conversion_rate DESC) as conversion_rate_rank,
                    ROW_NUMBER() OVER (ORDER BY avg_roi DESC) as roi_rank,
                    ROW_NUMBER() OVER (ORDER BY avg_acquisition_cost) as acquisition_cost_rank,
                    ROW_NUMBER() OVER (ORDER BY overall_ctr DESC) as ctr_rank
                FROM segment_metrics
            ),
            global_avgs AS (
                SELECT 
                    AVG(Conversion_Rate) as global_avg_conversion_rate,
                    AVG(ROI) as global_avg_roi,
                    AVG(Acquisition_Cost) as global_avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as global_avg_ctr
                FROM stg_campaigns
            ),
            company_avgs AS (
                SELECT 
                    AVG(Conversion_Rate) as company_avg_conversion_rate,
                    AVG(ROI) as company_avg_roi,
                    AVG(Acquisition_Cost) as company_avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as company_avg_ctr
                FROM stg_campaigns
                WHERE Company = ?
            )
            SELECT 
                sr.segment,
                sr.avg_conversion_rate,
                sr.avg_roi,
                sr.avg_acquisition_cost,
                sr.overall_ctr,
                sr.campaign_count,
                sr.conversion_rate_rank,
                sr.roi_rank,
                sr.acquisition_cost_rank,
                sr.ctr_rank,
                CASE WHEN sr.conversion_rate_rank = 1 THEN 1 ELSE 0 END as is_top_conversion_company,
                CASE WHEN sr.roi_rank = 1 THEN 1 ELSE 0 END as is_top_roi_company,
                CASE WHEN sr.acquisition_cost_rank = 1 THEN 1 ELSE 0 END as is_top_acquisition_cost_company,
                CASE WHEN sr.ctr_rank = 1 THEN 1 ELSE 0 END as is_top_ctr_company,
                (sr.avg_conversion_rate / NULLIF(ca.company_avg_conversion_rate, 0)) - 1 as conversion_rate_vs_segment_avg,
                (sr.avg_roi / NULLIF(ca.company_avg_roi, 0)) - 1 as roi_vs_segment_avg,
                (ca.company_avg_acquisition_cost / NULLIF(sr.avg_acquisition_cost, 0)) - 1 as acquisition_cost_vs_segment_avg,
                (sr.overall_ctr / NULLIF(ca.company_avg_ctr, 0)) - 1 as ctr_vs_segment_avg,
                (sr.avg_conversion_rate / NULLIF(ga.global_avg_conversion_rate, 0)) - 1 as conversion_rate_vs_global_avg,
                (sr.avg_roi / NULLIF(ga.global_avg_roi, 0)) - 1 as roi_vs_global_avg,
                (ga.global_avg_acquisition_cost / NULLIF(sr.avg_acquisition_cost, 0)) - 1 as acquisition_cost_vs_global_avg,
                (sr.overall_ctr / NULLIF(ga.global_avg_ctr, 0)) - 1 as ctr_vs_global_avg
            FROM segment_ranks sr
            CROSS JOIN global_avgs ga
            CROSS JOIN company_avgs ca
            ORDER BY sr.roi_rank
            """, [company_name, company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting segment rankings: {str(e)}")
        if conn:
            conn.close()
        raise  # Re-raise the exception after closing the connection
    finally:
        if conn:
            conn.close()

def get_segment_goal_matrix(company_name: str) -> List[Dict[str, Any]]:
    """
    Get the segment-goal performance matrix from the campaign_historical_performance_matrix model.
    
    Args:
        company_name: The name of the company
        
    Returns:
        List[Dict[str, Any]]: Segment-goal performance matrix
    """
    try:
        conn = get_analytics_connection()
        
        # Query for segment-goal matrix from campaign_historical_performance_matrix
        result = conn.execute("""
        SELECT 
            Campaign_Goal as goal,
            Customer_Segment as segment,
            avg_conversion_rate,
            avg_roi,
            avg_acquisition_cost,
            avg_ctr,
            campaign_count,
            composite_score,
            vs_goal_avg,
            vs_segment_avg,
            vs_global_avg,
            is_top_performer,
            rank_within_goal,
            rank_within_segment
        FROM campaign_historical_performance_matrix
        WHERE Company = ?
        ORDER BY composite_score DESC
        """, [company_name]).fetchdf()
        
        # If no data in the mart, fall back to a simplified query on stg_campaigns
        if result.empty:
            result = conn.execute("""
            WITH campaign_combos AS (
                SELECT 
                    Campaign_Goal as goal,
                    Customer_Segment as segment,
                    AVG(Conversion_Rate) as avg_conversion_rate,
                    AVG(ROI) as avg_roi,
                    AVG(Acquisition_Cost) as avg_acquisition_cost,
                    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                    COUNT(*) as campaign_count
                FROM stg_campaigns
                WHERE Company = ?
                GROUP BY Campaign_Goal, Customer_Segment
                HAVING campaign_count >= 2
            ),
            goal_avgs AS (
                SELECT 
                    goal,
                    AVG(avg_conversion_rate) as goal_avg_conversion_rate,
                    AVG(avg_roi) as goal_avg_roi,
                    AVG(avg_acquisition_cost) as goal_avg_acquisition_cost,
                    AVG(avg_ctr) as goal_avg_ctr
                FROM campaign_combos
                GROUP BY goal
            ),
            segment_avgs AS (
                SELECT 
                    segment,
                    AVG(avg_conversion_rate) as segment_avg_conversion_rate,
                    AVG(avg_roi) as segment_avg_roi,
                    AVG(avg_acquisition_cost) as segment_avg_acquisition_cost,
                    AVG(avg_ctr) as segment_avg_ctr
                FROM campaign_combos
                GROUP BY segment
            ),
            global_avgs AS (
                SELECT 
                    AVG(avg_conversion_rate) as global_avg_conversion_rate,
                    AVG(avg_roi) as global_avg_roi,
                    AVG(avg_acquisition_cost) as global_avg_acquisition_cost,
                    AVG(avg_ctr) as global_avg_ctr
                FROM campaign_combos
            ),
            normalized_metrics AS (
                SELECT 
                    cc.goal,
                    cc.segment,
                    cc.avg_conversion_rate,
                    cc.avg_roi,
                    cc.avg_acquisition_cost,
                    cc.avg_ctr,
                    cc.campaign_count,
                    cc.avg_conversion_rate / NULLIF((SELECT MAX(avg_conversion_rate) FROM campaign_combos), 0) as norm_conversion_rate,
                    cc.avg_roi / NULLIF((SELECT MAX(avg_roi) FROM campaign_combos), 0) as norm_roi,
                    (SELECT MIN(avg_acquisition_cost) FROM campaign_combos) / NULLIF(cc.avg_acquisition_cost, 0) as norm_acquisition_cost,
                    cc.avg_ctr / NULLIF((SELECT MAX(avg_ctr) FROM campaign_combos), 0) as norm_ctr
                FROM campaign_combos cc
            )
            SELECT 
                nm.goal,
                nm.segment,
                nm.avg_conversion_rate,
                nm.avg_roi,
                nm.avg_acquisition_cost,
                nm.avg_ctr,
                nm.campaign_count,
                (nm.norm_roi * 0.4) + (nm.norm_conversion_rate * 0.3) + (nm.norm_acquisition_cost * 0.2) + (nm.norm_ctr * 0.1) as composite_score,
                (nm.avg_roi / NULLIF(ga.goal_avg_roi, 0)) - 1 as vs_goal_avg,
                (nm.avg_roi / NULLIF(sa.segment_avg_roi, 0)) - 1 as vs_segment_avg,
                (nm.avg_roi / NULLIF(gla.global_avg_roi, 0)) - 1 as vs_global_avg,
                CASE WHEN nm.avg_roi > gla.global_avg_roi * 1.1 THEN 1 ELSE 0 END as is_top_performer,
                ROW_NUMBER() OVER (PARTITION BY nm.goal ORDER BY nm.avg_roi DESC) as rank_within_goal,
                ROW_NUMBER() OVER (PARTITION BY nm.segment ORDER BY nm.avg_roi DESC) as rank_within_segment
            FROM normalized_metrics nm
            JOIN goal_avgs ga ON nm.goal = ga.goal
            JOIN segment_avgs sa ON nm.segment = sa.segment
            CROSS JOIN global_avgs gla
            ORDER BY composite_score DESC
            """, [company_name, company_name]).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting segment-goal matrix: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def get_industry_segment_benchmarks() -> List[Dict[str, Any]]:
    """
    Get industry benchmarks for segments across all companies.
    
    Returns:
        List[Dict[str, Any]]: Industry benchmarks for segments
    """
    try:
        conn = get_analytics_connection()
        
        # Query for industry segment benchmarks from segment_historical_metrics
        result = conn.execute("""
        SELECT 
            segment,
            AVG(avg_roi) as avg_roi,
            AVG(avg_conversion_rate) as avg_conversion_rate,
            AVG(avg_acquisition_cost) as avg_acquisition_cost,
            AVG(avg_ctr) as avg_ctr,
            SUM(campaign_count) as campaign_count,
            COUNT(DISTINCT Company) as company_count,
            AVG(composite_score) as avg_composite_score
        FROM segment_historical_metrics
        GROUP BY segment
        ORDER BY avg_roi DESC
        """).fetchdf()
        
        # If no data in the mart, fall back to stg_campaigns
        if result.empty:
            result = conn.execute("""
            SELECT 
                Customer_Segment as segment,
                AVG(ROI) as avg_roi,
                AVG(Conversion_Rate) as avg_conversion_rate,
                AVG(Acquisition_Cost) as avg_acquisition_cost,
                CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as avg_ctr,
                COUNT(*) as campaign_count,
                COUNT(DISTINCT Company) as company_count,
                NULL as avg_composite_score
            FROM stg_campaigns
            GROUP BY Customer_Segment
            ORDER BY avg_roi DESC
            """).fetchdf()
        
        # Convert to list of dictionaries
        return result.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error getting industry segment benchmarks: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def generate_segment_insight(llm, company_name: str) -> str:
    """
    Generate segment performance insights.
    
    Args:
        llm: The language model to use
        company_name: The name of the company
        
    Returns:
        str: The generated insight
    """
    try:
        # Get data for the company
        segment_performance = get_segment_performance(company_name)
        segment_rankings = get_segment_rankings(company_name)
        segment_goal_matrix = get_segment_goal_matrix(company_name)
        industry_benchmarks = get_industry_segment_benchmarks()
        
        # Check if we have enough data
        if not segment_performance:
            return f"Insufficient segment data available for {company_name}."
        
        # Convert data to JSON strings
        segment_performance_json = json.dumps(segment_performance, cls=CustomJSONEncoder, indent=2)
        segment_rankings_json = json.dumps(segment_rankings, cls=CustomJSONEncoder, indent=2)
        segment_goal_matrix_json = json.dumps(segment_goal_matrix, cls=CustomJSONEncoder, indent=2)
        industry_benchmarks_json = json.dumps(industry_benchmarks, cls=CustomJSONEncoder, indent=2)
        
        # Create the chain
        chain = segment_prompt | llm | StrOutputParser()
        
        # Generate the insight
        insight = chain.invoke({
            "company_name": company_name,
            "segment_performance": segment_performance_json,
            "segment_rankings": segment_rankings_json,
            "segment_goal_matrix": segment_goal_matrix_json,
            "industry_benchmarks": industry_benchmarks_json
        })
        
        return insight
    except Exception as e:
        logger.error(f"Error generating segment insight: {str(e)}")
        raise  # Re-raise the exception for proper error handling
