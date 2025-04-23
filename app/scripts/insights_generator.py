"""
Insights Generator for the Meta Demo dashboard.

Generates concise, actionable insights from campaign performance data
using the latest data models and LLM summarization.
"""

import os
import logging
import json
import duckdb
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, date, timedelta
from decimal import Decimal

# LangChain imports
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path for the insights cache database
INSIGHTS_DB_PATH = '/data/db/insights_cache.duckdb'

# Custom JSON encoder to handle datetime and Decimal objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def get_insights_connection() -> duckdb.DuckDBPyConnection:
    """Get a connection to the insights cache database."""
    try:
        # Create the directory if it doesn't exist
        db_dir = os.path.dirname(INSIGHTS_DB_PATH)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Connect to the database
        conn = duckdb.connect(INSIGHTS_DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to insights cache DB: {str(e)}")
        raise

def get_analytics_connection() -> duckdb.DuckDBPyConnection:
    """Get a connection to the analytics database."""
    try:
        # Connect to the analytics database
        conn = duckdb.connect('/data/db/meta_analytics.duckdb')
        
        # Set up DATA_ROOT macro
        conn.execute("CREATE OR REPLACE MACRO DATA_ROOT() AS '/data'")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to analytics DB: {str(e)}")
        raise

def setup_insights_cache():
    """Set up the insights cache table if it doesn't exist."""
    try:
        conn = get_insights_connection()
        
        # Check if the table already exists
        table_exists = conn.execute("""
        SELECT count(*) FROM sqlite_master 
        WHERE type='table' AND name='insights_cache'
        """).fetchone()[0] > 0
        
        if not table_exists:
            # Create insights cache table if it doesn't exist
            conn.execute("""
            CREATE TABLE insights_cache (
                company_name VARCHAR,
                generated_at TIMESTAMP,
                insight_text TEXT,
                insight_type VARCHAR DEFAULT 'company',
                PRIMARY KEY (company_name)
            )
            """)
            logger.info("Created insights_cache table")
        else:
            # Log the schema of the existing table
            schema = conn.execute("PRAGMA table_info(insights_cache)").fetchall()
            logger.info(f"Existing insights_cache table schema: {schema}")
    except Exception as e:
        logger.error(f"Error setting up insights cache: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def get_cached_insight(company_name: str) -> Optional[str]:
    """
    Get a cached insight if it exists and is not too old.
    
    Args:
        company_name: The name of the company
        
    Returns:
        The cached insight text, or None if no valid cache exists
    """
    try:
        conn = get_insights_connection()
        
        # Ensure the cache table exists
        setup_insights_cache()
        
        # Get the cached insight
        result = conn.execute(
            """SELECT insight_text FROM insights_cache 
            WHERE company_name = ? 
            AND generated_at > ?""", 
            [company_name, datetime.now() - timedelta(hours=24)]
        ).fetchone()
        
        conn.close()
        
        if result:
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Error getting cached insight: {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def cache_insight(company_name: str, insight_text: str, insight_type: str = 'company') -> None:
    """Cache the insight for a company.
    
    Args:
        company_name: The name of the company
        insight_text: The insight text to cache (HTML with Tailwind CSS)
        insight_type: The type of insight (default: 'company')
    """
    try:
        conn = get_insights_connection()
        
        # Ensure the cache table exists
        setup_insights_cache()
        
        # Check if insight exists
        existing = conn.execute(
            "SELECT COUNT(*) FROM insights_cache WHERE company_name = ?", 
            [company_name]
        ).fetchone()[0]
        
        if existing > 0:
            # Update existing insight
            conn.execute(
                "UPDATE insights_cache SET insight_text = ?, generated_at = ?, insight_type = ? WHERE company_name = ?",
                [insight_text, datetime.now(), insight_type, company_name]
            )
        else:
            # Insert new insight
            conn.execute(
                "INSERT INTO insights_cache (company_name, insight_text, generated_at, insight_type) VALUES (?, ?, ?, ?)",
                [company_name, insight_text, datetime.now(), insight_type]
            )
        
        conn.commit()
        conn.close()
        
        # Also save to a file for easy access
        insights_dir = Path(__file__).parent.parent / "static" / "insights"
        insights_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = insights_dir / f"{company_name.replace(' ', '_').lower()}_insights.html"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(insight_text)
            
        logger.info(f"Saved insight to file: {file_path}")
    except Exception as e:
        logger.error(f"Error caching insight: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_company_metrics(company_name: str) -> Dict[str, Any]:
    """
    Fetch overall company metrics from the monthly metrics model.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict containing company metrics
    """
    try:
        conn = get_analytics_connection()
        
        # Get the latest month's metrics and compare to previous month
        metrics = conn.execute("""
        WITH current_metrics AS (
            SELECT 
                AVG(roi_vs_prev_month) as current_roi,
                AVG(conversion_rate_vs_prev_month) as current_conversion_rate,
                SUM(total_revenue) as current_revenue,
                MAX(month) as current_month
            FROM campaign_monthly_metrics
            WHERE Company = ?
            AND month = (SELECT MAX(month) FROM campaign_monthly_metrics WHERE Company = ?)
        ),
        previous_metrics AS (
            SELECT 
                AVG(roi_vs_prev_month) as previous_roi,
                AVG(conversion_rate_vs_prev_month) as previous_conversion_rate,
                SUM(total_revenue) as previous_revenue,
                MAX(month) as previous_month
            FROM campaign_monthly_metrics
            WHERE Company = ?
            AND month = (
                SELECT MAX(month) FROM campaign_monthly_metrics 
                WHERE Company = ? 
                AND month < (SELECT MAX(month) FROM campaign_monthly_metrics WHERE Company = ?)
            )
        )
        SELECT 
            cm.current_month,
            pm.previous_month,
            cm.current_roi,
            pm.previous_roi,
            (cm.current_roi - pm.previous_roi) / NULLIF(pm.previous_roi, 0) * 100 as roi_change_pct,
            cm.current_conversion_rate,
            pm.previous_conversion_rate,
            (cm.current_conversion_rate - pm.previous_conversion_rate) / NULLIF(pm.previous_conversion_rate, 0) * 100 as conversion_rate_change_pct,
            cm.current_revenue as current_revenue,
            pm.previous_revenue as previous_revenue,
            (cm.current_revenue - pm.previous_revenue) / NULLIF(pm.previous_revenue, 0) * 100 as revenue_change_pct
        FROM current_metrics cm, previous_metrics pm
        """, [company_name, company_name, company_name, company_name, company_name]).fetchone()
        
        if not metrics:
            return {}
            
        return {
            "current_roi": metrics[2],
            "current_conversion_rate": metrics[5],
            "current_revenue": metrics[8],
            "current_month": metrics[0],
            "previous_roi": metrics[3],
            "previous_conversion_rate": metrics[6],
            "previous_revenue": metrics[9],
            "previous_month": metrics[1],
            "roi_change_pct": metrics[4],
            "conversion_rate_change_pct": metrics[7],
            "revenue_change_pct": metrics[10]
        }
    except Exception as e:
        logger.error(f"Error fetching company metrics: {str(e)}")
        return {}
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_campaign_rankings(company_name: str) -> Dict[str, Any]:
    """
    Fetch campaign performance rankings.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict containing top and bottom campaigns by different metrics
    """
    try:
        conn = get_analytics_connection()
        
        # Get top performers by each metric
        top_performers = {}
        metric_mapping = {
            "roi": "roi",
            "conversion_rate": "conversion",  
            "revenue": "revenue",
            "cpa": "cpa"
        }
        
        for metric_key, db_metric in metric_mapping.items():
            try:
                query = f"""
                SELECT *
                FROM campaign_month_performance_rankings
                WHERE Company = ?
                AND is_top_{db_metric}_performer = TRUE
                ORDER BY {db_metric}_rank
                LIMIT 3
                """
                
                results = conn.execute(query, [company_name]).fetchall()
                top_performers[metric_key] = results
            except Exception as e:
                logger.warning(f"Error fetching top performers for {metric_key}: {str(e)}")
                top_performers[metric_key] = []
        
        # Get bottom performers by each metric
        bottom_performers = {}
        for metric_key, db_metric in metric_mapping.items():
            try:
                query = f"""
                SELECT *
                FROM campaign_month_performance_rankings
                WHERE Company = ?
                AND is_bottom_{db_metric}_performer = TRUE
                ORDER BY {db_metric}_rank_asc
                LIMIT 3
                """
                
                results = conn.execute(query, [company_name]).fetchall()
                bottom_performers[metric_key] = results
            except Exception as e:
                logger.warning(f"Error fetching bottom performers for {metric_key}: {str(e)}")
                bottom_performers[metric_key] = []
            

        
        return {
            "top_performers": top_performers,
            "bottom_performers": bottom_performers
        }
    except Exception as e:
        logger.error(f"Error fetching campaign rankings: {str(e)}")
        return {"top_performers": {}, "bottom_performers": {}}
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_channel_insights(company_name: str) -> Dict[str, Any]:
    """
    Fetch channel performance insights.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict containing channel performance data
    """
    try:
        conn = get_analytics_connection()
        
        # Get top performing channels
        top_channels = conn.execute("""
        SELECT 
            Channel_Used,
            AVG(avg_conversion_rate) as avg_conversion_rate,
            SUM(channel_share_clicks) as share_clicks,
            COUNT(*) as channel_count
        FROM channel_monthly_metrics
        WHERE Company = ?
        GROUP BY Channel_Used
        ORDER BY avg_conversion_rate DESC
        LIMIT 3
        """, [company_name]).fetchall()
        
        # Get channel anomalies
        anomalies = conn.execute("""
        SELECT 
            Channel_Used as Channel,
            'spend' as metric,
            spend_anomaly as actual_value,
            0 as expected_value,
            CAST(1 as INTEGER) as z_score,
            'Spend anomaly detected' as explanation
        FROM channel_quarter_anomalies
        WHERE Company = ?
        AND has_anomaly = TRUE
        ORDER BY spend_anomaly DESC
        LIMIT 3
        """, [company_name]).fetchall()
        
        return {
            "top_channels": top_channels,
            "anomalies": anomalies
        }
    except Exception as e:
        logger.error(f"Error fetching channel insights: {str(e)}")
        return {"top_channels": [], "anomalies": []}
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_audience_insights(company_name: str) -> Dict[str, Any]:
    """
    Fetch audience performance insights.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict containing audience performance data
    """
    try:
        conn = get_analytics_connection()
        
        # Get top performing audiences
        top_audiences = conn.execute("""
        SELECT 
            'Audience ' || ROW_NUMBER() OVER (ORDER BY response_rate DESC) as audience_name,
            response_rate,
            total_spend,
            total_revenue
        FROM audience_monthly_metrics
        WHERE Company = ?
        ORDER BY response_rate DESC
        LIMIT 3
        """, [company_name]).fetchall()
        
        # Get audience anomalies
        anomalies = conn.execute("""
        SELECT 
            'Audience ' || ROW_NUMBER() OVER (ORDER BY ABS(revenue_z) DESC) as Audience,
            'revenue' as metric,
            revenue_z as actual_value,
            0 as expected_value,
            revenue_z as z_score,
            'Revenue anomaly detected' as explanation
        FROM audience_quarter_anomalies
        WHERE Company = ?
        ORDER BY ABS(revenue_z) DESC
        LIMIT 3
        """, [company_name]).fetchall()
        
        return {
            "top_audiences": top_audiences,
            "anomalies": anomalies
        }
    except Exception as e:
        logger.error(f"Error fetching audience insights: {str(e)}")
        return {"top_audiences": [], "anomalies": []}
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_campaign_duration_insights(company_name: str) -> Dict[str, Any]:
    """
    Fetch campaign duration optimization insights.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict containing campaign duration optimization data
    """
    try:
        conn = get_analytics_connection()
        
        # Get optimal durations by dimension
        optimal_durations = conn.execute("""
        SELECT 
            dimension,
            optimal_duration_range,
            optimal_duration_bucket,
            optimal_conversion_rate
        FROM campaign_duration_quarter_analysis
        WHERE Company = ?
        ORDER BY optimal_conversion_rate DESC
        LIMIT 5
        """, [company_name]).fetchall()
        
        # Get overall optimal duration
        overall = conn.execute("""
        SELECT 
            optimal_duration_range,
            optimal_conversion_rate
        FROM campaign_duration_quarter_analysis
        WHERE Company = ?
        LIMIT 1
        """, [company_name]).fetchone()
        
        return {
            "optimal_durations": optimal_durations,
            "overall_optimal_duration": overall[0] if overall else None,
            "overall_roi_impact": overall[1] if overall else None
        }
    except Exception as e:
        logger.error(f"Error fetching campaign duration insights: {str(e)}")
        return {"optimal_durations": [], "overall_optimal_duration": None, "overall_roi_impact": None}
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_campaign_clusters(company_name: str) -> Dict[str, Any]:
    """
    Fetch high-performing campaign clusters.
    
    Args:
        company_name: The name of the company
        
    Returns:
        Dict containing high-performing campaign clusters
    """
    try:
        conn = get_analytics_connection()
        
        # Get high ROI clusters
        high_roi = conn.execute("""
        SELECT 
            segment,
            min_duration,
            optimal_min_duration,
            optimal_max_duration,
            is_optimal_duration,
            'Increase budget for this segment' as action_recommendation
        FROM campaign_quarter_clusters
        WHERE Company = ?
        ORDER BY optimal_min_duration DESC
        LIMIT 3
        """, [company_name]).fetchall()
        
        # Get high conversion clusters
        high_conversion = conn.execute("""
        SELECT 
            segment,
            min_duration,
            optimal_min_duration,
            optimal_max_duration,
            is_optimal_duration,
            'Optimize for conversions' as action_recommendation
        FROM campaign_quarter_clusters
        WHERE Company = ?
        ORDER BY optimal_max_duration DESC
        LIMIT 3
        """, [company_name]).fetchall()
        
        return {
            "high_roi": high_roi,
            "high_conversion": high_conversion
        }
    except Exception as e:
        logger.error(f"Error fetching campaign clusters: {str(e)}")
        return {"high_roi": [], "high_conversion": []}
    finally:
        if 'conn' in locals():
            conn.close()

class InsightsGenerator:
    """Generate concise, actionable insights for marketing dashboard."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.5-pro-preview-03-25", temperature: float = 0.2):
        """
        Initialize the insights generator.
        
        Args:
            api_key: Google API key (optional, will use env var if not provided)
            model: LLM model to use
            temperature: Temperature for LLM generation
        """
        try:
            # Use provided API key or get from environment
            self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
            
            if not self.api_key:
                raise ValueError("No Google API key provided. Set GOOGLE_API_KEY environment variable or pass api_key.")
            
            # Initialize LLM
            self.llm = ChatGoogleGenerativeAI(
                model=model,
                temperature=temperature,
                google_api_key=self.api_key,
                convert_system_message_to_human=True
            )
            
            logger.info(f"Initialized InsightsGenerator with model {model}")
        except Exception as e:
            logger.error(f"Error initializing InsightsGenerator: {str(e)}")
            raise
    
    def generate_insight(self, company_name: str, force_refresh: bool = False) -> str:
        """
        Generate comprehensive insights for a company.
        
        Args:
            company_name: The name of the company
            force_refresh: Whether to force a refresh of the insight
            
        Returns:
            str: The generated insight
        """
        try:
            # Check cache first unless force refresh is requested
            if not force_refresh:
                cached_insight = get_cached_insight(company_name)
                if cached_insight:
                    return cached_insight
            
            # Fetch all required data
            company_metrics = fetch_company_metrics(company_name)
            campaign_rankings = fetch_campaign_rankings(company_name)
            channel_insights = fetch_channel_insights(company_name)
            audience_insights = fetch_audience_insights(company_name)
            duration_insights = fetch_campaign_duration_insights(company_name)
            campaign_clusters = fetch_campaign_clusters(company_name)
            
            # Prepare data for LLM
            data = {
                "company_name": company_name,
                "company_metrics": company_metrics,
                "campaign_rankings": campaign_rankings,
                "channel_insights": channel_insights,
                "audience_insights": audience_insights,
                "duration_insights": duration_insights,
                "campaign_clusters": campaign_clusters
            }
            
            # Convert to JSON for LLM
            data_json = json.dumps(data, cls=CustomJSONEncoder)
            
            # Log the data being sent to the LLM for debugging
            logger.info(f"Data for {company_name} insights generation: {data_json}")
            
            # Save the data to a debug file for inspection
            debug_dir = Path("/data/debug")
            debug_dir.mkdir(exist_ok=True)
            with open(debug_dir / f"{company_name}_insights_data.json", "w") as f:
                f.write(data_json)
            
            # Prepare the data for the LLM prompt
            
            # Directly use the LLM with a simple prompt that includes Tailwind formatting instructions
            prompt = f"""You are an expert marketing analyst who provides extremely concise, data-driven insights for social media marketing campaigns.
            
            Generate a brief, actionable single-paragraph summary for {company_name}'s marketing performance based on this data: {data_json}
            
            Format your response as compact HTML with Tailwind CSS classes, following this structure:
            
            <p class="text-gray-700">{company_name} experienced a <span class="text-green-500 font-semibold">+X%</span> ROI change with top campaign achieving <span class="text-blue-500 font-semibold">X</span> ROI. Channel <span class="text-blue-500 font-semibold">[name]</span> performed <span class="text-green-500 font-semibold">X%</span> above average ROI, while optimal campaign duration is <span class="text-blue-500 font-semibold">X days</span>. Overall ROI trend is <span class="text-red-500 font-semibold">-Y%</span> suggesting focusing on <span class="text-blue-500 font-semibold">[specific action]</span>.</p>
            
            BE EXTREMELY CONCISE. Create a single paragraph with 2-3 sentences maximum. Always highlight numbers and percentages with color spans (green for positive, red for negative, blue for neutral). Include only the most important metrics and actionable insights. The entire output should be very compact to fit in a small dashboard space.
            
            IMPORTANT: 
            1. Follow the exact pattern from the example above, with colored spans for all metrics and numbers. 
            2. Do not include separate sections or headings - just one concise paragraph.
            3. Be consistent with percentage values - if ROI change is -0.78%, report it exactly as -0.78%, not as -78% or -120%.
            4. Do not include backticks or markdown formatting in your response - output only the HTML.
            5. Make sure all percentage values are consistent with the data provided."""
            
            
            
            
            # Log the prompt being sent to the LLM
            logger.info(f"Prompt for {company_name} insights generation:\n{prompt}")
            
            # Generate insight using LLM directly
            llm_response = self.llm.invoke(prompt)
            
            # Log the raw response from the LLM
            logger.info(f"Raw LLM response for {company_name}:\n{llm_response}")
            
            # Extract the content from the AIMessage object
            if hasattr(llm_response, 'content'):
                insight_text = llm_response.content
            else:
                # If it's already a string, use it directly
                insight_text = str(llm_response)
            
            # Only cache and return the insight if it was generated successfully
            if insight_text and len(insight_text) > 0:
                # Clean up the response - remove any backticks or markdown formatting
                if insight_text.startswith('```html') and insight_text.endswith('```'):
                    insight_text = insight_text[7:-3]  # Remove ```html at the start and ``` at the end
                elif insight_text.startswith('`') and insight_text.endswith('`'):
                    insight_text = insight_text[1:-1]  # Remove single backticks
                
                logger.info(f"Generated insight for {company_name} with length {len(insight_text)}")
                cache_insight(company_name, insight_text, 'company')
                return insight_text
            else:
                logger.error(f"Failed to generate insights for {company_name}")
                return None
        except Exception as e:
            logger.error(f"Error generating insight for {company_name}: {str(e)}")
            raise

def generate_all_insights(force_refresh: bool = False) -> bool:
    """
    Generate insights for all companies in the database.
    
    Args:
        force_refresh: Whether to force a refresh of all insights
        
    Returns:
        bool: True if all successful, False if any failed
    """
    # Set up colored output for CLI
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"
    
    try:
        # Get all company names
        conn = get_analytics_connection()
        companies = conn.execute("SELECT DISTINCT Company FROM campaign_monthly_metrics ORDER BY Company").fetchall()
        conn.close()
        
        if not companies:
            print(f"{RED}No companies found in the database.{RESET}")
            return False
        
        print(f"\n{YELLOW}Generating insights for {len(companies)} companies...{RESET}\n")
        
        # Initialize the insights generator
        generator = InsightsGenerator()
        
        # Track successes and failures
        successes = 0
        failures = 0
        failed_companies = []
        
        # Generate insights for each company
        for i, (company,) in enumerate(companies, 1):
            print(f"[{i}/{len(companies)}] Generating insights for {company}...")
            try:
                insight = generator.generate_insight(company, force_refresh)
                if insight:
                    print(f"  {GREEN}✓{RESET} Success! ({len(insight)} characters)")
                    successes += 1
                else:
                    print(f"  {RED}✗{RESET} Failed to generate insight")
                    failures += 1
                    failed_companies.append(company)
            except Exception as e:
                print(f"  {RED}✗{RESET} Error: {str(e)}")
                failures += 1
                failed_companies.append(company)
            
            # Small delay to avoid rate limiting
            time.sleep(1)
        
        # Print summary
        print(f"\n{YELLOW}Summary:{RESET}")
        print(f"  {GREEN}✓{RESET} Successfully generated insights for {successes} companies")
        if failures > 0:
            print(f"  {RED}✗{RESET} Failed to generate insights for {failures} companies")
            print(f"\n{YELLOW}Failed companies:{RESET}")
            for company in failed_companies:
                print(f"  - {company}")
        
        return failures == 0
    except Exception as e:
        logger.error(f"Error generating all insights: {str(e)}")
        print(f"{RED}Error: {str(e)}{RESET}")
        return False

def generate_insight_cli(company_name: str = None, insight_type: str = None, force_refresh: bool = False) -> bool:
    """
    Command-line interface for generating insights.
    
    Args:
        company_name: The name of the company (optional, if None will list available companies)
        insight_type: Type of insight to generate (currently only 'company' is supported)
        force_refresh: Whether to force a refresh of the insight
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Set up colored output for CLI
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"
    try:
        # Special case for 'all' to generate insights for all companies
        if company_name and company_name.lower() == 'all':
            return generate_all_insights(force_refresh)
            
        # If no company name is provided, list available companies
        if not company_name:
            conn = get_analytics_connection()
            companies = conn.execute("SELECT DISTINCT Company FROM campaign_monthly_metrics ORDER BY Company").fetchall()
            conn.close()
            
            print("\nAvailable companies:")
            for i, (company,) in enumerate(companies, 1):
                print(f"{i}. {company}")
            
            print("\nUsage: python -m app.main insights [company_name] [insight_type] [--force]")
            print("       python -m app.main insights all [--force]  # Generate for all companies")
            return True
            
        # Validate company exists
        conn = get_analytics_connection()
        company_exists = conn.execute(
            "SELECT COUNT(*) FROM campaign_monthly_metrics WHERE Company = ?", 
            [company_name]
        ).fetchone()[0] > 0
        conn.close()
        
        if not company_exists:
            print(f"Error: Company '{company_name}' not found in the database.")
            return False
        
        # Initialize the insights generator
        generator = InsightsGenerator()
        
        # Generate and print the insight
        try:
            insight = generator.generate_insight(company_name, force_refresh)
            print(f"\n{GREEN}Insight for {company_name} generated successfully!{RESET}\n")
            print(f"Insight saved to database and file.")
            print(f"HTML length: {len(insight)} characters")
            return True
        except Exception as e:
            print(f"\n{RED}Failed to generate insight for {company_name}.{RESET}")
            print(f"{YELLOW}Please check the logs for more details and try again later.{RESET}")
            return False
    except Exception as e:
        logger.error(f"Error in generate_insight_cli: {str(e)}")
        print(f"Error generating insight: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate insights for marketing dashboard")
    parser.add_argument("company", nargs="?", help="Company name to generate insights for")
    parser.add_argument("type", nargs="?", choices=["company"], default="company", help="Type of insight to generate")
    parser.add_argument("--force", action="store_true", help="Force refresh the insight")
    
    args = parser.parse_args()
    
    generate_insight_cli(args.company, args.type, args.force)
