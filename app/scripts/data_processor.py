#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data processing module for the Meta Demo application.
This module handles the conversion of CSV data to parquet files,
partitioned by company and month.
"""

import os
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
# Determine base directory - assume /data exists in container
BASE_DIR = Path('/')
DATA_DIR = BASE_DIR / "dataset"  # Original CSV location
DATA_ROOT = BASE_DIR / "data"  # Root data directory
RAW_DIR = DATA_ROOT / "raw"  # For storing raw data copies
PROCESSED_DIR = DATA_ROOT / "processed"  # For processed parquet files
DB_DIR = DATA_ROOT / "db"  # For DuckDB database files


def clean_company_name(name):
    """
    Clean company name for use in file paths.
    
    Args:
        name (str): Original company name
        
    Returns:
        str: Cleaned company name suitable for file paths
    """
    # Replace spaces and special characters
    return name.replace(' ', '_').replace('-', '_').lower()


def ensure_data_dirs():
    """
    Ensure all data directories exist.
    """
    # Create all required directories
    for directory in [DATA_ROOT, RAW_DIR, PROCESSED_DIR, DB_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")


def process_data(csv_file=None, output_dir=None):
    """
    Process the CSV data and save as parquet files partitioned by company and month.
    
    Args:
        csv_file (Path, optional): Path to the CSV file
        output_dir (Path, optional): Path to the output directory
        
    Returns:
        dict: Summary of the processing results
    """
    # Set default paths if not provided
    if csv_file is None:
        csv_file = DATA_DIR / "Social_Media_Advertising.csv"
    
    if output_dir is None:
        output_dir = PROCESSED_DIR
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Processing data from {csv_file}")
    logger.info(f"Output directory: {output_dir}")
    
    # Read the CSV file
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(df)} rows from CSV")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return {"status": "error", "message": str(e)}
    
    # Convert date to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Clean numeric fields
    # Remove currency symbols and convert to float
    df['Acquisition_Cost'] = df['Acquisition_Cost'].replace('[\$,]', '', regex=True).astype(float)
    
    # Extract numeric value from Duration (e.g., "15 Days" -> 15)
    df['Duration'] = df['Duration'].str.extract('(\d+)').astype(int)
    
    # Convert other numeric fields to appropriate types
    df['Clicks'] = df['Clicks'].astype(int)
    df['Impressions'] = df['Impressions'].astype(int)
    df['Conversion_Rate'] = df['Conversion_Rate'].astype(float)
    df['ROI'] = df['ROI'].astype(float)
    df['Engagement_Score'] = df['Engagement_Score'].astype(float)
    
    # Extract month
    df['month'] = df['Date'].dt.strftime('%m')
    
    # Clean company name for file paths
    df['company_path'] = df['Company'].apply(clean_company_name)
    
    # Summary statistics
    total_rows = len(df)
    companies = df['Company'].nunique()
    months = df['month'].nunique()
    
    logger.info(f"Found {companies} companies and {months} months in the data")
    
    # Group by company and month, then save to parquet
    results = []
    for (company, month), group_df in df.groupby(['company_path', 'month']):
        # Create directory if it doesn't exist
        company_month_dir = output_dir / company / month
        company_month_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to parquet
        output_file = company_month_dir / 'data.parquet'
        
        # Remove temporary columns before saving
        save_df = group_df.drop(columns=['company_path', 'month'])
        
        # Save to parquet
        save_df.to_parquet(output_file, index=False)
        
        results.append({
            "company": company,
            "month": month,
            "rows": len(group_df),
            "file": str(output_file)
        })
        
        logger.info(f"Saved {len(group_df)} rows to {output_file}")
    
    summary = {
        "status": "success",
        "total_rows": total_rows,
        "companies": companies,
        "months": months,
        "files_created": len(results),
        "details": results
    }
    
    logger.info(f"Processing complete. Created {len(results)} parquet files.")
    return summary


def main():
    """
    Main function to run the data processing.
    """
    logger.info("Starting data processing")
    
    # Ensure all data directories exist
    ensure_data_dirs()
    
    # Process the data
    summary = process_data()
    
    if summary["status"] == "success":
        logger.info(f"Successfully processed {summary['total_rows']} rows")
        logger.info(f"Created {summary['files_created']} parquet files")
    else:
        logger.error(f"Processing failed: {summary['message']}")
    
    logger.info("Data processing complete")
    return summary


if __name__ == "__main__":
    main()
