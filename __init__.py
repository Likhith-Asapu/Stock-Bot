"""
Stock-Bot - Stock Data Management and Analysis
Main entry point for the Stock-Bot package
"""

from .data_fetcher import StockDataFetcher
from .data_validator import DataValidator, DataUpdater
from .data_analysis import (
    analyze_company_file,
    compare_indices,
    find_largest_gainers_losers,
    export_summary_report
)

__version__ = "1.0.0"
__author__ = "Stock Analytics Team"

__all__ = [
    "StockDataFetcher",
    "DataValidator",
    "DataUpdater",
    "analyze_company_file",
    "compare_indices",
    "find_largest_gainers_losers",
    "export_summary_report"
]
