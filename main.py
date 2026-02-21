#!/usr/bin/env python3
"""
Stock-Bot CLI - Main entry point
Usage: python main.py [command] [options]
"""

import argparse
import sys
from pathlib import Path

from data_fetcher import StockDataFetcher
from data_validator import DataValidator, DataUpdater
from data_analysis import export_summary_report, compare_indices


def setup_parser():
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(description="Stock-Bot Data Management Tool")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch stock data")
    fetch_parser.add_argument("--companies", type=int, default=10, help="Number of companies to fetch (default: 10)")
    fetch_parser.add_argument("--data-dir", default="../Data", help="Data directory path")
    fetch_parser.add_argument("--refresh", action="store_true", help="Refresh company list")

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate data integrity")
    validate_parser.add_argument("--data-dir", default="../Data", help="Data directory path")

    # Summarize command
    summary_parser = subparsers.add_parser("summary", help="Generate data summary")
    summary_parser.add_argument("--data-dir", default="../Data", help="Data directory path")

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze stock data")
    analyze_parser.add_argument("--data-dir", default="../Data", help="Data directory path")
    analyze_parser.add_argument("--cap-type", choices=["LargeCap", "MediumCap", "SmallCap"], default="LargeCap", help="Market cap category")

    return parser


def cmd_fetch(args):
    """Handle fetch command"""
    print(f"Fetching data from {args.data_dir}...")
    fetcher = StockDataFetcher(data_dir=args.data_dir)

    if args.refresh:
        print("Refreshing company list...")
        companies = fetcher.fetch_companies(pages=300)
    else:
        companies = fetcher.load_companies()
        if not companies:
            print("No companies found. Fetching fresh list...")
            companies = fetcher.fetch_companies(pages=300)

    print(f"Found {len(companies)} companies")

    # Fetch limited number for demo
    sample_size = min(args.companies, len(companies))
    sample_companies = dict(list(companies.items())[:sample_size])

    print(f"Fetching data for {sample_size} companies...")
    results = fetcher.fetch_multiple_stocks(sample_companies)

    print(f"\nResults:")
    print(f"  Successful: {results['success']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Skipped: {results['skipped']}")


def cmd_validate(args):
    """Handle validate command"""
    print(f"Validating data in {args.data_dir}...")
    validator = DataValidator(data_dir=args.data_dir)

    results = validator.validate_all_data()
    print(f"\nValidation Results:")
    print(f"  Total Files: {results['total_files']}")
    print(f"  Valid: {results['valid_files']}")
    print(f"  Invalid: {results['invalid_files']}")

    if results['issues']:
        print(f"\nIssues Found ({len(results['issues'])}):")
        for issue in results['issues'][:10]:  # Show first 10
            print(f"  - {issue['file']}: {issue['error']}")


def cmd_summary(args):
    """Handle summary command"""
    print(f"Generating summary for {args.data_dir}...")
    report = export_summary_report(data_dir=args.data_dir)

    print("\n=== Indices Summary ===")
    for index_name, data in report.get('indices', {}).items():
        if 'error' not in data:
            print(f"{index_name}:")
            print(f"  Rows: {data.get('rows', 'N/A')}")
            print(f"  Date Range: {data['date_range']['start']} to {data['date_range']['end']}")


def cmd_analyze(args):
    """Handle analyze command"""
    print(f"Analyzing {args.cap_type} companies...")

    from data_analysis import find_largest_gainers_losers

    results = find_largest_gainers_losers(data_dir=args.data_dir, cap_type=args.cap_type)

    print(f"\n=== Top 5 Gainers ({args.cap_type}) ===")
    for company in results.get('gainers', [])[:5]:
        print(f"{company['company']}: +{company['pct_change']}% ({company['start_price']} → {company['end_price']})")

    print(f"\n=== Top 5 Losers ({args.cap_type}) ===")
    for company in results.get('losers', [])[:5]:
        print(f"{company['company']}: {company['pct_change']}% ({company['start_price']} → {company['end_price']})")


def main():
    """Main entry point"""
    parser = setup_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "fetch":
            cmd_fetch(args)
        elif args.command == "validate":
            cmd_validate(args)
        elif args.command == "summary":
            cmd_summary(args)
        elif args.command == "analyze":
            cmd_analyze(args)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
