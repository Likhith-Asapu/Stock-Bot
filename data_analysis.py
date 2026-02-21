"""
Data Analysis Utilities
Provides functions for analyzing stock data
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List
import os


def analyze_company_file(csv_path: str) -> Dict:
    """
    Analyze a single company CSV file

    Returns:
        Dictionary with analysis results
    """
    try:
        df = pd.read_csv(csv_path)

        return {
            "file": os.path.basename(csv_path),
            "rows": len(df),
            "columns": list(df.columns),
            "date_range": {
                "start": df['Date'].iloc[0] if len(df) > 0 else None,
                "end": df['Date'].iloc[-1] if len(df) > 0 else None
            },
            "price_stats": {
                "close_min": float(df['Close'].min()),
                "close_max": float(df['Close'].max()),
                "close_mean": float(df['Close'].mean()),
                "close_std": float(df['Close'].std())
            },
            "volume_stats": {
                "total_volume": float(df['Volume'].sum()) if 'Volume' in df.columns else 0,
                "avg_volume": float(df['Volume'].mean()) if 'Volume' in df.columns else 0
            }
        }
    except Exception as e:
        return {"file": os.path.basename(csv_path), "error": str(e)}


def compare_indices(data_dir: str = "Data") -> Dict:
    """
    Compare index performance

    Returns:
        Dictionary with index comparison
    """
    indices = ["NIFTY", "BSE", "CDSL"]
    comparison = {}

    for index_name in indices:
        csv_path = Path(data_dir) / f"{index_name}.csv"
        if csv_path.exists():
            comparison[index_name] = analyze_company_file(str(csv_path))

    return comparison


def find_largest_gainers_losers(data_dir: str = "Data", cap_type: str = "LargeCap", top_n: int = 10):
    """
    Find top gainers and losers for a market cap category

    Args:
        data_dir: Data directory path
        cap_type: LargeCap, MediumCap, or SmallCap
        top_n: Number of top results to return

    Returns:
        Dictionary with gainers and losers
    """
    cap_dir = Path(data_dir) / cap_type
    if not cap_dir.exists():
        return {"error": f"{cap_type} directory not found"}

    results = []

    for csv_file in cap_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file)
            if len(df) > 0:
                first_close = df['Close'].iloc[0]
                last_close = df['Close'].iloc[-1]
                pct_change = ((last_close - first_close) / first_close) * 100

                results.append({
                    "company": csv_file.stem,
                    "pct_change": round(pct_change, 2),
                    "start_price": round(first_close, 2),
                    "end_price": round(last_close, 2)
                })
        except Exception as e:
            print(f"Error analyzing {csv_file.name}: {e}")

    # Sort
    results.sort(key=lambda x: x['pct_change'], reverse=True)

    return {
        "gainers": results[:top_n],
        "losers": results[-top_n:]
    }


def export_summary_report(data_dir: str = "Data", output_file: str = "data_summary_report.json"):
    """
    Generate and export a comprehensive summary report
    """
    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "indices": compare_indices(data_dir),
        "largecap": find_largest_gainers_losers(data_dir, "LargeCap"),
        "mediumcap": find_largest_gainers_losers(data_dir, "MediumCap"),
        "smallcap": find_largest_gainers_losers(data_dir, "SmallCap")
    }

    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"✓ Report saved to {output_file}")
    return report


if __name__ == "__main__":
    print("Generating data summary report...")
    report = export_summary_report()

    print("\n=== Indices Comparison ===")
    for index_name, data in report.get('indices', {}).items():
        if 'error' not in data:
            print(f"{index_name}: {data['rows']} rows")

    print("\n=== Top Gainers (LargeCap) ===")
    for company in report['largecap'].get('gainers', [])[:5]:
        print(f"{company['company']}: +{company['pct_change']}%")
