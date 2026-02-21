"""
Stock Data Validation and Update Module
Validates data integrity and manages incremental updates
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataValidator:
    """Validate and clean stock data"""

    def __init__(self, data_dir: str = "Data"):
        self.data_dir = Path(data_dir)

    def validate_csv_format(self, csv_path: Path) -> Tuple[bool, str]:
        """
        Validate CSV file format

        Returns:
            Tuple of (is_valid, message)
        """
        try:
            df = pd.read_csv(csv_path)

            required_columns = ["Date", "Time", "Timestamp", "Open", "High", "Low", "Close"]
            if not all(col in df.columns for col in required_columns):
                return False, f"Missing required columns. Expected: {required_columns}"

            if len(df) == 0:
                return False, "CSV is empty"

            # Check data types
            numeric_cols = ["Timestamp", "Open", "High", "Low", "Close", "Volume"]
            for col in numeric_cols:
                if col in df.columns:
                    try:
                        pd.to_numeric(df[col], errors='coerce')
                    except Exception as e:
                        return False, f"Column {col} has invalid numeric data: {e}"

            return True, "Valid CSV format"

        except Exception as e:
            return False, f"Error reading CSV: {e}"

    def check_data_gaps(self, csv_path: Path) -> Dict:
        """
        Check for gaps in time series data

        Returns:
            Dictionary with gap information
        """
        try:
            df = pd.read_csv(csv_path)
            df['Timestamp'] = pd.to_numeric(df['Timestamp'])
            df = df.sort_values('Timestamp')

            gaps = []
            expected_interval = 60000  # 1 minute in milliseconds

            for i in range(1, len(df)):
                time_diff = df.iloc[i]['Timestamp'] - df.iloc[i-1]['Timestamp']
                if time_diff > expected_interval * 1.5:  # Allow 50% tolerance
                    gaps.append({
                        "position": i,
                        "gap_size_minutes": time_diff / 60000,
                        "before": df.iloc[i-1]['Date'],
                        "after": df.iloc[i]['Date']
                    })

            return {
                "total_records": len(df),
                "gap_count": len(gaps),
                "gaps": gaps
            }

        except Exception as e:
            logger.error(f"Error checking gaps in {csv_path}: {e}")
            return {"error": str(e)}

    def validate_price_range(self, csv_path: Path) -> Dict:
        """
        Validate price data is within reasonable ranges

        Returns:
            Dictionary with validation results
        """
        try:
            df = pd.read_csv(csv_path)

            # Convert to numeric
            df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
            df['High'] = pd.to_numeric(df['High'], errors='coerce')
            df['Low'] = pd.to_numeric(df['Low'], errors='coerce')

            issues = []

            # Check High >= Low
            bad_range = df[df['High'] < df['Low']]
            if len(bad_range) > 0:
                issues.append(f"{len(bad_range)} rows have High < Low")

            # Check Close between High and Low
            bad_close = df[(df['Close'] > df['High']) | (df['Close'] < df['Low'])]
            if len(bad_close) > 0:
                issues.append(f"{len(bad_close)} rows have Close outside High-Low range")

            # Check for extreme jumps (more than 20% in single candle)
            df['pct_change'] = df['Close'].pct_change().abs()
            extreme_jumps = df[df['pct_change'] > 0.20]
            if len(extreme_jumps) > 0:
                issues.append(f"{len(extreme_jumps)} rows have >20% price change")

            return {
                "file": csv_path.name,
                "is_valid": len(issues) == 0,
                "issues": issues,
                "price_range": {
                    "min": float(df['Close'].min()),
                    "max": float(df['Close'].max()),
                    "avg": float(df['Close'].mean())
                }
            }

        except Exception as e:
            logger.error(f"Error validating prices in {csv_path}: {e}")
            return {"error": str(e)}

    def validate_all_data(self, cap_types: List[str] = None) -> Dict:
        """
        Validate all data files

        Args:
            cap_types: List of cap types to check (default: all)
        """
        if cap_types is None:
            cap_types = ["LargeCap", "MediumCap", "SmallCap"]

        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_files": 0,
            "valid_files": 0,
            "invalid_files": 0,
            "issues": []
        }

        for cap_type in cap_types:
            cap_dir = self.data_dir / cap_type
            if not cap_dir.exists():
                continue

            for csv_file in cap_dir.glob("*.csv"):
                results["total_files"] += 1

                is_valid, msg = self.validate_csv_format(csv_file)
                if is_valid:
                    results["valid_files"] += 1
                else:
                    results["invalid_files"] += 1
                    results["issues"].append({"file": csv_file.name, "error": msg})

        return results


class DataUpdater:
    """Handle incremental data updates"""

    def __init__(self, data_dir: str = "Data"):
        self.data_dir = Path(data_dir)
        self.validator = DataValidator(data_dir)

    def get_latest_timestamp(self, csv_path: Path) -> Optional[int]:
        """Get the latest timestamp from a CSV file"""
        try:
            df = pd.read_csv(csv_path)
            if len(df) == 0:
                return None
            return int(df['Timestamp'].iloc[-1])
        except Exception as e:
            logger.error(f"Error reading {csv_path}: {e}")
            return None

    def append_new_data(self, csv_path: Path, new_data: List) -> bool:
        """
        Append new data to existing CSV

        Args:
            csv_path: Path to CSV file
            new_data: List of new rows to append

        Returns:
            True if successful
        """
        try:
            df = pd.read_csv(csv_path)

            # Create dataframe from new data
            new_df = pd.DataFrame(new_data, columns=["Date", "Time", "Timestamp", "Open", "High", "Low", "Close", "Volume"])

            # Remove duplicates based on Timestamp
            combined = pd.concat([df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['Timestamp'], keep='last')
            combined = combined.sort_values('Timestamp')

            # Save back
            combined.to_csv(csv_path, index=False)
            logger.info(f"Updated {csv_path} with {len(new_data)} new rows")
            return True

        except Exception as e:
            logger.error(f"Error appending data to {csv_path}: {e}")
            return False

    def get_update_stats(self) -> Dict:
        """Get statistics about data freshness"""
        stats = {"cap_types": {}}

        for cap_type in ["LargeCap", "MediumCap", "SmallCap"]:
            cap_dir = self.data_dir / cap_type
            if not cap_dir.exists():
                continue

            timestamps = []
            file_count = 0

            for csv_file in cap_dir.glob("*.csv"):
                file_count += 1
                latest_ts = self.get_latest_timestamp(csv_file)
                if latest_ts:
                    timestamps.append(latest_ts)

            if timestamps:
                latest_date = datetime.fromtimestamp(max(timestamps) / 1000, tz=timezone.utc)
                oldest_date = datetime.fromtimestamp(min(timestamps) / 1000, tz=timezone.utc)

                stats["cap_types"][cap_type] = {
                    "files": file_count,
                    "latest_date": latest_date.isoformat(),
                    "oldest_date": oldest_date.isoformat(),
                    "date_range_days": (latest_date - oldest_date).days
                }

        return stats


def main():
    """Example usage"""
    validator = DataValidator(data_dir="../Data")
    updater = DataUpdater(data_dir="../Data")

    # Validate all data
    results = validator.validate_all_data()
    print("\n=== Validation Results ===")
    print(f"Total Files: {results['total_files']}")
    print(f"Valid: {results['valid_files']}")
    print(f"Invalid: {results['invalid_files']}")

    # Get update stats
    stats = updater.get_update_stats()
    print("\n=== Update Stats ===")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
