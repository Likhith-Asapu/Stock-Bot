"""
Stock Data Fetcher Module
Fetches stock price data from Groww API and manages CSV storage
"""

import requests
import csv
import os
import json
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from pathlib import Path
from typing import Dict, List, Optional
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StockDataFetcher:
    """Fetch and manage stock data from Groww API"""

    BASE_URL = "https://groww.in/v1/api/stocks_data/v1/all_stocks"
    CHART_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH"

    def __init__(self, data_dir: str = "Data"):
        # Default data directory is a `Data` folder next to this module
        default_dir = Path(__file__).parent / "Data"
        self.data_dir = Path(data_dir) if data_dir != "Data" else default_dir
        self.data_dir.mkdir(exist_ok=True)
        self.companies_file = self.data_dir / "companies_metadata.json"

    def fetch_companies(self, pages: int = 300) -> Dict:
        """
        Fetch list of companies from Groww API

        Args:
            pages: Number of pages to fetch (15 companies per page)

        Returns:
            Dictionary of companies with metadata
        """
        companies = {}

        for page in tqdm(range(pages), desc="Fetching companies", total=pages):
            payload = {
                "listFilters": {"INDUSTRY": [], "INDEX": []},
                "objFilters": {
                    "CLOSE_PRICE": {"max": 100000, "min": 0},
                    "MARKET_CAP": {"min": 0, "max": 500000000000000}
                },
                "page": str(page),
                "size": "15",
                "sortBy": "NA",
                "sortType": "ASC"
            }

            headers = {"Content-Type": "application/json"}

            try:
                response = requests.post(self.BASE_URL, json=payload, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                continue

            data = response.json()

            for company in data.get("records", []):
                if "nseScriptCode" not in company or company["nseScriptCode"] is None:
                    continue

                company_name = company.get("companyName")
                companies[company_name] = {
                    "nseScriptCode": company.get("nseScriptCode"),
                    "searchId": company.get("searchId"),
                    "marketCap": company.get("marketCap", 0),
                    "closePrice": company.get("closePrice", 0),
                    "fetched_at": datetime.now(timezone.utc).isoformat()
                }

        # Save metadata
        with open(self.companies_file, 'w') as f:
            json.dump(companies, f, indent=4)
        logger.info(f"Saved {len(companies)} companies to {self.companies_file}")

        return companies

    def load_companies(self) -> Dict:
        """Load companies from saved metadata file"""
        if self.companies_file.exists():
            with open(self.companies_file, 'r') as f:
                return json.load(f)
        return {}

    def fetch_stock_data(self, company_name: str, nse_script_code: str,
                        interval_minutes: int = 1, duration_days: int = 4,
                        end_timestamp: Optional[int] = None) -> bool:
        """
        Fetch stock price data for a company

        Args:
            company_name: Name of the company
            nse_script_code: NSE script code
            interval_minutes: Candle interval in minutes (1, 5, 15, 30, 60)
            duration_days: Number of days to fetch at a time
            end_timestamp: End timestamp in milliseconds (defaults to current time)

        Returns:
            True if successful, False otherwise
        """
        csv_path = self.data_dir / f"{company_name}.csv"

        # Skip if file already exists
        if csv_path.exists():
            logger.info(f"File {csv_path} already exists. Skipping download.")
            return True

        url = f"{self.CHART_URL}/{nse_script_code}"

        if end_timestamp is None:
            end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        interval_millis = interval_minutes * 60 * 1000
        duration_millis = duration_days * 24 * 60 * 60 * 1000

        # Write header
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Time", "Timestamp", "Open", "High", "Low", "Close", "Volume"])

        current_end = end_timestamp
        all_candles = []

        # Fetch data in chunks
        while True:
            start_timestamp = current_end - duration_millis

            params = {
                "intervalInMinutes": interval_minutes,
                "endTimeInMillis": current_end,
                "startTimeInMillis": start_timestamp,
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for {company_name}: {e}")
                return False

            data = response.json()
            candles = data.get("candles", [])

            if not candles:
                break

            all_candles.extend(candles)
            current_end = start_timestamp - 1

            # Safety check to prevent infinite loops
            if len(all_candles) > 100000:
                logger.warning(f"Too many candles for {company_name}, stopping")
                break

        # Process and sort candles
        all_candles.sort(key=lambda x: x[0])

        # Write to CSV
        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            for candle in all_candles:
                ts_val = candle[0]
                # Detect seconds vs milliseconds. Seconds timestamps are ~1e9, ms ~1e12.
                try:
                    tnum = float(ts_val)
                    if tnum > 1e11:
                        seconds = tnum / 1000.0
                    else:
                        seconds = tnum
                except Exception:
                    seconds = float(ts_val)

                # Convert to IST when possible, otherwise UTC
                try:
                    if ZoneInfo is not None:
                        dt = datetime.fromtimestamp(seconds, tz=ZoneInfo("Asia/Kolkata"))
                    else:
                        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
                except Exception:
                    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M:%S")

                writer.writerow([
                    date_str,
                    time_str,
                    ts_val,
                    candle[1],  # Open
                    candle[2],  # High
                    candle[3],  # Low
                    candle[4],  # Close
                    candle[5]   # Volume
                ])

        logger.info(f"Saved {len(all_candles)} candles for {company_name}")
        return True

    def fetch_multiple_stocks(self, stocks: Dict[str, str], skip_existing: bool = True) -> Dict:
        """
        Fetch data for multiple stocks

        Args:
            stocks: Dictionary mapping company names to script codes
            skip_existing: Skip companies that already have data files

        Returns:
            Dictionary with results
        """
        results = {"success": 0, "failed": 0, "skipped": 0}

        for company_name, script_code in tqdm(stocks.items(), desc="Fetching stocks"):
            csv_path = self.data_dir / f"{company_name}.csv"

            if skip_existing and csv_path.exists():
                results["skipped"] += 1
                continue

            if self.fetch_stock_data(company_name, script_code):
                results["success"] += 1
            else:
                results["failed"] += 1

        return results

    def organize_by_market_cap(self, companies: Dict) -> None:
        """
        Organize CSV files into LargeCap, MediumCap, SmallCap folders

        Args:
            companies: Dictionary of company data with market caps
        """
        # Destination directories under the configured data directory
        largecap_dir = self.data_dir / "LargeCap"
        mediumcap_dir = self.data_dir / "MediumCap"
        smallcap_dir = self.data_dir / "SmallCap"

        for dir_path in [largecap_dir, mediumcap_dir, smallcap_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        for company_name, data in companies.items():
            csv_path = self.data_dir / f"{company_name}.csv"

            if not csv_path.exists():
                continue

            # Prefer explicit capitalization field if provided (e.g. "Large Cap"),
            # otherwise fall back to marketCap numeric thresholds.
            cap_label = None
            if isinstance(data, dict):
                cap_label = data.get("capitalization") or data.get("capitalizationLabel")

            if cap_label:
                # Normalize several common variants
                cl = cap_label.strip().lower()
                if "large" in cl:
                    dest_dir = largecap_dir
                elif "medium" in cl:
                    dest_dir = mediumcap_dir
                else:
                    dest_dir = smallcap_dir
            else:
                market_cap = data.get("marketCap", 0) if isinstance(data, dict) else 0
                if market_cap > 2e13:
                    dest_dir = largecap_dir
                elif market_cap > 2e11:
                    dest_dir = mediumcap_dir
                else:
                    dest_dir = smallcap_dir

            dest_path = dest_dir / f"{company_name}.csv"

            # Move file if not already there
            try:
                if dest_path != csv_path and csv_path.exists():
                    csv_path.rename(dest_path)
                    logger.info(f"Moved {company_name} to {dest_dir.name}")
            except Exception as e:
                logger.error(f"Failed to move {company_name}: {e}")

    def get_data_summary(self) -> Dict:
        """Get summary statistics about stored data"""
        summary = {
            "total_companies": 0,
            "largecap": 0,
            "mediumcap": 0,
            "smallcap": 0,
            "index_data": []
        }

        # Count CSVs inside each cap directory
        for cap_type, cap_dir in [
            ("largecap", "LargeCap"),
            ("mediumcap", "MediumCap"),
            ("smallcap", "SmallCap")
        ]:
            cap_path = self.data_dir / cap_dir
            if cap_path.exists():
                # Count CSVs directly under the cap folder
                count = len(list(cap_path.glob("*.csv")))
                summary[cap_type] = count
                summary["total_companies"] += count

        # Also count any company CSVs left directly in the data directory (not organized yet)
        root_csvs = [p for p in self.data_dir.glob("*.csv") if p.name not in ("BSE.csv", "NIFTY.csv", "CDSL.csv")]
        if root_csvs:
            summary['total_companies'] += len(root_csvs)

        # Check index data

        for index_file in ["BSE.csv", "NIFTY.csv", "CDSL.csv"]:
            index_path = self.data_dir / index_file
            if index_path.exists():
                summary["index_data"].append(index_file)

        # If total is still zero, try counting recursively to be robust
        if summary['total_companies'] == 0:
            total = 0
            for cap_dir in (self.data_dir / 'LargeCap', self.data_dir / 'MediumCap', self.data_dir / 'SmallCap'):
                if cap_dir.exists():
                    total += len(list(cap_dir.rglob('*.csv')))
            # include root csvs as well
            total += len(list(self.data_dir.glob('*.csv')))
            summary['total_companies'] = total

        return summary


def main():
    """Run fetching/organizing.

    Behavior:
    - If a `company_data.json` file is found (searches next to this file, parent, and cwd),
      it will fetch CSVs for those companies and organize them into LargeCap/MediumCap/SmallCap
      under the module's `Data/` directory.
    - Otherwise it will fetch company metadata from the Groww API and save it.
    """
    base_dir = Path(__file__).parent
    data_dir = base_dir / 'Data'
    fetcher = StockDataFetcher(data_dir=str(data_dir))

    # Look for updated company data first, then fallback to the original
    candidate_paths = [
        base_dir / 'company_data_updated.json',
        base_dir / 'company_data.json',
        base_dir.parent / 'company_data_updated.json',
        base_dir.parent / 'company_data.json',
        Path.cwd() / 'company_data_updated.json',
        Path.cwd() / 'company_data.json'
    ]

    company_file = None
    for p in candidate_paths:
        if p.exists():
            company_file = p
            break

    if company_file:
        try:
            with open(company_file, 'r') as f:
                company_data = json.load(f)

            stocks = {name: meta.get('nseScriptCode') for name, meta in company_data.items() if meta.get('nseScriptCode')}
            results = fetcher.fetch_multiple_stocks(stocks, skip_existing=True)
            try:
                fetcher.organize_by_market_cap(company_data)
            except Exception as e:
                logger.error(f"Organize step failed: {e}")

            summary = fetcher.get_data_summary()
            print(json.dumps({"results": results, "summary": summary}, indent=2))
        except Exception as e:
            logger.error(f"Failed running full fetch: {e}")
    else:
        # Fall back to fetching company metadata only
        companies = fetcher.fetch_companies(pages=300)
        summary = fetcher.get_data_summary()
        print("\n=== Data Summary ===")
        print(f"Total Companies: {summary['total_companies']}")
        print(f"LargeCap: {summary['largecap']}")
        print(f"MediumCap: {summary['mediumcap']}")
        print(f"SmallCap: {summary['smallcap']}")
        print(f"Index Data: {summary['index_data']}")


if __name__ == "__main__":
    main()
