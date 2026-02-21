# Stock-Bot

Automated stock data fetching, validation, and analysis tool for Indian stock market data (NSE/BSE).

## Features

- **Data Fetching**: Fetch real-time stock data from Groww API
- **Data Organization**: Automatically organize companies by market cap (LargeCap, MediumCap, SmallCap)
- **Data Validation**: Comprehensive data integrity checks and gap detection
- **Data Analysis**: Analyze stock performance, identify gainers/losers
- **Multiple Indices**: Support for NIFTY, BSE, CDSL indices
- **CLI Tool**: Command-line interface for easy data management

## Installation

### Prerequisites
- Python 3.9+
- pip or conda

### Setup

```bash
# Clone the repository
cd Stock-Bot

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Command Line Interface

```bash
# Fetch stock data (refreshes company list and fetches first N companies)
python main.py fetch --companies 50 --refresh

# Validate data integrity
python main.py validate

# Generate data summary
python main.py summary

# Analyze stock performance
python main.py analyze --cap-type LargeCap

# Get help
python main.py --help
```

### Python API

```python
from stock_bot import StockDataFetcher, DataValidator

# Fetch companies and data
fetcher = StockDataFetcher(data_dir="Data")
companies = fetcher.fetch_companies(pages=300)

# Validate data
validator = DataValidator(data_dir="Data")
results = validator.validate_all_data()
print(f"Valid files: {results['valid_files']}/{results['total_files']}")
```

## Project Structure

```
Stock-Bot/
├── data_fetcher.py      # Core fetching functionality
├── data_validator.py    # Data validation and updates
├── data_analysis.py     # Analysis utilities
├── main.py             # CLI entry point
├── __init__.py         # Package initialization
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Data Structure

Data is organized as follows:
```
Data/
├── NIFTY.csv           # NIFTY index data
├── BSE.csv             # BSE index data
├── CDSL.csv            # CDSL index data
├── LargeCap/           # Large cap companies (>₹20 trillion)
│   ├── Company1.csv
│   ├── Company2.csv
│   └── ...
├── MediumCap/          # Medium cap companies (₹200B - ₹20T)
│   ├── Company1.csv
│   ├── Company2.csv
│   └── ...
└── SmallCap/           # Small cap companies (<₹200 billion)
    ├── Company1.csv
    ├── Company2.csv
    └── ...
```

## CSV Format

Each stock CSV file contains:
- **Date**: Trading date (YYYY-MM-DD)
- **Time**: Trading time (HH:MM)
- **Timestamp**: Milliseconds since epoch
- **Open**: Opening price
- **High**: Highest price
- **Low**: Lowest price
- **Close**: Closing price
- **Volume**: Trading volume

## Recent Updates (February 2025)

### Data Updates
- Rebuilt database with fresh API fetch from Groww
- Organized 312 LargeCap, 1382 MediumCap, and SmallCap companies
- Updated index data (NIFTY, BSE, CDSL)
- Last data update: June 22, 2025

### Code Improvements
- Fixed deprecated `datetime.utcfromtimestamp()` with timezone-aware `fromtimestamp()`
- Added comprehensive error handling with try-except blocks
- Improved data validation and integrity checks
- Added data gap detection
- Created modular Python package structure

### New Features
- CLI tool for easy data management
- Data summary and analysis reports
- Market performance analysis (gainers/losers)
- Incremental data update support

## API Reference

### StockDataFetcher

```python
fetcher = StockDataFetcher(data_dir="Data")

# Fetch all companies
companies = fetcher.fetch_companies(pages=300)

# Fetch single stock
fetcher.fetch_stock_data("RELIANCE", {"nseScriptCode": "RELIANCE"})

# Fetch multiple stocks
results = fetcher.fetch_multiple_stocks(stocks_dict)

# Organize by market cap
fetcher.organize_by_market_cap(companies)
```

### DataValidator

```python
validator = DataValidator(data_dir="Data")

# Validate all data
results = validator.validate_all_data()

# Check individual file
is_valid, msg = validator.validate_csv_format(csv_path)

# Check price ranges
validator.validate_price_range(csv_path)
```

### Data Analysis

```python
from data_analysis import compare_indices, find_largest_gainers_losers

# Compare indices performances
indices = compare_indices()

# Find top gainers/losers
results = find_largest_gainers_losers(cap_type="LargeCap")
```

## Performance Notes

- Fetching fresh data for 300+ companies may take 1-2 hours
- Each request includes up to 4 days of 1-minute candles (~5760 records)
- Data is stored in CSV format for easy analysis with pandas
- Organized structure supports quick filtering by market cap

## Troubleshooting

**Issue**: API timeout errors
- **Solution**: Reduce the number of pages or use skip_existing=True

**Issue**: Duplicate timestamps in data
- **Solution**: Run validator and use DataUpdater.append_new_data()

**Issue**: Missing company data
- **Solution**: Refresh company list with `fetch_companies(pages=300)`

## Future Enhancements

- [ ] Real-time data streaming
- [ ] WebSocket support for live updates
- [ ] Database backend (PostgreSQL/MongoDB)
- [ ] Web UI for data exploration
- [ ] Machine learning integration
- [ ] Automated backtesting framework

## License

MIT License - Feel free to use and modify

## Contributing

Contributions welcome! Please submit pull requests or issues.