import sys
import json
import logging
from pathlib import Path

# Ensure local package import
sys.path.insert(0, Path(__file__).parent.as_posix())
from data_fetcher import StockDataFetcher

BASE = Path(__file__).parent
DATA_DIR = BASE / 'Data'
LOG_FILE = DATA_DIR / 'fetch_10.log'
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging to file only
logger = logging.getLogger()
logger.setLevel(logging.INFO)
for h in list(logger.handlers):
    logger.removeHandler(h)
fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)

# Load company data (prefer updated)
candidates = [Path.cwd() / 'company_data_updated.json', Path.cwd() / 'company_data.json', BASE / 'company_data_updated.json', BASE / 'company_data.json']
company_data = None
for c in candidates:
    if c.exists():
        with open(c, 'r') as f:
            company_data = json.load(f)
        break

if company_data is None:
    logger.error('No company_data file found')
    raise SystemExit(1)

# Pick first 10 companies with a script code
items = list(company_data.items())[:10]
stocks = {name: meta.get('nseScriptCode') for name, meta in items if meta.get('nseScriptCode')}

logger.info(f'Starting fetch for {len(stocks)} companies')
fetcher = StockDataFetcher(data_dir=str(DATA_DIR))
results = fetcher.fetch_multiple_stocks(stocks, skip_existing=False)
logger.info(f'Fetch results: {results}')

# Organize files
try:
    fetcher.organize_by_market_cap(company_data)
    logger.info('Organization complete')
except Exception as e:
    logger.error(f'Organization failed: {e}')

# Print a short summary to stdout
print(json.dumps({"results": results}, indent=2))
print(f'Log written to: {LOG_FILE}')
