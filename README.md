# Forex Factory Economic Calendar Scraper

A Python toolkit for scraping and processing economic calendar data from [Forex Factory](https://www.forexfactory.com/calendar). Extracts high-impact USD economic events and bank holidays into a clean Parquet dataset.

## Features

- **Cloudflare Bypass** — Uses `undetected-chromedriver` with your real Chrome profile to avoid bot detection
- **Incremental Scraping** — Skips already-downloaded months; safe to interrupt and resume
- **Data Pipeline** — Parses JSON → filters by currency/impact → removes noise → outputs Parquet
- **Cross-Platform** — Auto-detects Chrome profile path on Windows, macOS, and Linux

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Scrape Data

```bash
# First run: leave browser visible to pass any Cloudflare checks
python scrape.py
```

The scraper will:
1. Open Chrome with your profile
2. Navigate to each month's calendar page
3. Extract calendar data from the page's JavaScript state
4. Save raw JSON to `out/days_YYYY_MM.json`

### 3. Process Data

```bash
# Full pipeline: JSON -> Parquet (recommended)
python pipeline.py

# Or run individual steps:
python pipeline.py --step parse      # JSON -> CSV
python pipeline.py --step sanitize   # Remove 'speaks' events
python pipeline.py --step parquet    # CSV -> Parquet
```

## Output

The pipeline produces `economic_events.parquet` with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `datetime_utc` | datetime64[ns, UTC] | Event timestamp |
| `currency` | string | Currency code (USD) |
| `impact` | string | Impact level (high, holiday) |
| `title` | string | Event name |
| `id` | string | Forex Factory event ID |

## Configuration

### Chrome Profile Path

**Important:** Edit `scrape.py` and set your Chrome user data path in the config section:

```python
# Chrome profile path (change this to your Chrome user data path!)
USER_DATA_DIR = r"C:\Users\YOUR_USERNAME\AppData\Local\Google\Chrome\User Data"
PROFILE_DIR   = "Default"  # e.g. "Profile 1", "Default", etc.
```

Common paths by OS:
- **Windows:** `C:\Users\<USERNAME>\AppData\Local\Google\Chrome\User Data`
- **macOS:** `~/Library/Application Support/Google/Chrome`
- **Linux:** `~/.config/google-chrome`

### Other Settings

Edit these constants in `scrape.py` as needed:

| Setting | Default | Description |
|---------|---------|-------------|
| `START_DATE` | `2021-01-01` | Scraping start date |
| `END_DATE` | `2025-12-31` | Scraping end date |
| `OUT_DIR` | `out` | Output directory for JSON |
| `PROXY` | `""` | Optional proxy URL |
| `HEADLESS` | `False` | Run headless (riskier for CF) |

### Pipeline Configuration

Edit `pipeline.py` constants to customize filtering:

```python
KEEP_CURRENCIES = {"USD"}           # Filter by currency
KEEP_IMPACTS = {"high", "holiday"}  # Filter by impact level
```

## Project Structure

```
├── scrape.py           # Web scraper using undetected-chromedriver
├── pipeline.py         # Data processing pipeline
├── requirements.txt    # Python dependencies
├── out/                # Scraped JSON data
│   ├── days_2021_01.json
│   ├── days_2021_02.json
│   └── ...
└── economic_events.parquet  # Final processed output
```

## Tips

### First Run
Run the scraper with browser visible (default). If Cloudflare presents a challenge, solve it manually once—your session cookies will persist in your Chrome profile.

### Handling Blocks
If you get blocked:
1. The scraper saves a screenshot to `out/cf_block_YYYY_MM.png`
2. Solve the captcha in the browser window
3. Press Enter in the terminal to continue

### Rate Limiting
The scraper includes random delays between requests. Avoid running too frequently to prevent IP blocks.

## Data Source

Data is scraped from the Forex Factory economic calendar. This tool is for personal/research use. Please respect their terms of service and rate limits.

## License

MIT


