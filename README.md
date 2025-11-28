# Forex Factory Economic Calendar Scraper

A high-performance Python toolkit for scraping and processing economic calendar data from [Forex Factory](https://www.forexfactory.com/calendar). By default extracts high-impact USD economic events and bank holidays into a clean Parquet dataset, however all economic events are preserved and parsing can be customized in pipeline.py.

## Features

- **⚡ 37% Faster** — Uses [nodriver](https://ultrafunkamsterdam.github.io/nodriver/) with direct CDP communication (no Selenium overhead)
- **🛡️ Cloudflare Bypass** — Undetected browser automation that passes bot checks
- **🚀 Optimized Loading** — Blocks images, fonts, CSS, and trackers for maximum speed
- **📦 Incremental Scraping** — Skips already-downloaded months; safe to interrupt and resume
- **🔄 Data Pipeline** — Parses JSON → filters by currency/impact → removes noise → outputs Parquet

## Performance

| Scraper | 6 Months | Technology |
|---------|----------|------------|
| `scrape.py` | **10.3s** | nodriver (CDP) |
| `scrape_selenium.py` | 16.5s | undetected-chromedriver (Selenium) |

**nodriver is 37% faster** thanks to direct Chrome DevTools Protocol communication and aggressive resource blocking.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Scrape Data

```bash
# Recommended: Use the fast nodriver scraper
python scrape.py

# Alternative: Selenium-based scraper (more stable, slower)
python scrape_selenium.py
```

The scraper will:
1. Open Chrome (creates a dedicated profile in `./nodriver_profile`)
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

### nodriver Scraper (`scrape_nodriver.py`)

| Setting | Default | Description |
|---------|---------|-------------|
| `START_DATE` | `2021-01-01` | Scraping start date |
| `END_DATE` | `2025-12-31` | Scraping end date |
| `HEADLESS` | `False` | Run headless (faster but riskier for CF) |
| `BLOCK_RESOURCES` | `True` | Block images/fonts/CSS for speed |

**Speed tuning:**
```python
WAIT_SECS = 2.0        # Max time to wait for calendar data
POST_LOAD_DELAY = 1.0  # Delay after navigation for JS to hydrate
POLL_INTERVAL = 0.05   # How often to check for data
BETWEEN_PAGES = 0.1    # Delay between pages
```

### Selenium Scraper (`scrape.py`)

Edit `USER_DATA_DIR` to point to your Chrome profile:
```python
USER_DATA_DIR = r"C:\Users\YOUR_USERNAME\AppData\Local\Google\Chrome\User Data"
```

Common paths by OS:
- **Windows:** `C:\Users\<USERNAME>\AppData\Local\Google\Chrome\User Data`
- **macOS:** `~/Library/Application Support/Google/Chrome`
- **Linux:** `~/.config/google-chrome`

### Pipeline Configuration

Edit `pipeline.py` constants to customize filtering:

```python
KEEP_CURRENCIES = {"USD"}           # Filter by currency
KEEP_IMPACTS = {"high", "holiday"}  # Filter by impact level
```

## Project Structure

```
├── scrape_nodriver.py  # Fast async scraper using nodriver (recommended)
├── scrape.py           # Selenium scraper using undetected-chromedriver
├── pipeline.py         # Data processing pipeline
├── requirements.txt    # Python dependencies
├── nodriver_profile/   # Dedicated Chrome profile for nodriver
├── out/                # Scraped JSON data
│   ├── days_2021_01.json
│   ├── days_2021_02.json
│   └── ...
└── economic_events.parquet  # Final processed output
```

## Tips

### First Run
Run with browser visible (`HEADLESS = False`). If Cloudflare presents a challenge, solve it manually once—cookies persist in the scraper profile.

### Handling Blocks
If you get blocked:
1. The scraper saves a screenshot to `out/cf_block_YYYY_MM.png`
2. Solve the captcha in the browser window
3. Press Enter in the terminal to continue

### Which Scraper to Use?
- **`scrape.py`** — Use this. It's faster and doesn't require configuring your Chrome profile path.
- **`scrape_selenium.py`** — Fallback if nodriver has issues. Uses your real Chrome profile (better for persistent cookies).

## Why nodriver?

[nodriver](https://ultrafunkamsterdam.github.io/nodriver/) is the official successor to `undetected-chromedriver`:

- **No Selenium/webdriver** — Direct Chrome DevTools Protocol communication
- **Fully async** — Non-blocking I/O for better performance  
- **Better stealth** — Improved WAF/bot detection resistance
- **No chromedriver binary** — Less setup, fewer version mismatches

## Data Source

Data is scraped from the Forex Factory economic calendar. This tool is for personal/research use. Please respect their terms of service and rate limits.

## License

MIT
