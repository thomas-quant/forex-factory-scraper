# Forex Factory Economic Calendar Scraper

A Python toolkit for scraping and processing economic calendar data from [Forex Factory](https://www.forexfactory.com/calendar). It scrapes raw month JSON files and then runs the existing pipeline to produce a Parquet dataset.

## Features

- **Browserless scraping** — `scrape.py` uses `curl_cffi` instead of browser automation
- **Proven parity** — matched the old headed nodriver scraper across the tested comparison span
- **Faster runs** — measured much faster than headed nodriver in local comparison runs
- **Incremental scraping** — skips already-downloaded months; safe to interrupt and resume
- **Pipeline unchanged** — JSON → CSV → cleaned CSV → Parquet

## Performance

Measured locally over **2021-01-01 → 2021-06-30**:

| Scraper | Time | Notes |
|---------|------|-------|
| `scrape.py` | **6.171s** | current `curl_cffi` scraper |
| old headed nodriver | 17.678s | Windows Chromium, headed |

On that same 6-month span, the current scraper matched the old headed nodriver output exactly while running about **2.87x faster**.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Scrape Data

```bash
python scrape.py
```

`scrape.py` fetches Forex Factory HTML with `curl_cffi`, extracts the embedded calendar state, and writes raw JSON to `out/days_YYYY_MM.json`.

Example:

```bash
python scrape.py --start-date 2026-03-01 --end-date 2026-03-31
python scrape.py --start-date 2026-03-01 --end-date 2026-03-31 --between-pages-delay 1.0
python scrape.py --start-date 2026-03-01 --end-date 2026-03-31 --retry-delay 0.5
```

Both delay settings default to `0`.

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

### Scraper (`scrape.py`)

| Setting | Default | Description |
|---------|---------|-------------|
| `START_DATE` | `2021-01-01` | Scraping start date |
| `END_DATE` | `2021-06-30` | Scraping end date |
| `OUT_DIR` | `out` | Output directory |
| `MAX_ATTEMPTS` | `3` | Retries per month |
| `BETWEEN_PAGES_DELAY` | `0.0` | Delay between month requests |
| `RETRY_DELAY` | `0.0` | Delay before retrying failed requests |

### Pipeline Configuration

Edit `pipeline.py` constants to customize filtering:

```python
KEEP_CURRENCIES = {"USD"}           # Filter by currency
KEEP_IMPACTS = {"high", "holiday"}  # Filter by impact level
```

## Project Structure

```
├── scrape.py         # Browserless scraper using curl_cffi
├── pipeline.py       # Data processing pipeline
├── requirements.txt  # Python dependencies
├── out/              # Scraped JSON data
│   ├── days_2021_01.json
│   ├── days_2021_02.json
│   └── ...
└── economic_events.parquet  # Final processed output
```

## Notes

- The old nodriver and Selenium scrapers have been removed.
- `scrape.py` is now the single scraper entrypoint.
- Comparison runs showed exact output parity against the old headed nodriver path for the tested range.

## Data Source

Data is scraped from the Forex Factory economic calendar. This tool is for personal/research use. Please respect their terms of service and rate limits.

## License

MIT
