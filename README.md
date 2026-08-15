# forexfactory

**Fetch the Forex Factory economic calendar once and reuse it everywhere** — a shared local parquet cache any of your projects can read, with the data fidelity needed for expected-vs-surprise analysis.

[![PyPI version](https://img.shields.io/pypi/v/forexfactory.svg)](https://pypi.org/project/forexfactory/)
[![Python versions](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue.svg)](https://pypi.org/project/forexfactory/)
[![CI](https://img.shields.io/github/actions/workflow/status/thomas-quant/forexfactory/ci.yml?label=CI)](https://github.com/thomas-quant/forexfactory/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Quick Start

```bash
pip install forexfactory
forexfactory populate
forexfactory query --currency USD --impact high
```

That's it. The parquet path is printed to stdout — pipe it straight into pandas.

```bash
PARQUET=$(forexfactory query --currency USD --impact high)
python -c "import pandas as pd; print(pd.read_parquet('$PARQUET').head())"
```

## CLI Reference

### `forexfactory populate`

Build the local parquet cache from on-disk raw JSON files (zero network calls by default):

```bash
forexfactory populate
```

Default scope: **USD** currency, **high** and **holiday** impact, all months on disk (~195 months, 2010-01 to 2026-03). Widen the scope with repeatable flags:

```bash
forexfactory populate --currency USD --currency EUR --impact high --impact medium
```

Force-rebuild every month unconditionally (bypasses the manifest skip-check — use after a schema migration):

```bash
forexfactory populate --force --raw-dir out
```

### `forexfactory refresh`

Fetch months not yet in the cache from Forex Factory over the network:

```bash
forexfactory refresh
```

Gap-fills from the last cached month through the current month with a polite 1-second delay between requests. Does not overwrite already-settled months.

### `forexfactory query`

Print the absolute path of the result parquet to stdout — nothing else — making it shell-friendly:

```bash
forexfactory query --currency USD --impact high
```

By default returns data-bearing events (with `forecast`/`actual` fields) and bank holidays. To include speeches and other no-data events:

```bash
forexfactory query --currency USD --impact high --include-no-data
```

If the requested currency/impact combination has not been populated, the command exits non-zero and prints actionable guidance to stderr.

### `forexfactory status`

Report cache location, date range, scope, schema version, and settled/matured state:

```bash
forexfactory status
forexfactory status --json   # machine-readable JSON
```

### `forexfactory --version`

Print the installed package version:

```bash
forexfactory --version
```

## Library API

### `forexfactory.get(...)` — Path to filtered parquet

```python
import forexfactory
from pathlib import Path

path: Path = forexfactory.get(currencies=["USD"], impacts=["high"])

import pandas as pd
df = pd.read_parquet(path)
```

`forexfactory.get(...)` returns a `pathlib.Path` to a filtered parquet file in the local cache. All parameters are keyword-only:

```python
path = forexfactory.get(
    currencies=["USD"],          # list of currency codes (default: ["USD", "EUR", "GBP", "JPY"])
    impacts=["high"],            # list of impact levels (default: ["high", "medium", "holiday"])
    start="2024-01",             # first month to include (YYYY-MM, optional)
    end="2024-12",               # last month to include (YYYY-MM, optional)
    include_no_data=False,       # include speeches and no-data events (default: False)
    cache_dir=None,              # override cache directory (default: ~/.cache/forexfactory)
    auto_fetch=True,             # auto-refresh matured months before returning (default: True)
)
```

### `forexfactory.read(...)` — DataFrame convenience loader

`forexfactory.read(...)` complements `get()` — it accepts the same keyword-only parameters but returns a `pandas.DataFrame` instead of a `Path`, so you never need to handle a file path yourself:

```python
import forexfactory

df = forexfactory.read(currencies=["USD"], impacts=["high"])
# df is a pandas.DataFrame with a datetime_utc DatetimeIndex and datetime_utc column
```

The returned DataFrame has:
- A `DatetimeIndex` named `datetime_utc`, sorted ascending
- `datetime_utc` retained as a column (for groupby/merge ergonomics)
- All schema columns as stored — no automatic type coercion
- A `siteId` column always present (null for pre-v1.1 cached months)

### `forexfactory.surprise(df)` and `forexfactory.surprise_z(df)` — opt-in surprise metrics

Surprise helpers operate on the DataFrame returned by `read()` and return row-aligned `pd.Series`. They are opt-in — `read()` returns the plain schema; you compose surprise yourself:

```python
import forexfactory

df = forexfactory.read(currencies=["USD"], impacts=["high"])
df["surprise"] = forexfactory.surprise(df)    # raw actual − forecast (D-01)
df["surprise_z"] = forexfactory.surprise_z(df)  # z-scored over each ebaseId's full history (D-02)
```

- `surprise(df)`: `actual − forecast`, verbatim. NaN when either is NaN. Sign is numeric — no polarity adjustment.
- `surprise_z(df)`: z-scored over each `ebaseId`'s full history in `df`. NaN for groups with fewer than 2 releases or zero standard deviation.

### `forexfactory.actual_initial(df)` and `forexfactory.actual_revised(df)` — point-in-time vintages

Forex Factory preserves the figure that printed on release day and reports any later
restatement in the *next* release's `revision` cell, rather than overwriting the
original. That makes the cache a point-in-time archive: scraping 2010 today returns
2010's first prints. These helpers expose both vintages.

```python
df = forexfactory.read(currencies=["USD"], impacts=["high"])
df["actual_initial"] = forexfactory.actual_initial(df)  # what printed on the day
df["actual_revised"] = forexfactory.actual_revised(df)  # latest known value
```

- `actual_initial(df)`: the first-printed value — the stored `actual`, named so the point-in-time guarantee is explicit at the call site. **Use this for backtests**: `surprise(df)` is already built on it, so it carries no look-ahead.
- `actual_revised(df)`: the next release's `revision` when one was published, otherwise the first print. The most recent release in each series has no successor yet and so returns its own print.

Two vintages, not a full revision triangle — later restatements (GDP third estimates,
annual benchmark revisions) appear only when Forex Factory surfaces them in a
subsequent `revision` cell. Across the reference cache 45.8% of released events carry
a revision.

## Output Schema

The cache stores data as parquet with the following columns (schema_version 3):

**Core fields (DATA-01)**

| Column | Type | Description |
|--------|------|-------------|
| `datetime_utc` | datetime64[ns, UTC] | Event timestamp in UTC |
| `currency` | string | Currency code (e.g. USD) |
| `impact` | string | Impact level (high, holiday, medium, low) |
| `title` | string | Event name |
| `id` | Int64 | Forex Factory event ID (nullable) |
| `leaked` | boolean | Whether Forex Factory marked the event as leaked |

**Raw value strings (verbatim from FF)**

| Column | Type | Description |
|--------|------|-------------|
| `forecast_raw` | string | Raw forecast value string (e.g. `"4.3%"`, `"202K"`, `""`) |
| `actual_raw` | string | Raw actual value string |
| `previous_raw` | string | Raw previous value string |
| `revision_raw` | string | Raw revision value string |

**Parsed numerics**

Magnitude suffixes expanded (K=×1e3, M=×1e6, B=×1e9, T=×1e12). Percent divided by 100 (`"4.3%"` → 0.043). Unparseable or empty strings become null (NaN).

| Column | Type | Description |
|--------|------|-------------|
| `forecast` | float64 | Parsed forecast numeric (null if unparseable) |
| `actual` | float64 | Parsed actual numeric |
| `previous` | float64 | Parsed previous numeric |
| `revision` | float64 | Parsed revision numeric |

**Surprise flags and identity**

| Column | Type | Description |
|--------|------|-------------|
| `actualBetterWorse` | Int64 | FF surprise flag: 1=better, 2=worse, 0=neutral/n/a (nullable) |
| `revisionBetterWorse` | Int64 | FF revision flag: 1=better, 2=worse, 0=neutral/n/a (nullable) |
| `ebaseId` | Int64 | FF metric series identifier (nullable) |
| `country` | string | Country code (e.g. US, UK, JN, EZ) |
| `hasDataValues` | boolean | True for data releases with forecast/actual/previous; False for speeches and holidays |
| `siteId` | object | FF site identifier (nullable — populated for new scrapes, null for pre-v1.1 cached months; prerequisite for the v2 graph API) |

## Cache Layout

The cache lives at `~/.cache/forexfactory/` by default (override with `--cache-dir` or the
`FOREXFACTORY_CACHE_DIR` environment variable):

```
~/.cache/forexfactory/
|-- manifest.json          # populated scope + per-month provenance
|-- queries/               # per-scope result parquets
|   `-- USD_high_....parquet
`-- YYYY-MM.parquet        # one file per calendar month
```

## Project Structure

```text
forexfactory/
|-- pyproject.toml
|-- requirements.txt
|-- README.md
|-- LICENSE
|-- CHANGELOG.md
|-- CONTRIBUTING.md
|-- src/forexfactory/
|   |-- __init__.py
|   |-- cli.py
|   |-- _cache.py
|   |-- _pipeline.py
|   |-- _populate.py
|   |-- _query.py
|   |-- _refresh.py
|   |-- _scrape.py
|   `-- py.typed
|-- tests/
|   |-- test_cache.py
|   |-- test_cli.py
|   |-- test_docs.py
|   |-- test_pipeline.py
|   |-- test_populate.py
|   |-- test_query.py
|   |-- test_refresh.py
|   `-- test_scrape.py
`-- out/                    # optional raw-staging dir (populated on re-scrape)
```

## Data Source

Data is scraped from the [Forex Factory](https://www.forexfactory.com/calendar) economic calendar. For personal/research use — please respect their terms of service and rate limits.

## License

MIT
