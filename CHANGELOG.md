# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [1.1.2] - 2026-08-15

### Fixed

- **Every scrape returned HTTP 403 — the scraper was fully broken in 1.1.0 and 1.1.1.** `_scrape.py` hardcoded a `user-agent` header claiming `Chrome/131.0.0.0`, but `curl_cffi` 0.16 resolves the `impersonate="chrome"` alias to **chrome146**. The advertised UA and the underlying TLS/JA3 fingerprint disagreed, and Forex Factory rejected the mismatch on every request. The header is now removed — `curl_cffi` derives a User-Agent that matches whichever profile `IMPERSONATE` selects, so the two can no longer drift apart. If you saw `Failed to scrape … after 3 attempts: HTTP Error 403`, upgrade to 1.1.2.

  `IMPERSONATE` deliberately stays the floating `"chrome"` alias rather than a pinned version: the defect was the *mismatch*, not the float, and pinning `chrome131` would freeze a 2024-era fingerprint that ages badly against anti-bot measures. A regression test now asserts `HEADERS` carries no `user-agent` key.

## [1.1.1] - 2026-06-14

### Fixed

- **`populate` no longer reports a silent `populated=0 skipped=0 empty=0` success** when no raw JSON is found. It now logs a clear warning naming the resolved raw directory and pointing to `forexfactory refresh` (distinguishing a missing dir, an empty dir, and files narrowed out by `--start`/`--end`). The usual cause is running `populate` from a directory whose relative default raw dir (`out`) has no `days_*.json` files.
- **Duplicate `[populate] done …` / `[refresh] done …` log line** — the summary was emitted by both the service layer and the CLI; the redundant CLI line was removed.

### Changed

- **`--currency` and `--impact` accept multiple values per flag** (e.g. `--impact high medium holiday`) across `populate`, `refresh`, and `query`. Repeated flags still accumulate (`--impact high --impact medium`).
- **Scope filters are now case-insensitive** — currencies are normalized to uppercase and impacts to lowercase at the service boundary, so `--currency usd` / `--impact HIGH` match stored data and the manifest scope instead of silently matching nothing.

## [1.1.0] - 2026-06-10

### Added

- **`py.typed` marker** (PEP 561) so downstream type-checkers consume forexfactory's type hints
- **Explicit `__all__`** in `forexfactory/__init__.py` listing the public surface: `get`, `populate`, `__version__`
- **Full distribution metadata** in `pyproject.toml`: description, authors, MIT license, keywords, classifiers, project URLs, `requires-python = ">=3.12"`
- **`LICENSE`** file (MIT License)
- **Dynamic `__version__`** sourced from installed package metadata via `importlib.metadata.version`; no more hardcoded duplicate
- **`ruff` lint + format config** (`[tool.ruff]`) with curated rule set (`E`, `F`, `I`, `UP`, `B`, `SIM`); all source and test files pass clean
- **`mypy` strict type-checking config** (`[tool.mypy]`) against `src/forexfactory`; all nine source modules pass with zero violations
- **GitHub Actions CI** (`.github/workflows/ci.yml`): push/PR trigger, Python 3.12 + 3.13 matrix, runs `python -m pytest -q`, `ruff check .`, `ruff format --check .`, `mypy src/forexfactory`
- **`[project.optional-dependencies] dev`** group in `pyproject.toml` (`pip install -e ".[dev]"` installs ruff, mypy, pytest, pandas-stubs)
- **`CONTRIBUTING.md`** documenting dev setup and the exact CI commands
- **`CHANGELOG.md`** (this file) following Keep a Changelog
- **`forexfactory --version`** prints the installed package version (sourced from `__version__`)
- **`forexfactory status`** reports cache location, covered date range, scope (currencies/impacts), schema version, and settled/matured state; `--json` flag for machine-readable output

### Changed

- **README rewritten** for a public audience: value-prop hero (fetch once, reuse everywhere), badges row (PyPI / Python / CI / License), ~5-line quickstart, then CLI reference, Library API, schema table, cache layout, and project structure
- **Public `get()` and `populate()` fully type-annotated** with complete parameter and return types (`list[str] | None`, `str | None`, `Path | None`, `dict[str, int]`, etc.)
- **Version bumped** from `0.1.0` to `1.1.0`
- **`__version__` now sourced from package metadata** (`importlib.metadata`) rather than a hardcoded literal in `__init__.py`

## [1.0.0] - 2026-06-09

### Added

- **pip-installable `forexfactory` package** (src layout, `pyproject.toml`); `import forexfactory` works
- **Unified `forexfactory` CLI**: `populate` / `refresh` / `query` subcommands with `--currency`, `--impact`, `--start`, `--end`, `--cache-dir`, `--no-auto-fetch`, `--force-refresh` flags
- **Library API** `forexfactory.get(currencies=[...], impacts=[...]) -> pathlib.Path` returning the path to the filtered parquet; `forexfactory.populate(...)` for explicit cache population
- **Shared parquet cache** in a configurable user directory (`~/.cache/forexfactory`); per-month parquet files plus a `manifest.json` sidecar; override with `--cache-dir` or `FOREXFACTORY_CACHE_DIR`
- **Wide analytical schema** (schema version 2): `datetime_utc`, `currency`, `impact`, `title`, `id`, `leaked`, value fields (`forecast_raw`, `actual_raw`, `previous_raw`, `revision_raw`, `forecast`, `actual`, `previous`, `revision`), surprise flags (`actualBetterWorse`, `revisionBetterWorse`), and identity fields (`ebaseId`, `country`, `hasDataValues`)
- **Scope-miss auto-widen**: a `get()`/`query` call for an uncached currency/impact combo automatically fetches the missing scope and widens the cache permanently; failure raises `AutoFetchError` (fail-closed)
- **Matured-month auto-refresh**: settled-false months that have fully passed auto re-fetch on the next `populate`/`query` to fill in actual values; failure serves stale data and warns (never crashes)
- **`auto_fetch` knob** (`get(auto_fetch=False)` / `--no-auto-fetch`) suppresses both auto-triggers for strict cache-only reads
- **Force-refresh on demand**: `--force-refresh` on `populate`/`refresh` (and `force_refresh=` library kwarg) re-scrapes over the network and overwrites cached parquets; partial failures keep prior parquets and report `fetched/skipped/failed`
- **Settled months never auto-refetched** (fully-past history is immutable)
- **Fixture-based regression tests** for the fragile `calendarComponentStates` parser (4 golden HTML fixtures covering both assignment forms, empty state, and multi-candidate)
- **Code-quality baseline**: shared `_deduplicate_rows()`, `--in-dir` honored, no empty-JSON skip-poisoning, stale date defaults removed
- **~195 months of pre-scraped data** (2010-01 through 2026-03) re-processed to schema version 2 with zero HTTP requests
