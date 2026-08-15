"""
forexfactory — Cached Economic Calendar Data Provider
======================================================
Install once, fetch once, and read the Forex Factory economic calendar from
any project via a shared local cache.

Usage:
    from forexfactory import get, populate
    path = get(currencies=["USD"], impacts=["high"])
    # path is a pathlib.Path to a filtered Parquet file

    result = populate(force_refresh=True, start="2025-01")
    # result == {"fetched": N, "skipped": N, "failed": N}
"""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import pandas as pd

from forexfactory._analytics import actual_initial, actual_revised, surprise, surprise_z

try:
    __version__: str = version("forexfactory")
except PackageNotFoundError:  # pragma: no cover - exercised in non-installed environments
    __version__ = "0.0.0"

__all__ = [
    "get",
    "read",
    "populate",
    "surprise",
    "surprise_z",
    "actual_initial",
    "actual_revised",
    "__version__",
]


def get(
    *,
    currencies: list[str] | None = None,
    impacts: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    include_no_data: bool = False,
    cache_dir: Path | None = None,
    auto_fetch: bool = True,
) -> Path:
    """Return a Path to a filtered Parquet file from the local cache.

    Args:
        auto_fetch: When True (default), auto-refresh matured months and widen scope
                    on miss before returning (CACHE-05 / D-07/D-09). When False, strict
                    cache-only read — raises ValueError on scope miss or stale data.

    Lazily imports _query so that `import forexfactory` works before
    _query.py exists (it is implemented in plan 04).
    """
    from . import _query  # noqa: PLC0415 — intentional lazy import

    return _query.run_query(
        currencies=currencies,
        impacts=impacts,
        start=start,
        end=end,
        include_no_data=include_no_data,
        cache_dir=cache_dir,
        auto_fetch=auto_fetch,
        # session and progress are not exposed at the library level (D-11 — library silent)
    )


def read(
    *,
    currencies: list[str] | None = None,
    impacts: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    include_no_data: bool = False,
    cache_dir: Path | None = None,
    auto_fetch: bool = True,
) -> pd.DataFrame:
    """Return a filtered DataFrame from the local cache with a sorted DatetimeIndex.

    Mirrors `get()` exactly (D-08): same keyword-only signature, calls `get()` to
    resolve the result parquet Path, then loads it via `pd.read_parquet`.

    Post-load transform (D-10): sets a DatetimeIndex named 'datetime_utc' — coercing
    that column to a UTC DatetimeIndex so the invariant holds even for a zero-row
    result — while retaining 'datetime_utc' as a column, then sorts ascending.  Apart
    from the index coercion, columns are returned as stored — no other dtype coercion
    (D-11).  No surprise columns added (D-09) — use `forexfactory.surprise(df)`
    and `forexfactory.surprise_z(df)` for that.

    The result always carries a `siteId` column (null for pre-bump months — D-15).
    """
    path = get(
        currencies=currencies,
        impacts=impacts,
        start=start,
        end=end,
        include_no_data=include_no_data,
        cache_dir=cache_dir,
        auto_fetch=auto_fetch,
    )
    df = pd.read_parquet(path)
    # Set a DatetimeIndex named 'datetime_utc', retaining it as a column too (D-10).
    # pd.to_datetime(..., utc=True) coerces object-dtype columns (empty PHASE2_COLUMNS path)
    # to a proper DatetimeIndex regardless of row count (T-05-10 / WR-01).
    # drop=False keeps the column; set_index copies the coerced Series as the index.
    df = df.set_index(pd.to_datetime(df["datetime_utc"], utc=True), drop=False)
    df.index.name = "datetime_utc"
    # Sort ascending by the DatetimeIndex (D-10); handles unordered per-month concat.
    return df.sort_index()


def populate(
    *,
    currencies: list[str] | None = None,
    impacts: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    raw_dir: str | Path | None = None,
    cache_dir: Path | None = None,
    force: bool = False,
    force_refresh: bool = False,
    auto_fetch: bool = True,
) -> dict[str, int]:
    """Populate or refresh the local cache; library mirror of the populate CLI command.

    With force_refresh=False (default): reads on-disk raw JSON and builds parquet.
    With auto_fetch=True (default): also auto-refreshes matured months over the
    network before the disk-ingest loop (CACHE-05).  Pass auto_fetch=False for
    strict cache-only behavior (zero network calls).
    Returns {"populated": N, "skipped": N, "empty": N}.

    With force_refresh=True: re-scrapes the requested range over the network and
    overwrites existing cached parquets (CACHE-06 / D-03). Returns
    {"fetched": N, "skipped": N, "failed": N}.

    Args:
        auto_fetch: When True (default), auto-refresh matured months before the
                    disk-ingest loop (CACHE-05 / D-08/D-09). When False, strict
                    cache-only — no automatic network activity.
    """
    from . import _populate  # noqa: PLC0415 — intentional lazy import

    if raw_dir is not None:
        return _populate.run_populate(
            currencies=currencies,
            impacts=impacts,
            start=start,
            end=end,
            raw_dir=str(raw_dir),
            cache_dir=cache_dir,
            force=force,
            force_refresh=force_refresh,
            auto_fetch=auto_fetch,
        )
    return _populate.run_populate(
        currencies=currencies,
        impacts=impacts,
        start=start,
        end=end,
        cache_dir=cache_dir,
        force=force,
        force_refresh=force_refresh,
        auto_fetch=auto_fetch,
    )
