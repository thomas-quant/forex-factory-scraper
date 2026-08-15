"""
Quant helpers for Forex Factory economic event data.
=====================================================
Provides row-aligned, NaN-safe surprise metrics over a DataFrame returned by
forexfactory.read() (or equivalent parquet-sourced DataFrame with the Phase-2
analytical schema columns: actual, forecast, ebaseId).

Usage:
    import forexfactory
    df = forexfactory.read(currencies=["USD"], impacts=["high"])
    df["surprise"] = forexfactory.surprise(df)
    df["surprise_z"] = forexfactory.surprise_z(df)
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def surprise(df: pd.DataFrame) -> pd.Series:
    """Return raw actual − forecast for every row, row-aligned to df.index.

    Satisfies D-01 (raw arithmetic, no polarity adjustment) and D-03 (NaN-propagate,
    never raise, output index equals input index).  When 'actual' or 'forecast' columns
    are absent the entire Series is NaN; pandas column subtraction is otherwise
    inherently row-aligned and NaN-propagating.
    """
    if "actual" not in df.columns or "forecast" not in df.columns:
        return pd.Series(float("nan"), index=df.index)
    return df["actual"] - df["forecast"]


def surprise_z(df: pd.DataFrame) -> pd.Series:
    """Return z-scored surprise per ebaseId group, row-aligned to df.index.

    Computes z = (surprise − group_mean) / group_std over each ebaseId's full
    history present in df (D-02 — single groupby over all rows, look-ahead accepted
    for v1.1).  NaN rules (D-03):
      - NaN actual or forecast => NaN surprise => NaN z (excluded from group stats).
      - Groups with <2 non-NaN releases => NaN (pandas ddof=1 std yields NaN for
        size-1 groups).
      - Groups with std == 0 (constant surprise) => NaN (explicit guard to avoid
        divide-by-zero / inf).
    Output Series is reindexed to df.index so row count and order are preserved even
    when the groupby transform drops or reorders anything.
    """
    if "ebaseId" not in df.columns:
        return pd.Series(float("nan"), index=df.index)

    if df.empty:
        return pd.Series(dtype=float, index=df.index)

    s = surprise(df)

    def _standardize(group: pd.Series) -> pd.Series:
        """Standardize a group's surprise values; return NaN if std==0 or <2 valid."""
        std = group.std()  # ddof=1 — NaN for size-1 groups
        if pd.isna(std) or std == 0:
            return pd.Series(float("nan"), index=group.index)
        mean = group.mean()
        return (group - mean) / std

    result = s.groupby(df["ebaseId"], dropna=False).transform(_standardize)

    # Reindex to df.index so row-alignment is guaranteed even after groupby transform.
    return result.reindex(df.index)


_VINTAGE_COLUMNS = ("actual", "revision", "ebaseId", "datetime_utc")


def actual_initial(df: pd.DataFrame) -> pd.Series:
    """Return the first-printed actual for every row, row-aligned to df.index.

    Forex Factory preserves the value that printed on release day and never rewrites
    it, so the stored 'actual' column *is* the initial vintage.  Verified empirically
    (2026-08-15) by re-scraping 2010-03, 2015-06, 2021-09 and 2024-09 and diffing
    against a 2026-06-14 snapshot: 0 of 188 actuals changed across the two-month gap,
    and 'revision' matches the prior release's actual in only 0.2% of pairs — i.e.
    revisions are carried in their own field rather than overwriting the print.

    This helper exists to name that guarantee at the call site; use it instead of
    reaching for df["actual"] directly when the point-in-time semantics matter.
    """
    if "actual" not in df.columns:
        return pd.Series(float("nan"), index=df.index)
    return pd.to_numeric(df["actual"], errors="coerce")


def actual_revised(df: pd.DataFrame) -> pd.Series:
    """Return the latest known actual per row, row-aligned to df.index.

    Forex Factory reports a revision to period N on the release of period N+1: the
    next row in the same ebaseId series carries the restated figure in its 'revision'
    field.  So the latest known value for a row is the *next* release's revision when
    one was published, and the first print otherwise.

    Yields two vintages (first print, first revision) — not a full revision triangle.
    Later restatements (GDP third estimates, annual benchmark revisions) are only
    captured when Forex Factory surfaces them in a subsequent 'revision' cell.

    NaN rules mirror surprise() (D-03): a NaN 'actual' (unreleased event) yields NaN,
    the most recent release in each series has no successor and so falls back to its
    first print, and missing columns yield an all-NaN Series rather than raising.
    """
    if not set(_VINTAGE_COLUMNS).issubset(df.columns):
        return pd.Series(float("nan"), index=df.index)

    if df.empty:
        return pd.Series(dtype=float, index=df.index)

    # Positional frame: read() sets a DatetimeIndex that repeats for events sharing a
    # release time, so index-based alignment would be ambiguous.  reset_index makes the
    # RangeIndex the identity we sort away from and back to.
    work = pd.DataFrame(
        {
            "dt": pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce"),
            "ebaseId": df["ebaseId"],
            "actual": pd.to_numeric(df["actual"], errors="coerce"),
            "revision": pd.to_numeric(df["revision"], errors="coerce"),
        }
    ).reset_index(drop=True)

    # mergesort is the stable kind — ties in (ebaseId, dt) keep their original order.
    work = work.sort_values(["ebaseId", "dt"], kind="mergesort")
    next_revision = work.groupby("ebaseId", dropna=False, sort=False)["revision"].shift(-1)

    revised = next_revision.where(next_revision.notna(), work["actual"])
    # An unreleased event has no vintage at all, even if the row after it was revised.
    revised = revised.where(work["actual"].notna())

    # sort_index() undoes the sort_values above, restoring df's original row order.
    return pd.Series(revised.sort_index().to_numpy(), index=df.index, name="actual_revised")
