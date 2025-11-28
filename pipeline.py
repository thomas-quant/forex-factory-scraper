"""
Forex Factory Economic Events Pipeline
=======================================
Consolidates: parse.py, sanitize.py, to_parquet.py

Usage:
  python pipeline.py                    # full pipeline -> parquet only (no CSV)
  python pipeline.py --step parse       # JSON -> CSV
  python pipeline.py --step sanitize    # sanitize CSV -> CSV
  python pipeline.py --step parquet     # CSV -> Parquet
"""

import os
import json
import glob
import csv
import argparse
from datetime import datetime, timezone

import pandas as pd

# ========= CONFIG =========
IN_DIR = "out"                              # where days_YYYY_MM.json live
PARSED_CSV = "ff_usd_high_holiday.csv"      # output from parse step
CLEAN_CSV = "ff_usd_high_holiday_clean.csv" # output from sanitize step
OUT_PARQUET = "economic_events.parquet"     # final parquet output
KEEP_CURRENCIES = {"USD"}                   # only USD
KEEP_IMPACTS = {"high", "holiday"}          # red folder + bank holidays
# ==========================


# ─────────────────────────────────────────────────────────────────────────────
# PARSE: JSON -> CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_days_files(in_dir: str):
    """Yield (path, days_list) for each days_*.json file."""
    paths = sorted(glob.glob(os.path.join(in_dir, "days_*.json")))
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            try:
                days = json.load(f)
                if isinstance(days, list):
                    yield p, days
            except json.JSONDecodeError:
                print(f"[warn] bad JSON: {p}")


def norm_impact(s: str) -> str:
    """Normalize impact string to one of: high, holiday, medium, low."""
    s = (s or "").strip().lower()
    if s in {"high", "holiday", "medium", "low"}:
        return s
    if "non-economic" in s or "holiday" in s or "bank" in s:
        return "holiday"
    if "high" in s or "red" in s:
        return "high"
    if "medium" in s or "orange" in s:
        return "medium"
    if "low" in s or "yellow" in s:
        return "low"
    return s


def to_iso(dt_epoch: int | float | None):
    """Convert epoch seconds (UTC) to (date_iso, time_utc)."""
    if not dt_epoch:
        return "", ""
    try:
        ts = float(dt_epoch)
        if ts > 10_000_000_000:  # milliseconds -> seconds
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.date().isoformat(), dt.strftime("%H:%M:%S")
    except Exception:
        return "", ""


def flatten_events(days, src_path=None):
    """Flatten nested days/events structure into individual event dicts."""
    for d in days:
        for ev in d.get("events", []):
            currency = (ev.get("currency") or "").upper()
            impact = norm_impact(ev.get("impactName") or ev.get("impactTitle") or "")
            title = ev.get("prefixedName") or ev.get("name") or ev.get("soloTitle") or ""
            dateline = ev.get("dateline")
            date_iso, time_utc = to_iso(dateline)
            yield {
                "date": date_iso,
                "time_utc": time_utc,
                "currency": currency,
                "impact": impact,
                "title": title,
                "id": ev.get("id", ""),
            }


def parse_json_to_csv(
    in_dir: str = IN_DIR,
    out_csv: str = PARSED_CSV,
    keep_currencies: set = KEEP_CURRENCIES,
    keep_impacts: set = KEEP_IMPACTS,
) -> str:
    """Parse all days_*.json files and write filtered events to CSV."""
    rows = []
    for path, days in load_days_files(in_dir):
        for r in flatten_events(days, path):
            if keep_currencies and r["currency"] not in keep_currencies:
                continue
            if keep_impacts and r["impact"] not in keep_impacts:
                continue
            rows.append(r)

    # De-duplicate
    dedup = {}
    for r in rows:
        key = (r["id"], r["date"], r["time_utc"]) if r["id"] else (r["date"], r["time_utc"], r["currency"], r["title"])
        dedup[key] = r
    rows = list(dedup.values())
    rows.sort(key=lambda x: (x["date"], x["time_utc"], x["title"]))

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    cols = ["date", "time_utc", "currency", "impact", "title", "id"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    print(f"[parse] wrote {len(rows)} rows -> {out_csv}")
    return out_csv


# ─────────────────────────────────────────────────────────────────────────────
# SANITIZE: Remove unwanted rows from CSV
# ─────────────────────────────────────────────────────────────────────────────

def should_keep_row(row: dict) -> bool:
    """Return True if the row should be kept (no 'speaks' in title)."""
    title = (row.get("title") or "").lower()
    return "speaks" not in title


def sanitize_csv(in_csv: str = PARSED_CSV, out_csv: str = CLEAN_CSV) -> str:
    """Remove rows with 'speaks' in the title."""
    if not os.path.isfile(in_csv):
        raise FileNotFoundError(f"Input CSV not found: {in_csv}")

    with open(in_csv, "r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)
        rows = [r for r in reader if should_keep_row(r)]
        fieldnames = reader.fieldnames

    with open(out_csv, "w", encoding="utf-8", newline="") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"[sanitize] kept {len(rows)} rows -> {out_csv}")
    return out_csv


# ─────────────────────────────────────────────────────────────────────────────
# PARQUET: Convert CSV to Parquet
# ─────────────────────────────────────────────────────────────────────────────

def csv_to_parquet(csv_path: str, parquet_path: str = None) -> str:
    """Convert CSV to Parquet, combining date+time into datetime_utc column."""
    if parquet_path is None:
        parquet_path = os.path.splitext(csv_path)[0] + ".parquet"

    df = pd.read_csv(csv_path)

    # Combine date + time into a single datetime column (UTC)
    if "date" in df.columns and "time_utc" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["date"] + " " + df["time_utc"], utc=True)
        df = df.drop(columns=["date", "time_utc"])
        df = df[["datetime_utc"] + [c for c in df.columns if c != "datetime_utc"]]

    df.to_parquet(parquet_path, index=False)
    print(f"[parquet] converted -> {parquet_path}")
    return parquet_path


# ─────────────────────────────────────────────────────────────────────────────
# MAIN: Run full pipeline or individual steps
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(out_parquet: str = OUT_PARQUET):
    """Run full pipeline in memory: JSON -> filtered -> sanitized -> Parquet (no intermediate CSVs)."""
    # Parse JSON files
    rows = []
    for path, days in load_days_files(IN_DIR):
        for r in flatten_events(days, path):
            if KEEP_CURRENCIES and r["currency"] not in KEEP_CURRENCIES:
                continue
            if KEEP_IMPACTS and r["impact"] not in KEEP_IMPACTS:
                continue
            rows.append(r)

    # De-duplicate
    dedup = {}
    for r in rows:
        key = (r["id"], r["date"], r["time_utc"]) if r["id"] else (r["date"], r["time_utc"], r["currency"], r["title"])
        dedup[key] = r
    rows = list(dedup.values())
    rows.sort(key=lambda x: (x["date"], x["time_utc"], x["title"]))

    # Sanitize (remove 'speaks')
    rows = [r for r in rows if should_keep_row(r)]

    # Convert to DataFrame and output parquet
    df = pd.DataFrame(rows)
    if "date" in df.columns and "time_utc" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["date"] + " " + df["time_utc"], utc=True)
        df = df.drop(columns=["date", "time_utc"])
        df = df[["datetime_utc"] + [c for c in df.columns if c != "datetime_utc"]]

    df.to_parquet(out_parquet, index=False)
    print(f"[done] {len(df)} rows -> {out_parquet}")


def main():
    parser = argparse.ArgumentParser(
        description="Forex Factory Economic Events Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Steps:
  parse     - Parse JSON files to CSV (filtered by currency/impact)
  sanitize  - Remove 'speaks' events from CSV
  parquet   - Convert CSV to Parquet format
  
Without --step, runs full pipeline directly to Parquet (no intermediate CSVs).
        """,
    )
    parser.add_argument(
        "--step",
        choices=["parse", "sanitize", "parquet"],
        help="Run a specific step only",
    )
    parser.add_argument("--in-dir", default=IN_DIR, help="Input directory for JSON files")
    parser.add_argument("--csv", help="Input CSV for sanitize/parquet steps")
    parser.add_argument("--out", help="Output file path")

    args = parser.parse_args()

    if args.step == "parse":
        out = args.out or PARSED_CSV
        parse_json_to_csv(in_dir=args.in_dir, out_csv=out)

    elif args.step == "sanitize":
        in_csv = args.csv or PARSED_CSV
        out_csv = args.out or CLEAN_CSV
        sanitize_csv(in_csv=in_csv, out_csv=out_csv)

    elif args.step == "parquet":
        in_csv = args.csv or CLEAN_CSV
        out_parquet = args.out or OUT_PARQUET
        csv_to_parquet(csv_path=in_csv, parquet_path=out_parquet)

    else:
        # No step specified -> run full pipeline (parquet only, no intermediate CSVs)
        out_parquet = args.out or OUT_PARQUET
        run_pipeline(out_parquet=out_parquet)


if __name__ == "__main__":
    main()

