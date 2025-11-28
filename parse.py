import os, json, glob, csv
from datetime import datetime, timezone

# ========= CONFIG =========
IN_DIR   = "out"                       # where days_YYYY_MM.json live
OUT_CSV  = "ff_usd_high_holiday.csv"   # output file
KEEP_CURRENCIES = {"USD"}              # only USD
KEEP_IMPACTS    = {"high", "holiday"}  # red folder + bank holidays
# =========================

def load_days_files(in_dir: str):
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
    # we rely on "impactName" but add a safety map
    s = (s or "").strip().lower()
    if s in {"high", "holiday", "medium", "low"}:
        return s
    # fallbacks from other fields if you ever use them
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
    """events[].dateline appears to be epoch seconds (UTC)."""
    if not dt_epoch:
        return "", ""
    try:
        ts = float(dt_epoch)
        if ts > 10_000_000_000:  # just in case it's ms
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.date().isoformat(), dt.strftime("%H:%M:%S")
    except Exception:
        return "", ""

def flatten(days, src_path):
    for d in days:
        # day-level dateline is midnight; event has its own dateline we’ll use
        for ev in d.get("events", []):
            # fields we care about (present in your sample)
            currency   = (ev.get("currency") or "").upper()
            impact     = norm_impact(ev.get("impactName") or ev.get("impactTitle") or "")
            title      = ev.get("prefixedName") or ev.get("name") or ev.get("soloTitle") or ""
            dateline   = ev.get("dateline")  # epoch seconds
            date_iso, time_utc = to_iso(dateline)
            yield {
                "date": date_iso,                 # UTC date from epoch
                "time_utc": time_utc,             # HH:MM:SS UTC
                "currency": currency,
                "impact": impact,                  # 'high' or 'holiday' etc.
                "title": title,
                "id": ev.get("id", ""),
            }

def main():
    rows = []
    for path, days in load_days_files(IN_DIR):
        for r in flatten(days, path):
            if KEEP_CURRENCIES and r["currency"] not in KEEP_CURRENCIES:
                continue
            if KEEP_IMPACTS and r["impact"] not in KEEP_IMPACTS:
                continue
            rows.append(r)

    # de-dupe (in case months overlap or FF repeats an event)
    dedup = {}
    for r in rows:
        key = (r["id"], r["date"], r["time_utc"]) if r["id"] else (r["date"], r["time_utc"], r["currency"], r["title"])
        dedup[key] = r
    rows = list(dedup.values())
    rows.sort(key=lambda x: (x["date"], x["time_utc"], x["title"]))

    os.makedirs(os.path.dirname(OUT_CSV) or ".", exist_ok=True)
    cols = ["date","time_utc","currency","impact","title","id"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    print(f"[done] wrote {len(rows)} rows -> {OUT_CSV}")

if __name__ == "__main__":
    main()
