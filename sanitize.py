import csv
import os

# ====== CONFIG ======
IN_CSV  = "ff_usd_high_holiday.csv"        # input from the parser step
OUT_CSV = "ff_usd_high_holiday_clean.csv"  # output after removing "speaks"
# ====================

def keep_row(row: dict) -> bool:
    title = (row.get("title") or "").lower()
    return "speaks" not in title  # strict to your requirement

def main():
    assert os.path.isfile(IN_CSV), f"Input CSV not found: {IN_CSV}"
    with open(IN_CSV, "r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)
        rows = [r for r in reader if keep_row(r)]
        fieldnames = reader.fieldnames

    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"[done] kept {len(rows)} rows -> {OUT_CSV}")

if __name__ == "__main__":
    main()
