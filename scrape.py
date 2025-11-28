# ff_rip_month_days_uc.py
import os, json, time, random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

# ====== CONFIG ======
START_DATE = "2021-01-01"
END_DATE   = "2024-12-31"
OUT_DIR    = "out"

# Use your real Chrome profile (change this path!)
# On Windows: C:\Users\<YOU>\AppData\Local\Google\Chrome\User Data
USER_DATA_DIR = r"C:\Users\YOU\AppData\Local\Google\Chrome\User Data"
PROFILE_DIR   = "Default"  # e.g. "Profile 1", "Default", etc.

# Optional: residential proxy (scheme://user:pass@host:port) or "" for none
PROXY = ""

# Headless is riskier with CF; start visible for the first run to accept cookies & pass checks.
HEADLESS = False
WAIT_SECS = 6
# ====================

BASE = "https://www.forexfactory.com/calendar"

def month_iter(start: date, end: date):
    cur = start.replace(day=1)
    while cur <= end:
        yield cur
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)

def ff_month_token(d: date) -> str:
    return f"{d.strftime('%b').lower()}.{d.year}"

@dataclass
class MonthPage:
    anchor: date
    url: str

def build_month_pages(start: date, end: date):
    return [MonthPage(m, f"{BASE}?month={ff_month_token(m)}") for m in month_iter(start, end)]

# ---------- undetected-chromedriver setup ----------
import undetected_chromedriver as uc

def build_driver():
    uc_opts = uc.ChromeOptions()

    # Real profile (huge for CF)
    if USER_DATA_DIR:
        uc_opts.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    if PROFILE_DIR:
        uc_opts.add_argument(f"--profile-directory={PROFILE_DIR}")

    # Normal-looking flags
    uc_opts.add_argument("--disable-gpu")
    uc_opts.add_argument("--no-sandbox")
    uc_opts.add_argument("--window-size=1400,1000")
    uc_opts.add_argument("--lang=en-US,en")
    # Avoid the obvious Selenium flag
    uc_opts.add_argument("--disable-blink-features=AutomationControlled")

    if PROXY:
        uc_opts.add_argument(f"--proxy-server={PROXY}")

    if HEADLESS:
        # Headless “new” + UA helps, but CF may still challenge; prefer a first run non-headless.
        uc_opts.add_argument("--headless=new")

    driver = uc.Chrome(options=uc_opts, use_subprocess=True)
    driver.set_page_load_timeout(75)
    driver.implicitly_wait(2)

    # Make navigator.webdriver False (uc usually does this already)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """
        })
    except Exception:
        pass

    return driver

# ---------- JS read ----------
JS_READ_CALSTATE = r"""
const states = window.calendarComponentStates || {};
const out = [];
for (const k of Object.keys(states)) {
  const s = states[k];
  if (s && Array.isArray(s.days)) out.push({key:k, days:s.days});
}
return JSON.stringify(out);
"""

def wait_and_get_days(driver, timeout_sec=WAIT_SECS):
    end = time.time() + timeout_sec
    best = []
    while time.time() < end:
        try:
            raw = driver.execute_script(JS_READ_CALSTATE)
            if raw:
                arr = json.loads(raw)
                if arr:
                    # pick the entry with most days/events
                    arr.sort(key=lambda e: (len(e.get("days", [])),
                                            sum(len(d.get("events", [])) for d in e.get("days", []))),
                             reverse=True)
                    best = arr[0]["days"]
                    if best:
                        return best
        except Exception:
            pass
        time.sleep(0.25 + random.random()*0.25)
    return best

# ---------- main ----------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end   = datetime.strptime(END_DATE,   "%Y-%m-%d").date()
    pages = build_month_pages(start, end)

    driver = build_driver()
    try:
        for mp in pages:
            out_path = os.path.join(OUT_DIR, f"days_{mp.anchor.strftime('%Y_%m')}.json")
            if os.path.isfile(out_path):
                print(f"[skip] {mp.anchor:%Y-%m} -> {out_path}")
                continue

            url = mp.url
            print(f"[load] {url}")
            driver.get(url)

            # Allow CF checks/cookie popups; do them once manually if needed (first run).
            time.sleep(1.5 + random.random()*0.7)

            days = wait_and_get_days(driver)
            print(f"  -> extracted days: {len(days)}")

            if not days:
                # If CF shows a challenge, PAUSE so you can solve it once,
                # then press Enter in the console to continue scraping.
                try:
                    driver.save_screenshot(os.path.join(OUT_DIR, f"cf_block_{mp.anchor:%Y_%m}.png"))
                    print("  !! No days found. If you see a CF check/captcha, solve it in the browser window.")
                    input("  Press Enter here after passing the check...")
                    # try again
                    days = wait_and_get_days(driver, timeout_sec=WAIT_SECS)
                    print(f"  -> retry extracted days: {len(days)}")
                except Exception:
                    pass

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(days, f, ensure_ascii=False, indent=2)
            print(f"[save] {out_path}")

            # polite pacing
            time.sleep(0.6 + random.random()*0.4)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
