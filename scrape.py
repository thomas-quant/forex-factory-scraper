# ff_rip_month_days_uc.py
"""
Forex Factory Calendar Scraper
==============================
Scrapes economic calendar data from ForexFactory using undetected-chromedriver.

Usage:
    python scrape.py
"""

import os
import sys
import json
import time
import random
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

# ====== LOGGING SETUP ======
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ====== CONFIG ======
START_DATE = "2020-09-01"
END_DATE   = "2025-12-31"
OUT_DIR    = "out"

# Chrome profile path (change this to your Chrome user data path!)
# Windows: C:\Users\<USERNAME>\AppData\Local\Google\Chrome\User Data
# macOS:   ~/Library/Application Support/Google/Chrome
# Linux:   ~/.config/google-chrome
USER_DATA_DIR = r"C:\Users\christ\AppData\Local\Google\Chrome\User Data"
PROFILE_DIR   = "Default"  # e.g. "Profile 1", "Default", etc.

# Optional: residential proxy (scheme://user:pass@host:port) or "" for none
PROXY = ""

# Headless is riskier with CF; start visible for the first run to accept cookies & pass checks.
HEADLESS = False

# Speed settings (reduce for faster scraping, increase if getting blocked)
WAIT_SECS = 4          # Max time to wait for calendar data (was 6)
POST_LOAD_DELAY = 0.8  # Delay after page load (was 1.5)
BETWEEN_PAGES_DELAY = 0.3  # Delay between pages (was 0.6)
POLL_INTERVAL = 0.15   # How often to check for data (was 0.25)

# Set to True for maximum speed (less human-like, higher block risk)
FAST_MODE = True
# ====================

BASE = "https://www.forexfactory.com/calendar"


def month_iter(start: date, end: date):
    """Iterate through months from start to end date."""
    cur = start.replace(day=1)
    while cur <= end:
        yield cur
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)


def ff_month_token(d: date) -> str:
    """Generate ForexFactory month token (e.g., 'jan.2021')."""
    return f"{d.strftime('%b').lower()}.{d.year}"


@dataclass
class MonthPage:
    anchor: date
    url: str


def build_month_pages(start: date, end: date) -> list[MonthPage]:
    """Build list of month pages to scrape."""
    return [MonthPage(m, f"{BASE}?month={ff_month_token(m)}") for m in month_iter(start, end)]


# ---------- undetected-chromedriver setup ----------
import undetected_chromedriver as uc


def build_driver():
    """Build and configure undetected Chrome driver."""
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
    
    # Speed optimizations - disable unnecessary resources
    if FAST_MODE:
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # Disable images
            "profile.managed_default_content_settings.fonts": 2,   # Disable custom fonts
        }
        uc_opts.add_experimental_option("prefs", prefs)
        uc_opts.add_argument("--disable-extensions")

    if PROXY:
        uc_opts.add_argument(f"--proxy-server={PROXY}")

    if HEADLESS:
        # Headless "new" + UA helps, but CF may still challenge; prefer a first run non-headless.
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


def wait_and_get_days(driver, timeout_sec=WAIT_SECS) -> list:
    """Poll for calendar data until timeout or data found."""
    end_time = time.time() + timeout_sec
    best = []
    poll_interval = POLL_INTERVAL if FAST_MODE else POLL_INTERVAL + random.random() * 0.1
    
    while time.time() < end_time:
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
        time.sleep(poll_interval)
    return best


# ---------- main ----------
def main():
    """Main scraping loop."""
    os.makedirs(OUT_DIR, exist_ok=True)

    # Parse and validate dates
    try:
        start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        end = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)

    if start > end:
        logger.error(f"Start date ({start}) is after end date ({end})")
        sys.exit(1)

    pages = build_month_pages(start, end)

    logger.info(f"Chrome profile: {USER_DATA_DIR}")
    logger.info(f"Scraping {len(pages)} months from {start} to {end}")
    if FAST_MODE:
        logger.info("FAST_MODE enabled - reduced delays, images disabled")

    driver = build_driver()
    success_count = 0
    fail_count = 0

    try:
        for i, mp in enumerate(pages, 1):
            out_path = os.path.join(OUT_DIR, f"days_{mp.anchor.strftime('%Y_%m')}.json")

            if os.path.isfile(out_path):
                logger.info(f"[{i}/{len(pages)}] Skip (exists): {mp.anchor:%Y-%m}")
                continue

            logger.info(f"[{i}/{len(pages)}] Loading: {mp.url}")
            driver.get(mp.url)

            # Allow CF checks/cookie popups; do them once manually if needed (first run).
            if FAST_MODE:
                time.sleep(POST_LOAD_DELAY)
            else:
                time.sleep(POST_LOAD_DELAY + random.random() * 0.5)

            days = wait_and_get_days(driver)
            logger.info(f"  -> Extracted {len(days)} days")

            if not days:
                # If CF shows a challenge, PAUSE so you can solve it once,
                # then press Enter in the console to continue scraping.
                try:
                    screenshot_path = os.path.join(OUT_DIR, f"cf_block_{mp.anchor:%Y_%m}.png")
                    driver.save_screenshot(screenshot_path)
                    logger.warning(f"  Screenshot saved: {screenshot_path}")
                    logger.warning("  No days found. If you see a CF check/captcha, solve it in the browser.")
                    input("  Press Enter here after passing the check...")
                    # try again
                    days = wait_and_get_days(driver, timeout_sec=WAIT_SECS)
                    logger.info(f"  -> Retry extracted {len(days)} days")
                except EOFError:
                    logger.warning("  Non-interactive mode, skipping manual check")
                except Exception:
                    pass

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(days, f, ensure_ascii=False, indent=2)

            if days:
                logger.info(f"  Saved: {out_path}")
                success_count += 1
            else:
                logger.warning(f"  Saved empty file: {out_path}")
                fail_count += 1

            # polite pacing
            if FAST_MODE:
                time.sleep(BETWEEN_PAGES_DELAY)
            else:
                time.sleep(BETWEEN_PAGES_DELAY + random.random() * 0.3)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        driver.quit()
        logger.info(f"Done. Success: {success_count}, Failed/Empty: {fail_count}")


if __name__ == "__main__":
    main()
