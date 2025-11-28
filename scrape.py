# ff_rip_nodriver.py
"""
Forex Factory Calendar Scraper (nodriver version)
=================================================
Experimental async scraper using nodriver.

Usage:
    python scrape_nodriver.py
"""

import os
import sys
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import nodriver as uc
from nodriver import cdp

# ====== LOGGING SETUP ======
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for troubleshooting
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ====== CONFIG ======
START_DATE = "2021-01-01"
END_DATE   = "2021-06-30"
OUT_DIR    = "out"

# Use a dedicated scraper profile (not your main Chrome!)
SCRAPER_PROFILE = "./nodriver_profile"

# Speed settings - tune these based on your connection/results
WAIT_SECS = 2.0        # Max time to wait for calendar data
POST_LOAD_DELAY = 1.0  # Delay after navigation for JS to hydrate
POLL_INTERVAL = 0.05   # How often to check for data
BETWEEN_PAGES = 0.1    # Delay between pages

# Block these resource types for faster loading
BLOCK_RESOURCES = True

# Use headless for faster rendering (set False if getting blocked)
HEADLESS = False
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


def build_month_pages(start: date, end: date) -> list[MonthPage]:
    return [MonthPage(m, f"{BASE}?month={ff_month_token(m)}") for m in month_iter(start, end)]


# ---------- JS extraction ----------
JS_READ_CALSTATE = """
(function() {
    const states = window.calendarComponentStates || {};
    const out = [];
    for (const k of Object.keys(states)) {
        const s = states[k];
        if (s && Array.isArray(s.days)) {
            out.push({key: k, days: s.days});
        }
    }
    return JSON.stringify(out);
})()
"""


async def wait_and_get_days(tab, timeout_sec: float = WAIT_SECS) -> list:
    """Poll for calendar data until timeout or data found."""
    end_time = asyncio.get_event_loop().time() + timeout_sec
    best = []
    
    while asyncio.get_event_loop().time() < end_time:
        try:
            raw = await tab.evaluate(JS_READ_CALSTATE)
            if raw:
                arr = json.loads(raw)
                if arr:
                    arr.sort(key=lambda e: (
                        len(e.get("days", [])),
                        sum(len(d.get("events", [])) for d in e.get("days", []))
                    ), reverse=True)
                    best = arr[0]["days"]
                    if best:
                        return best
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL)
    
    return best


async def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(SCRAPER_PROFILE, exist_ok=True)

    try:
        start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        end = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)

    pages = build_month_pages(start, end)
    
    logger.info(f"Using scraper profile: {os.path.abspath(SCRAPER_PROFILE)}")
    logger.info(f"Scraping {len(pages)} months from {start} to {end}")

    # Start browser with speed optimizations
    browser = await uc.start(
        user_data_dir=os.path.abspath(SCRAPER_PROFILE),
        headless=HEADLESS,
        browser_args=[
            "--disable-gpu",
            "--no-sandbox",
            "--window-size=1280,720",  # Smaller window = less rendering
            "--lang=en-US,en",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--disable-logging",
            "--disable-notifications",
            "--disable-default-apps",
            "--disable-popup-blocking",
            "--disable-prompt-on-repost",
            "--disable-hang-monitor",
            "--disable-client-side-phishing-detection",
            "--metrics-recording-only",
            "--no-first-run",
            "--safebrowsing-disable-auto-update",
            "--blink-settings=imagesEnabled=false",  # Disable images
        ]
    )
    
    tab = browser.main_tab
    
    # Block images, fonts, stylesheets via CDP
    if BLOCK_RESOURCES:
        try:
            await tab.send(cdp.network.enable())
            await tab.send(cdp.network.set_blocked_ur_ls([
                "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg", "*.ico",
                "*.woff", "*.woff2", "*.ttf", "*.eot",
                "*.css",
                "*google-analytics*", "*googletagmanager*", "*facebook*",
                "*doubleclick*", "*adsense*",
            ]))
            logger.info("Resource blocking enabled (images, fonts, CSS, trackers)")
        except Exception as e:
            logger.debug(f"Could not enable resource blocking: {e}")
    success_count = 0
    fail_count = 0

    try:
        for i, mp in enumerate(pages, 1):
            out_path = os.path.join(OUT_DIR, f"days_{mp.anchor.strftime('%Y_%m')}.json")

            if os.path.isfile(out_path):
                logger.info(f"[{i}/{len(pages)}] Skip (exists): {mp.anchor:%Y-%m}")
                continue

            logger.info(f"[{i}/{len(pages)}] Loading: {mp.url}")
            
            try:
                # Don't use tab.get() - it waits for full load which can hang on CF pages
                # Instead, navigate and wait manually
                await tab.send(cdp.page.navigate(mp.url))
                logger.debug("Navigation command sent, waiting for page...")
            except Exception as e:
                logger.error(f"Navigation failed: {e}")
                fail_count += 1
                continue

            # Wait for JS to hydrate
            await asyncio.sleep(POST_LOAD_DELAY)

            # Try to extract data
            days = await wait_and_get_days(tab)
            logger.info(f"  -> Extracted {len(days)} days")

            if not days:
                # CF challenge or slow page - prompt for manual intervention
                try:
                    screenshot_path = os.path.join(OUT_DIR, f"cf_block_{mp.anchor:%Y_%m}.png")
                    await tab.save_screenshot(screenshot_path)
                    logger.warning(f"  No data. Screenshot: {screenshot_path}")
                    logger.warning("  If CF challenge, solve it in browser then press Enter...")
                    input()
                    await asyncio.sleep(1)
                    days = await wait_and_get_days(tab, timeout_sec=WAIT_SECS)
                    logger.info(f"  -> Retry: {len(days)} days")
                except (EOFError, KeyboardInterrupt):
                    break
                except Exception:
                    pass

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(days, f, ensure_ascii=False, separators=(",", ":"))

            if days:
                logger.info(f"  Saved: {out_path}")
                success_count += 1
            else:
                logger.warning(f"  Saved empty: {out_path}")
                fail_count += 1

            await asyncio.sleep(BETWEEN_PAGES)

    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        try:
            browser.stop()
        except:
            pass
        logger.info(f"Done. Success: {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    uc.loop().run_until_complete(main())

