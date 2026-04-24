"""
Forex Factory Calendar Scraper (curl_cffi version)
===============================================================
Fetches calendar pages without a browser and extracts the embedded calendar
state from the HTML.

Usage:
    python scrape.py
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - exercised by users without dependency
    curl_requests = None

# ====== LOGGING SETUP ======
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ====== CONFIG ======
START_DATE = "2021-01-01"
END_DATE = "2021-06-30"
OUT_DIR = "out"

# curl_cffi request settings
IMPERSONATE = "chrome"
REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 3
BETWEEN_PAGES_DELAY = 0.0
RETRY_DELAY = 0.0

BASE = "https://www.forexfactory.com/calendar"

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.forexfactory.com/",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}
# ====================


@dataclass(frozen=True)
class MonthPage:
    anchor: date
    url: str


@dataclass(frozen=True)
class ScrapeResult:
    success_count: int
    fail_count: int
    skip_count: int


def month_iter(start: date, end: date):
    """Iterate through month anchors from start to end date."""
    cur = start.replace(day=1)
    while cur <= end:
        yield cur
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)


def ff_month_token(d: date) -> str:
    """Generate Forex Factory month token (e.g. 'jan.2026')."""
    return f"{d.strftime('%b').lower()}.{d.year}"


def build_month_pages(start: date, end: date) -> list[MonthPage]:
    """Build list of month pages to scrape."""
    return [MonthPage(m, f"{BASE}?month={ff_month_token(m)}") for m in month_iter(start, end)]


def _find_matching_brace(text: str, open_index: int) -> int:
    """Return index of matching closing brace, ignoring braces inside strings."""
    depth = 0
    in_string: str | None = None
    escaped = False

    for i in range(open_index, len(text)):
        ch = text[i]

        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_string:
                in_string = None
            continue

        if ch in {'"', "'"}:
            in_string = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i

    raise ValueError("calendarComponentStates object is not closed")


def _find_matching_bracket(text: str, open_index: int) -> int:
    """Return index of matching closing bracket, ignoring brackets inside strings."""
    depth = 0
    in_string: str | None = None
    escaped = False

    for i in range(open_index, len(text)):
        ch = text[i]

        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_string:
                in_string = None
            continue

        if ch in {'"', "'"}:
            in_string = ch
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return i

    raise ValueError("array is not closed")


def _quote_js_object_keys(value: str) -> str:
    """Quote simple JavaScript object keys so JSON can parse them."""
    return re.sub(r'([,{]\s*)([A-Za-z_$][\w$]*)(\s*:)', r'\1"\2"\3', value)


def _replace_single_quoted_strings(value: str) -> str:
    """Convert JavaScript single-quoted strings to JSON strings outside JSON strings."""
    out: list[str] = []
    i = 0
    in_double = False
    escaped = False

    while i < len(value):
        ch = value[i]

        if in_double:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_double = False
            i += 1
            continue

        if ch == '"':
            in_double = True
            out.append(ch)
            i += 1
            continue

        if ch != "'":
            out.append(ch)
            i += 1
            continue

        i += 1
        chars: list[str] = []
        escaped_single = False
        while i < len(value):
            inner = value[i]
            if escaped_single:
                chars.append(inner)
                escaped_single = False
            elif inner == "\\":
                escaped_single = True
            elif inner == "'":
                break
            else:
                chars.append(inner)
            i += 1

        out.append(json.dumps("".join(chars)))
        if i < len(value) and value[i] == "'":
            i += 1

    return "".join(out)


def _loads_js_object(value: str) -> Any:
    return json.loads(_replace_single_quoted_strings(_quote_js_object_keys(value)))


def _extract_state_json(html: str) -> str:
    """Extract whole-object assignment: window.calendarComponentStates = {...}."""
    marker = "window.calendarComponentStates"
    search_from = 0

    while True:
        marker_index = html.find(marker, search_from)
        if marker_index == -1:
            raise ValueError("calendarComponentStates not found")

        equals_index = html.find("=", marker_index + len(marker))
        if equals_index == -1:
            raise ValueError("calendarComponentStates assignment not found")

        between = html[marker_index + len(marker) : equals_index].strip()
        if between == "":
            open_index = html.find("{", equals_index)
            if open_index == -1:
                raise ValueError("calendarComponentStates object not found")
            close_index = _find_matching_brace(html, open_index)
            return html[open_index : close_index + 1]

        search_from = equals_index + 1


def _extract_assigned_state_objects(html: str) -> dict[str, Any]:
    """Extract bracket assignments: window.calendarComponentStates[1] = {...}."""
    pattern = re.compile(r"window\.calendarComponentStates\[([^\]]+)\]\s*=")
    states: dict[str, Any] = {}

    for match in pattern.finditer(html):
        open_index = html.find("{", match.end())
        if open_index == -1:
            continue
        close_index = _find_matching_brace(html, open_index)
        raw = html[open_index : close_index + 1]
        states[match.group(1).strip("'\"")] = {"days": _extract_days_array_from_state_object(raw)}

    if not states:
        raise ValueError("calendarComponentStates assignments not found")
    return states


def _select_best_days(states: dict[str, Any]) -> list:
    candidates = []
    for key, state in states.items():
        if isinstance(state, dict) and isinstance(state.get("days"), list):
            days = state["days"]
            event_count = sum(
                len(day.get("events", []))
                for day in days
                if isinstance(day, dict)
            )
            candidates.append((len(days), event_count, key, days))

    if not candidates:
        return []

    candidates.sort(reverse=True)
    return candidates[0][3]


def _extract_days_array_from_state_object(raw: str) -> list:
    """Extract and parse only the `days` array from a JS state object."""
    match = re.search(r'([,{]\s*)days\s*:', raw)
    if not match:
        return []

    array_start = raw.find("[", match.end())
    if array_start == -1:
        return []

    array_end = _find_matching_bracket(raw, array_start)
    return json.loads(raw[array_start : array_end + 1])


def extract_days(html: str) -> list:
    """Extract the most complete `days` list from Forex Factory HTML."""
    try:
        state_json = _extract_state_json(html)
        states = _loads_js_object(state_json)
    except ValueError:
        states = _extract_assigned_state_objects(html)

    if not isinstance(states, dict):
        return []
    return _select_best_days(states)


def build_session():
    """Create a curl_cffi session."""
    if curl_requests is None:
        raise RuntimeError("curl_cffi is required. Install it with: pip install curl_cffi")
    return curl_requests.Session()


def scrape_month(
    session,
    page: MonthPage,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    retry_delay: float = RETRY_DELAY,
) -> list:
    """Fetch a month page and return extracted days."""
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(
                page.url,
                headers=HEADERS,
                impersonate=IMPERSONATE,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            days = extract_days(response.text)
            if days:
                return days
            last_error = ValueError("no calendar days found in response")
        except Exception as exc:  # retry all transient fetch/parse failures
            last_error = exc

        if attempt < max_attempts:
            logger.debug("Attempt %s/%s failed for %s: %s", attempt, max_attempts, page.url, last_error)
            if retry_delay > 0:
                time.sleep(retry_delay)

    if last_error:
        logger.warning("Failed to scrape %s after %s attempts: %s", page.anchor, max_attempts, last_error)
    return []


def run_scraper(
    pages: list[MonthPage],
    *,
    out_dir: str = OUT_DIR,
    session=None,
    between_pages_delay: float = BETWEEN_PAGES_DELAY,
    retry_delay: float = RETRY_DELAY,
) -> ScrapeResult:
    """Scrape pages, skip existing files, write days_YYYY_MM.json files."""
    os.makedirs(out_dir, exist_ok=True)
    session = session or build_session()

    success_count = 0
    fail_count = 0
    skip_count = 0

    for i, page in enumerate(pages, 1):
        out_path = os.path.join(out_dir, f"days_{page.anchor.strftime('%Y_%m')}.json")

        if os.path.isfile(out_path):
            logger.info("[%s/%s] Skip (exists): %s", i, len(pages), page.anchor.strftime("%Y-%m"))
            skip_count += 1
            continue

        logger.info("[%s/%s] Loading: %s", i, len(pages), page.url)
        days = scrape_month(session, page, retry_delay=retry_delay)
        logger.info("  -> Extracted %s days", len(days))

        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(days, handle, ensure_ascii=False, separators=(",", ":"))

        if days:
            logger.info("  Saved: %s", out_path)
            success_count += 1
        else:
            logger.warning("  Saved empty: %s", out_path)
            fail_count += 1

        if between_pages_delay > 0:
            time.sleep(between_pages_delay)

    return ScrapeResult(success_count=success_count, fail_count=fail_count, skip_count=skip_count)


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Forex Factory scraper using curl_cffi")
    parser.add_argument("--start-date", default=START_DATE, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end-date", default=END_DATE, help="End date, YYYY-MM-DD")
    parser.add_argument("--out-dir", default=OUT_DIR, help="Output directory for days_YYYY_MM.json files")
    parser.add_argument(
        "--between-pages-delay",
        type=float,
        default=BETWEEN_PAGES_DELAY,
        help="Seconds to sleep between month requests (default: 0)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=RETRY_DELAY,
        help="Seconds to sleep before retrying a failed month request (default: 0)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> ScrapeResult:
    args = parse_args(argv)

    try:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError as exc:
        logger.error("Invalid date format: %s", exc)
        sys.exit(1)

    if start > end:
        logger.error("Start date (%s) is after end date (%s)", start, end)
        sys.exit(1)

    pages = build_month_pages(start, end)
    logger.info("curl_cffi scraper. Scraping %s months from %s to %s", len(pages), start, end)
    result = run_scraper(
        pages,
        out_dir=args.out_dir,
        between_pages_delay=args.between_pages_delay,
        retry_delay=args.retry_delay,
    )
    logger.info("Done. Success: %s, Failed/Empty: %s, Skipped: %s", result.success_count, result.fail_count, result.skip_count)
    return result


if __name__ == "__main__":
    main()
