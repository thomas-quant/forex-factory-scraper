"""
Forex Factory Calendar Scraper (curl_cffi version)
===============================================================
Fetches calendar pages without a browser and extracts the embedded calendar
state from the HTML. Used by forexfactory._refresh to back the `refresh`
command (SRC-02).

Usage:
    from forexfactory._scrape import run_scraper, build_month_pages
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - exercised by users without dependency
    curl_requests = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# ====== CONFIG ======
OUT_DIR = "out"

# curl_cffi request settings
# Floating alias on purpose: it tracks curl_cffi's newest Chrome profile, so the
# fingerprint stays current as the library updates.  Do NOT pair this with a
# hardcoded user-agent header (see HEADERS below).
IMPERSONATE = "chrome"
REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 3
BETWEEN_PAGES_DELAY = 1.0  # D-11: polite non-zero default (CONCERNS: was 0.0)
RETRY_DELAY = 1.0  # D-11: polite non-zero default (CONCERNS: was 0.0)

BASE = "https://www.forexfactory.com/calendar"

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",  # noqa: E501 — literal HTTP Accept header; cannot shorten
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.forexfactory.com/",
    "upgrade-insecure-requests": "1",
    # NO "user-agent" HERE — DELIBERATE.  curl_cffi derives a User-Agent that matches
    # the IMPERSONATE profile's TLS/JA3 fingerprint.  Hardcoding one desynchronises the
    # two the moment the profile moves: a pinned Chrome/131 UA against the chrome146
    # fingerprint that IMPERSONATE="chrome" resolves to under curl_cffi 0.16 is a 403
    # from Forex Factory on every request.  Let the profile own the UA.
    # Guarded by test_headers_declare_no_user_agent.
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


def month_iter(start: date, end: date) -> Iterator[date]:
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
    return re.sub(r"([,{]\s*)([A-Za-z_$][\w$]*)(\s*:)", r'\1"\2"\3', value)


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


def _select_best_days(states: dict[str, Any]) -> list[Any]:
    candidates = []
    for key, state in states.items():
        if isinstance(state, dict) and isinstance(state.get("days"), list):
            days = state["days"]
            event_count = sum(len(day.get("events", [])) for day in days if isinstance(day, dict))
            candidates.append((len(days), event_count, key, days))

    if not candidates:
        return []

    candidates.sort(reverse=True)
    result: list[Any] = candidates[0][3]
    return result


def _extract_days_array_from_state_object(raw: str) -> list[Any]:
    """Extract and parse only the `days` array from a JS state object."""
    match = re.search(r"([,{]\s*)days\s*:", raw)
    if not match:
        return []

    array_start = raw.find("[", match.end())
    if array_start == -1:
        return []

    array_end = _find_matching_bracket(raw, array_start)
    parsed: list[Any] = json.loads(raw[array_start : array_end + 1])
    return parsed


def extract_days(html: str) -> list[Any]:
    """Extract the most complete `days` list from Forex Factory HTML."""
    try:
        state_json = _extract_state_json(html)
        states = _loads_js_object(state_json)
    except ValueError:
        states = _extract_assigned_state_objects(html)

    if not isinstance(states, dict):
        return []
    return _select_best_days(states)


def build_session() -> Any:
    """Create a curl_cffi session."""
    if curl_requests is None:
        raise RuntimeError("curl_cffi is required. Install it with: pip install curl_cffi")
    return curl_requests.Session()


def scrape_month(
    session: Any,
    page: MonthPage,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    retry_delay: float = RETRY_DELAY,
) -> list[Any]:
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
            logger.debug(
                "Attempt %s/%s failed for %s: %s", attempt, max_attempts, page.url, last_error
            )
            if retry_delay > 0:
                time.sleep(retry_delay)

    if last_error:
        logger.warning(
            "Failed to scrape %s after %s attempts: %s", page.anchor, max_attempts, last_error
        )
    return []


def run_scraper(
    pages: list[MonthPage],
    *,
    out_dir: str = OUT_DIR,
    session: Any = None,
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

        # WR-03: also require file size > 0 so a pre-existing 0-byte file does
        # not permanently suppress re-scraping this month.
        if os.path.isfile(out_path) and os.path.getsize(out_path) > 0:
            logger.info("[%s/%s] Skip (exists): %s", i, len(pages), page.anchor.strftime("%Y-%m"))
            skip_count += 1
            continue

        logger.info("[%s/%s] Loading: %s", i, len(pages), page.url)
        days = scrape_month(session, page, retry_delay=retry_delay)
        logger.info("  -> Extracted %s days", len(days))

        # QUAL-03: only write file when days is non-empty; never write an empty
        # JSON file that would permanently poison the skip-check on future runs.
        if days:
            with open(out_path, "w", encoding="utf-8") as handle:
                json.dump(days, handle, ensure_ascii=False, separators=(",", ":"))
            logger.info("  Saved: %s", out_path)
            success_count += 1
        else:
            logger.warning("  Skipping write for %s — no days extracted", page.anchor)
            fail_count += 1

        if between_pages_delay > 0:
            time.sleep(between_pages_delay)

    return ScrapeResult(success_count=success_count, fail_count=fail_count, skip_count=skip_count)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forex Factory scraper using curl_cffi")
    # QUAL-04: stale hardcoded date defaults removed; dates are now
    # required (default=None); refresh computes its range dynamically in _refresh.py.
    parser.add_argument("--start-date", default=None, help="Start date, YYYY-MM-DD (required)")
    parser.add_argument("--end-date", default=None, help="End date, YYYY-MM-DD (required)")
    parser.add_argument(
        "--out-dir", default=OUT_DIR, help="Output directory for days_YYYY_MM.json files"
    )
    parser.add_argument(
        "--between-pages-delay",
        type=float,
        default=BETWEEN_PAGES_DELAY,
        help="Seconds to sleep between month requests (default: %(default)s)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=RETRY_DELAY,
        help="Seconds to sleep before retrying a failed month request (default: %(default)s)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> ScrapeResult:
    args = parse_args(argv)

    # QUAL-04: dates are no longer defaulted to a hardcoded range; require them explicitly.
    if args.start_date is None or args.end_date is None:
        logger.error("--start-date and --end-date are required")
        sys.exit(1)

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
    logger.info(
        "Done. Success: %s, Failed/Empty: %s, Skipped: %s",
        result.success_count,
        result.fail_count,
        result.skip_count,
    )
    return result


if __name__ == "__main__":
    main()
