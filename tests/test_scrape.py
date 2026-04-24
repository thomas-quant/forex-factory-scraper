import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import scrape as scraper


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ScrapeTests(unittest.TestCase):
    def test_build_month_pages_matches_forex_factory_month_tokens(self):
        pages = scraper.build_month_pages(date(2026, 1, 15), date(2026, 3, 1))

        self.assertEqual([p.anchor for p in pages], [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)])
        self.assertEqual(pages[0].url, "https://www.forexfactory.com/calendar?month=jan.2026")
        self.assertEqual(pages[2].url, "https://www.forexfactory.com/calendar?month=mar.2026")

    def test_extract_days_from_calendar_component_states_selects_most_complete_state(self):
        sparse_days = [{"events": []}]
        rich_days = [
            {"events": [{"id": "nfp-1"}, {"id": "cpi-1"}]},
            {"events": [{"id": "fomc-1"}]},
        ]
        html = f"""
        <script>
          window.calendarComponentStates = {{
            "short": {{"days": {json.dumps(sparse_days)}}},
            "month": {{"days": {json.dumps(rich_days)}}}
          }};
        </script>
        """

        self.assertEqual(scraper.extract_days(html), rich_days)

    def test_extract_days_from_bracket_assignment_with_unquoted_days_key(self):
        days = [{"events": [{"id": "event-1"}]}]
        html = f"""<script>
        if (typeof window.calendarComponentStates === 'undefined') {{ window.calendarComponentStates = {{}} }}
        window.calendarComponentStates[1] = {{
          days: {json.dumps(days)},
          other: {{"ignored": true}}
        }};
        </script>"""

        self.assertEqual(scraper.extract_days(html), days)

    def test_extract_days_handles_single_quoted_scalar_state_fields(self):
        days = [{"events": [{"id": "event-1"}]}]
        html = f"""<script>
        window.calendarComponentStates[1] = {{
          days: {json.dumps(days)},
          time: '2:50am',
          upNext: 'apr24.2026',
          hideDefaultView: 0
        }};
        </script>"""

        self.assertEqual(scraper.extract_days(html), days)

    def test_extract_days_ignores_apostrophes_inside_days_strings(self):
        days = [{"events": [{"notice": "Source did not release Jan's data", "id": "event-1"}]}]
        html = f"""<script>
        window.calendarComponentStates[1] = {{
          days: {json.dumps(days)},
          time: '2:50am'
        }};
        </script>"""

        self.assertEqual(scraper.extract_days(html), days)

    def test_scrape_month_retries_then_returns_days(self):
        days = [{"events": [{"id": "event-1"}]}]
        html = f'<script>window.calendarComponentStates = {{"month": {{"days": {json.dumps(days)}}}}};</script>'
        session = FakeSession([RuntimeError("temporary"), FakeResponse(html)])
        page = scraper.MonthPage(date(2026, 4, 1), "https://example.test/calendar?month=apr.2026")

        with patch.object(scraper.time, "sleep"):
            result = scraper.scrape_month(session, page, max_attempts=2)

        self.assertEqual(result, days)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(session.calls[1][1]["impersonate"], scraper.IMPERSONATE)
        self.assertEqual(session.calls[1][1]["timeout"], scraper.REQUEST_TIMEOUT)


    def test_parse_args_defaults_between_pages_delay_to_zero(self):
        args = scraper.parse_args([])

        self.assertEqual(args.between_pages_delay, 0.0)
        self.assertEqual(args.retry_delay, 0.0)

    def test_main_passes_cli_delays_to_run_scraper(self):
        captured = {}

        def fake_run_scraper(
            pages,
            *,
            out_dir,
            session=None,
            between_pages_delay=0.0,
            retry_delay=0.0,
        ):
            captured["pages"] = pages
            captured["out_dir"] = out_dir
            captured["between_pages_delay"] = between_pages_delay
            captured["retry_delay"] = retry_delay
            return scraper.ScrapeResult(success_count=0, fail_count=0, skip_count=0)

        with patch.object(scraper, "run_scraper", side_effect=fake_run_scraper):
            result = scraper.main([
                "--start-date", "2026-03-01",
                "--end-date", "2026-03-31",
                "--out-dir", "out-test",
                "--between-pages-delay", "1.25",
                "--retry-delay", "0.5",
            ])

        self.assertEqual(result, scraper.ScrapeResult(success_count=0, fail_count=0, skip_count=0))
        self.assertEqual(captured["out_dir"], "out-test")
        self.assertEqual(captured["between_pages_delay"], 1.25)
        self.assertEqual(captured["retry_delay"], 0.5)
        self.assertEqual(len(captured["pages"]), 1)

    def test_run_scraper_passes_retry_delay_to_scrape_month(self):
        pages = [scraper.MonthPage(date(2026, 2, 1), "https://example.test/feb")]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(scraper, "scrape_month", return_value=[{"events": []}]) as mock_scrape_month:
                with patch.object(scraper.time, "sleep"):
                    scraper.run_scraper(pages, out_dir=tmpdir, session=object(), retry_delay=0.75)

        self.assertEqual(mock_scrape_month.call_args.kwargs["retry_delay"], 0.75)

    def test_run_scraper_skips_existing_files_and_writes_new_days(self):
        days = [{"events": [{"id": "event-1"}]}]
        html = f'<script>window.calendarComponentStates = {{"month": {{"days": {json.dumps(days)}}}}};</script>'
        session = FakeSession([FakeResponse(html)])

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            (out_dir / "days_2026_01.json").write_text("[]", encoding="utf-8")
            pages = [
                scraper.MonthPage(date(2026, 1, 1), "https://example.test/jan"),
                scraper.MonthPage(date(2026, 2, 1), "https://example.test/feb"),
            ]

            with patch.object(scraper.time, "sleep"):
                result = scraper.run_scraper(pages, out_dir=str(out_dir), session=session)

            self.assertEqual(result.success_count, 1)
            self.assertEqual(result.skip_count, 1)
            self.assertEqual(result.fail_count, 0)
            saved = json.loads((out_dir / "days_2026_02.json").read_text(encoding="utf-8"))
            self.assertEqual(saved, days)
            self.assertEqual([call[0] for call in session.calls], ["https://example.test/feb"])


if __name__ == "__main__":
    unittest.main()
