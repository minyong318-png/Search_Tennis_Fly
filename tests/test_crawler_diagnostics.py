import json
import tempfile
import unittest
from pathlib import Path

from crawler_diagnostics import run_crawler


class CrawlerDiagnosticsTests(unittest.TestCase):
    def test_records_successful_slot_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "diagnostics.jsonl"
            result = run_crawler(
                "sample",
                "sample-crawler",
                "https://example.test/reserve",
                lambda: {
                    "facilities": {"court-1": {"title": "Sample Court"}},
                    "availability": {
                        "court-1": {
                            "20260712": [
                                {
                                    "timeContent": "09:00 ~ 10:00",
                                    "remaining": 2,
                                    "reserveUrl": "https://example.test/reserve",
                                }
                            ]
                        }
                    },
                },
                diagnostics_path=path,
            )

            self.assertEqual("normal", result["diagnostic"]["status"])
            self.assertEqual(1, result["diagnostic"]["discovered_courts"])
            self.assertEqual(1, result["diagnostic"]["discovered_slots"])
            self.assertEqual(1, result["diagnostic"]["available_slots"])
            self.assertEqual(1, result["diagnostic"]["requested_dates"])
            self.assertIsNotNone(result["diagnostic"]["last_success_at"])
            saved = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual("sample", saved["region"])

    def test_distinguishes_no_reservations_from_parse_failure(self):
        empty = run_crawler(
            "empty",
            "empty-crawler",
            "https://example.test",
            lambda: {
                "facilities": {"court-1": {"title": "Court"}},
                "availability": {"court-1": {"20260712": []}},
            },
        )
        failed = run_crawler(
            "failed",
            "failed-crawler",
            "https://example.test",
            lambda: {"facilities": {}, "availability": {}},
        )

        self.assertEqual("no_reservations", empty["diagnostic"]["status"])
        self.assertEqual("parse_failure", failed["diagnostic"]["status"])

    def test_exception_is_isolated_and_recorded(self):
        def broken():
            raise TimeoutError("upstream timed out")

        result = run_crawler("broken", "broken-crawler", "https://example.test", broken)

        self.assertEqual({}, result["facilities"])
        self.assertEqual({}, result["availability"])
        self.assertTrue(result["partial_failure"])
        self.assertEqual("connection_failure", result["diagnostic"]["status"])
        self.assertEqual("TimeoutError", result["diagnostic"]["error_type"])

    def test_partial_and_authentication_failures_are_not_reported_normal(self):
        partial = run_crawler(
            "partial",
            "partial-crawler",
            "https://example.test",
            lambda: {
                "facilities": {"court-1": {"title": "Court"}},
                "availability": {"court-1": {"20260712": [{"timeContent": "10:00"}]}},
                "partial_failure": True,
                "error_message": "one source rejected requests",
            },
        )
        auth = run_crawler(
            "auth",
            "auth-crawler",
            "https://example.test",
            lambda: {"facilities": {}, "availability": {}, "login_required": True},
        )

        self.assertEqual("partial_failure", partial["diagnostic"]["status"])
        self.assertEqual("authentication_required", auth["diagnostic"]["status"])

    def test_automation_blocked_takes_precedence_over_login_required(self):
        result = run_crawler(
            "seongnam",
            "isdc",
            "https://res.isdc.co.kr",
            lambda: {
                "facilities": {},
                "availability": {},
                "login_required": True,
                "automation_blocked": True,
                "error_type": "AutomationBlocked",
            },
        )

        self.assertEqual("automation_blocked", result["diagnostic"]["status"])
        self.assertEqual("AutomationBlocked", result["diagnostic"]["error_type"])


if __name__ == "__main__":
    unittest.main()
