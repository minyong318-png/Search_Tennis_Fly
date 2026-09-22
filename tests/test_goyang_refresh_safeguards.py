import os
import sys
import unittest
from contextlib import ExitStack
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import refresh_and_notify as refresh


class GoyangRefreshSafeguardTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {"RUN_TARGET": "goyang"}))
        self.stack.enter_context(patch.object(refresh, "is_goyang_crawl_window", return_value=True))
        self.stack.enter_context(patch.object(refresh, "set_crawl_exit_node"))
        self.stack.enter_context(patch.object(refresh.crawl_goyang, "LAST_PARTIAL_FAILURE", False, create=True))

    def _mock_sources(self, overrides=None):
        payloads = {
            "crawl_gytennis": {
                "facilities": {"gy-gytennis-7": {"title": "충장", "_court_numbers": [1, 2]}},
                "availability": {"gy-gytennis-7": {"2026-09-23": [], "2026-09-24": []}},
            },
            "crawl_daehwa": {
                "facilities": {"gy-daehwa": {"title": "대화", "_court_numbers": [1, 2, 3, 4]}},
                "availability": {"gy-daehwa": {"2026-09-23": []}},
            },
            "crawl_baekseok": {
                "facilities": {"gy-baekseok": {"title": "백석", "_court_numbers": [1]}},
                "availability": {"gy-baekseok": {"2026-09-23": []}},
            },
        }
        payloads.update(overrides or {})
        for name, result in payloads.items():
            self.stack.enter_context(patch.object(refresh.crawl_goyang, name, return_value=result))

    def test_split_preserves_verified_empty_dates_and_courts(self):
        daymap = {
            "20260923": [{"courtNo": "1", "timeContent": "09:00 ~ 11:00"}],
            "20260924": [],
        }

        split = refresh._split_daymap_by_court(
            daymap, lambda slot, ymd: f"https://example.test/{ymd}", court_numbers=[1, 2]
        )

        self.assertEqual([], split["1"]["20260924"])
        self.assertEqual({"20260923": [], "20260924": []}, split["2"])
        self.assertEqual("https://example.test/20260923", split["1"]["20260923"][0]["reserveUrl"])
        self.assertNotIn("reserveUrl", daymap["20260923"][0])

    def test_split_without_inventory_keeps_legacy_behavior(self):
        split = refresh._split_daymap_by_court(
            {"20260923": [{"courtNo": "1", "timeContent": "09:00"}], "20260924": []},
            lambda slot, ymd: None,
        )

        self.assertEqual({"1": {"20260923": [{"courtNo": "1", "timeContent": "09:00"}]}}, split)

    def test_successful_empty_sources_keep_inventory_and_dates(self):
        self._mock_sources()

        facilities, availability = refresh.crawl_all()

        self.assertFalse(refresh.crawl_goyang.LAST_PARTIAL_FAILURE)
        self.assertEqual(7, len(facilities))
        self.assertEqual({"20260923": [], "20260924": []}, availability["goyang:gytennis:7:2"])
        self.assertEqual({"20260923": []}, availability["goyang:daehwa:4"])
        self.assertEqual({"20260923": []}, availability["goyang:baekseok:1"])

    def test_invalid_source_without_failure_flag_protects_goyang(self):
        self._mock_sources({"crawl_daehwa": {"facilities": {}, "availability": {}}})

        refresh.crawl_all()

        self.assertTrue(refresh.crawl_goyang.LAST_PARTIAL_FAILURE)

    def test_partial_goyang_run_fails_before_database_connection(self):
        self._mock_sources({"crawl_daehwa": {"facilities": {}, "availability": {}, "partial_failure": True}})
        with patch.dict(os.environ, {"DATABASE_URL": "unused", "VAPID_PRIVATE_KEY": "unused", "VAPID_SUBJECT": "unused"}), patch.object(
            refresh.psycopg, "connect"
        ) as connect:
            with self.assertRaisesRegex(RuntimeError, "Goyang"):
                refresh.main()

        connect.assert_not_called()

    def test_outside_window_is_an_intentional_skip_without_database_work(self):
        with patch.object(refresh, "is_goyang_crawl_window", return_value=False), patch.object(
            refresh, "crawl_all"
        ) as crawl, patch.object(refresh.psycopg, "connect") as connect, patch.dict(os.environ, {}, clear=True):
            os.environ["RUN_TARGET"] = "goyang"
            refresh.main()

        crawl.assert_not_called()
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
