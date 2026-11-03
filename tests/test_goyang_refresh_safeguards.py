import os
import sys
import unittest
import sqlite3
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

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
        self.assertEqual({'goyang:daehwa:'}, refresh.LAST_FAILED_PREFIXES)

    def test_all_failed_goyang_sources_stop_before_database_connection(self):
        self._mock_sources({name: {'facilities': {}, 'availability': {}, 'partial_failure': True}
                            for name in ('crawl_gytennis', 'crawl_daehwa', 'crawl_baekseok')})
        with patch.dict(os.environ, {"DATABASE_URL": "unused", "VAPID_PRIVATE_KEY": "unused", "VAPID_SUBJECT": "unused"}), patch.object(
            refresh.psycopg, "connect"
        ) as connect:
            with self.assertRaisesRegex(RuntimeError, "Goyang"):
                refresh.main()

        connect.assert_not_called()

    def test_failed_source_is_isolated_and_success_clears_failure_state(self):
        self._mock_sources({'crawl_daehwa': {'facilities': {}, 'availability': {}, 'partial_failure': True}})
        refresh.crawl_all()
        self.assertEqual({'goyang:daehwa:'}, refresh.LAST_FAILED_PREFIXES)
        self._mock_sources()
        refresh.crawl_all()
        self.assertFalse(refresh.crawl_goyang.LAST_PARTIAL_FAILURE)
        self.assertEqual(set(), refresh.LAST_FAILED_PREFIXES)

    def test_partial_source_does_not_block_healthy_cache_writes_or_publish_failed_slots(self):
        failed_source = {
            'facilities': {'gy-daehwa': {'title': '대화', '_court_numbers': [1]}},
            'availability': {'gy-daehwa': {'2026-12-31': [{'courtNo': '1', 'timeContent': '08:00 ~ 10:00'}]}},
            'partial_failure': True,
        }
        self._mock_sources({'crawl_daehwa': failed_source})
        conn = MagicMock()
        database = sqlite3.connect(':memory:')
        self.addCleanup(database.close)
        database.execute("attach database ':memory:' as public")
        database.execute('create table public.availability_cache (facility_id text, date_ymd text, slots_json text, updated_at text, checked_at text, query_status text, availability_status text)')
        cached_rows = [('goyang:daehwa:1', '2026-11-04'), ('goyang:daehwa:1', '2026-11-02'), ('goyang:gytennis:7:1', '2026-11-04')]
        database.executemany('insert into public.availability_cache values (?, ?, ?, ?, ?, ?, ?)',
                             [(fid, day, '[{"slot":"old"}]', 'old', 'last-success', 'success', 'available') for fid, day in cached_rows])
        class Cursor:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def execute(self, sql, params):
                sql = sql.replace('%s', '?').replace('now()', 'CURRENT_TIMESTAMP')
                database.execute(sql, tuple(value.isoformat() if hasattr(value, 'isoformat') else value for value in params))
        conn.cursor.side_effect = Cursor
        self.stack.enter_context(patch.object(refresh, 'kst_today_yyyymmdd', return_value='20261103'))
        self.stack.enter_context(patch.dict(os.environ, {'DATABASE_URL': 'unused', 'VAPID_PRIVATE_KEY': 'unused', 'VAPID_SUBJECT': 'unused'}))
        connect = self.stack.enter_context(patch.object(refresh.psycopg, 'connect'))
        connect.return_value.__enter__.return_value = conn
        for name in ('ensure_extra_schema', 'purge_blocked_frontend_prefixes', 'cleanup_tracking_tables',
                     'cleanup_expired_alarms', 'delete_expired_availability_cache'):
            self.stack.enter_context(patch.object(refresh, name, return_value=0))
        clear = self.stack.enter_context(patch.object(refresh, 'clear_availability_cache_for_target'))
        self.stack.enter_context(patch.object(refresh, 'upsert_facilities_for_frontend'))
        write = self.stack.enter_context(patch.object(refresh, 'upsert_availability_cache_for_frontend'))
        alarms = self.stack.enter_context(patch.object(refresh, 'load_alarms', return_value=[]))

        with self.assertRaisesRegex(RuntimeError, 'healthy sources saved'):
            refresh.main()

        conn.commit.assert_called_once()
        published = write.call_args.args[2]
        self.assertEqual({'goyang:gytennis:7:1', 'goyang:gytennis:7:2', 'goyang:baekseok:1'}, set(published))
        self.assertEqual([], published['goyang:gytennis:7:2']['20260923'])
        self.assertEqual([], published['goyang:baekseok:1']['20260923'])
        self.assertIn('goyang:daehwa:', clear.call_args.kwargs['exclude_prefixes'])
        self.assertEqual(('20260923', '20260924'), clear.call_args.args[2:4])
        alarms.assert_not_called()
        rows = {(fid, day): (slots, checked, query, availability) for fid, day, slots, checked, query, availability in database.execute(
            'select facility_id, date_ymd, slots_json, checked_at, query_status, availability_status from public.availability_cache')}
        self.assertEqual(('[{"slot":"old"}]', 'last-success', 'failed', 'unknown'), rows['goyang:daehwa:1', '2026-11-04'])
        self.assertEqual(('[{"slot":"old"}]', 'last-success', 'success', 'available'), rows['goyang:gytennis:7:1', '2026-11-04'])
        self.assertEqual(('[{"slot":"old"}]', 'last-success', 'success', 'available'), rows['goyang:daehwa:1', '2026-11-02'])

    def test_cache_clear_preserves_failed_child_source_but_clears_healthy_siblings(self):
        # Execute the actual WHERE clause locally; adapt only PostgreSQL syntax.
        database = sqlite3.connect(':memory:')
        self.addCleanup(database.close)
        database.execute("attach database ':memory:' as public")
        database.execute('create table public.availability_cache (facility_id text, date_ymd text, slots_json text, updated_at text)')
        ids = ['goyang:daehwa:1', 'goyang:gytennis:7:1', 'goyang:baekseok:6', 'yongin:1']
        database.executemany('insert into public.availability_cache values (?, ?, ?, ?)',
                             [(fid, '2026-11-04', '[{"slot":"old"}]', 'old') for fid in ids])

        class Cursor:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def execute(self, sql, params):
                sql = sql.replace('%s', '?').replace('::jsonb', '').replace('now()', 'CURRENT_TIMESTAMP')
                database.execute(sql, tuple(value.isoformat() if hasattr(value, 'isoformat') else value for value in params))
        conn = MagicMock()
        conn.cursor.side_effect = Cursor
        refresh.clear_availability_cache_for_target(conn, 'goyang', '20261104', '20261104',
                                                   exclude_prefixes=['goyang:daehwa:'], commit=False)
        rows = {fid: (slots, updated) for fid, slots, updated in database.execute(
            'select facility_id, slots_json, updated_at from public.availability_cache')}
        self.assertEqual(('[{"slot":"old"}]', 'old'), rows['goyang:daehwa:1'])
        self.assertEqual(('[{"slot":"old"}]', 'old'), rows['yongin:1'])
        self.assertEqual('[]', rows['goyang:gytennis:7:1'][0])
        self.assertEqual('[]', rows['goyang:baekseok:6'][0])
        conn.commit.assert_not_called()

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
