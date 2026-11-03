import os
import sys
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import refresh_and_notify as refresh


class GoyangSourceIsolationTests(unittest.TestCase):
    def test_all_target_publishes_healthy_source_and_excludes_failed_source_from_alarms(self):
        healthy_id = "goyang:gytennis:7:1"
        failed_id = "goyang:daehwa:1"
        ymd = "20261104"
        sid = "subscription-1"
        healthy_slot = {"timeContent": "09:00 ~ 11:00", "courtNo": "1"}
        failed_slot = {"timeContent": "13:00 ~ 15:00", "courtNo": "1"}
        facilities = {
            healthy_id: {"title": "충장 1코트"},
            failed_id: {"title": "대화 1코트"},
        }
        availability = {
            healthy_id: {ymd: [healthy_slot]},
            failed_id: {ymd: [failed_slot]},
        }
        healthy_group = refresh.get_court_group(facilities[healthy_id]["title"], healthy_id)
        failed_group = refresh.get_court_group(facilities[failed_id]["title"], failed_id)
        healthy_key = refresh.slot_key_from_time(healthy_slot)
        failed_key = refresh.slot_key_from_time(failed_slot)
        alarms = [
            {"subscription_id": sid, "court_group": group, "date": ymd}
            for group in (healthy_group, failed_group)
        ]
        baselines = {
            (sid, healthy_group, ymd): {healthy_key},
            # This existing baseline makes the failed source's apparent opening
            # eligible for a push if its source exclusion is ever removed.
            (sid, failed_group, ymd): {"07:00 ~ 09:00|1"},
        }
        conn = MagicMock()

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "RUN_TARGET": "all",
                "DATABASE_URL": "unused",
                "VAPID_PRIVATE_KEY": "unused",
                "VAPID_SUBJECT": "unused",
            }))
            stack.enter_context(patch.object(refresh, "LAST_FAILED_PREFIXES", {"goyang:daehwa:"}))
            stack.enter_context(patch.object(refresh, "YONGIN_FACILITY_LIST_PARTIAL_FAILURE", False))
            stack.enter_context(patch.object(refresh.crawl_goyang, "LAST_PARTIAL_FAILURE", True, create=True))
            stack.enter_context(patch.object(refresh.crawl_anyang, "LAST_PARTIAL_FAILURE", False, create=True))
            stack.enter_context(patch.object(refresh.crawl_paju, "LAST_PARTIAL_FAILURE", False, create=True))
            crawl = stack.enter_context(patch.object(refresh, "crawl_all", return_value=(facilities, availability)))
            connect = stack.enter_context(patch.object(refresh.psycopg, "connect"))
            connect.return_value.__enter__.return_value = conn
            for name in (
                "ensure_extra_schema", "purge_blocked_frontend_prefixes", "cleanup_tracking_tables",
                "cleanup_expired_alarms", "delete_expired_availability_cache",
                "prune_stale_yongin_facilities", "prune_stale_prefix_facilities",
                "bulk_upsert_slots_snapshot", "bulk_mark_sent",
            ):
                stack.enter_context(patch.object(refresh, name, return_value=0))
            clear = stack.enter_context(patch.object(refresh, "clear_availability_cache_for_target"))
            mark_failed = stack.enter_context(patch.object(refresh, "mark_failed_goyang_sources"))
            write_facilities = stack.enter_context(patch.object(refresh, "upsert_facilities_for_frontend"))
            write_slots = stack.enter_context(patch.object(refresh, "upsert_availability_cache_for_frontend"))
            load_alarms = stack.enter_context(patch.object(refresh, "load_alarms", return_value=alarms))
            stack.enter_context(patch.object(refresh, "load_push_subscriptions", return_value={
                sid: {"endpoint": "https://example.test/push"},
            }))
            stack.enter_context(patch.object(refresh, "preload_baseline", return_value=baselines))
            preload_sent = stack.enter_context(patch.object(refresh, "preload_sent_slots", return_value={sid: set()}))
            # A freshness rejection must not hide a missing failed-source guard.
            current = stack.enter_context(patch.object(refresh, "alarm_slot_is_current", return_value=True))
            send_push = stack.enter_context(patch.object(refresh, "send_push"))

            refresh.main()

        crawl.assert_called_once_with()
        conn.commit.assert_called_once_with()
        conn.cursor.assert_not_called()
        self.assertEqual({healthy_id: facilities[healthy_id]}, write_facilities.call_args.args[1])
        self.assertEqual({healthy_id: availability[healthy_id]}, write_slots.call_args.args[2])
        self.assertIn("goyang:daehwa:", clear.call_args.kwargs["exclude_prefixes"])
        self.assertNotIn("goyang:", clear.call_args.kwargs["exclude_prefixes"])
        mark_failed.assert_called_once_with(conn, ("goyang:daehwa:",))
        load_alarms.assert_called_once_with(conn)
        preload_sent.assert_called_once_with(conn, [sid], [refresh.sent_slot_key(healthy_group, ymd, healthy_key)])
        self.assertNotIn(refresh.sent_slot_key(failed_group, ymd, failed_key), preload_sent.call_args.args[2])
        current.assert_called_once_with(facilities[healthy_id], ymd, healthy_slot, healthy_id)
        send_push.assert_not_called()


if __name__ == "__main__":
    unittest.main()
