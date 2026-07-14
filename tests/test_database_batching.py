import unittest
from unittest.mock import MagicMock

import refresh_and_notify


class DatabaseBatchingTests(unittest.TestCase):
    def test_expired_cache_cleanup_joins_outer_transaction(self):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 12
        conn.cursor.return_value.__enter__.return_value = cursor

        deleted = refresh_and_notify.delete_expired_availability_cache(conn, commit=False)

        self.assertEqual(12, deleted)
        self.assertIn("date_ymd <", cursor.execute.call_args.args[0])
        conn.commit.assert_not_called()

    def test_availability_upsert_can_join_outer_transaction(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor

        refresh_and_notify.upsert_availability_cache_for_frontend(
            conn,
            {"suwon:court": {"title": "Court"}},
            {"suwon:court": {"20260712": [{"timeContent": "09:00 ~ 10:00"}]}},
            commit=False,
        )

        cursor.executemany.assert_called_once()
        conn.commit.assert_not_called()

    def test_tracking_cleanup_can_join_outer_transaction(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = ("public.some_table",)
        cursor.rowcount = 3

        deleted = refresh_and_notify.cleanup_tracking_tables(conn, commit=False)

        self.assertEqual(
            {"slots_snapshot": 3, "baseline_slots": 3, "sent_slots": 3},
            deleted,
        )
        executed_sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("slots_snapshot", executed_sql)
        self.assertIn("baseline_slots", executed_sql)
        self.assertIn("sent_slots", executed_sql)
        conn.commit.assert_not_called()

    def test_prefix_prune_removes_stale_snapshots_too(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = ("public.slots_snapshot",)
        cursor.rowcount = 2

        deleted = refresh_and_notify.prune_stale_prefix_facilities(
            conn,
            "hanam:",
            ["hanam:current"],
            commit=False,
        )

        self.assertEqual((2, 2, 2), deleted)
        executed_sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("availability_cache", executed_sql)
        self.assertIn("slots_snapshot", executed_sql)
        self.assertIn("facilities", executed_sql)
        conn.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
