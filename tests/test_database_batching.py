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


if __name__ == "__main__":
    unittest.main()
