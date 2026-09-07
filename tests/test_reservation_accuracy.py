import asyncio
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class ReservationAccuracyTests(unittest.TestCase):
    def test_canonical_reservation_types_survive_normalization(self):
        import tennis_core

        for reservation_type in ("district_priority", "city_priority", "general", "unknown"):
            with self.subTest(reservation_type=reservation_type):
                self.assertEqual(reservation_type, tennis_core.normalize_reservation_type(reservation_type))

    def test_source_waiting_status_and_canonical_not_open_are_not_open(self):
        import tennis_core

        for status in ("접수대기", "접수 대기", "not_open"):
            with self.subTest(status=status):
                self.assertEqual("not_open", tennis_core.normalize_application_status(status))

    def test_not_open_product_does_not_request_available_times(self):
        import tennis_core

        with patch.object(tennis_core, "fetch_times", new=AsyncMock(return_value=[])) as fetch_times:
            result = asyncio.run(tennis_core.fetch_availability(
                None,
                "14168",
                title="남사 테니스장_10월",
                facility_meta={"applicationStatus": "not_open", "applicationStatusLabel": "접수대기"},
                start_date=datetime(2026, 9, 7, tzinfo=timezone.utc),
            ))

        fetch_times.assert_not_awaited()
        self.assertEqual("not_open", result["_availability_status_by_date"]["20261001"])
        self.assertEqual([], result["20261001"])

    def test_parse_facility_html_preserves_reservation_type_and_application_status(self):
        import tennis_core

        html = """
        <li class="reserve_box_item">
          <div class="reserve_img"><span class="above_item">구민우선</span></div>
          <div class="reserve_con">
            <button class="reserve_state">접수마감</button>
            <div class="reserve_title">[유료] 아르피아 테니스장(1코트)_09월
              <div class="reserve_position">수지구, 죽전2동</div>
            </div>
            <ul class="bu">
              <li>접수기간 : 2026-08-19 ~ 2026-08-19</li>
              <li>이용기간 : 2026-09-01 ~ 2026-09-30</li>
            </ul>
            <div class="btn_wrap"><a href="/publicsports/sports/selectFcltyRceptResveViewU.do?resveId=13707">상세보기</a></div>
          </div>
        </li>
        """

        facility = tennis_core.parse_facility_html(html, "GNRLRESVE")["13707"]

        self.assertEqual("district_priority", facility["reservationType"])
        self.assertEqual("구민우선", facility["reservationTypeLabel"])
        self.assertEqual("closed", facility["applicationStatus"])
        self.assertEqual("접수마감", facility["applicationStatusLabel"])
        self.assertEqual("2026-08-19", facility["applicationStartDate"])
        self.assertEqual("2026-09-30", facility["useEndDate"])

    def test_closed_product_returns_checked_empty_dates_without_requesting_times(self):
        import tennis_core

        with patch.object(tennis_core, "fetch_times", new=AsyncMock()) as fetch_times:
            result = asyncio.run(
                tennis_core.fetch_availability(
                    None,
                    "13707",
                    title="[유료] 아르피아 테니스장(1코트)_09월",
                    facility_meta={
                        "applicationStatus": "closed",
                        "useStartDate": "2026-09-01",
                        "useEndDate": "2026-09-30",
                    },
                    start_date=datetime(2026, 9, 8, tzinfo=timezone.utc),
                )
            )

        self.assertFalse(fetch_times.await_args_list)
        self.assertTrue(result["20260908"] == [])
        self.assertEqual("closed", result["_availability_status_by_date"]["20260908"])
        self.assertIn("20260908", result["_checked_dates"])

    @patch("tennis_core._requests_session")
    def test_time_response_without_reservation_list_is_not_confirmed_empty(self, session_factory):
        import tennis_core

        response = MagicMock()
        response.json.return_value = {"message": "temporary error"}
        session_factory.return_value.post.return_value = response

        with self.assertRaises(ValueError):
            tennis_core.fetch_times_sync("20260908", "13707")

    def test_frontend_upsert_writes_confirmed_empty_and_status_metadata(self):
        import refresh_and_notify

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor

        refresh_and_notify.upsert_availability_cache_for_frontend(
            conn,
            {
                "yongin:13707": {
                    "title": "아르피아 테니스장(1코트)_09월",
                    "_crawled_at": "2026-09-07T02:00:00+00:00",
                    "_availability_status_by_date": {"20260908": "closed"},
                    "_failed_dates": [],
                }
            },
            {"yongin:13707": {"20260908": []}},
            commit=False,
        )

        rows = cursor.executemany.call_args.args[1]
        self.assertEqual(1, len(rows))
        self.assertEqual("yongin:13707", rows[0][0])
        self.assertEqual("[]", rows[0][2])
        self.assertEqual("success", rows[0][4])
        self.assertEqual("closed", rows[0][5])
        self.assertIn("availability_status", cursor.executemany.call_args.args[0])

    def test_slot_obj_filter_rejects_explicit_zero_or_unavailable_slots(self):
        import refresh_and_notify

        slots = [
            {"timeContent": "09:00 ~ 11:00", "remaining": 0},
            {"timeContent": "11:00 ~ 13:00", "available": False},
            {"timeContent": "13:00 ~ 15:00", "status": "예약마감"},
            {"timeContent": "15:00 ~ 17:00"},
        ]

        publishable = refresh_and_notify.filter_publishable_slots(slots)

        self.assertEqual(["15:00 ~ 17:00"], [slot["timeContent"] for slot in publishable])

    def test_failed_date_is_published_as_blocked_without_overwriting_last_success(self):
        import refresh_and_notify

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        refresh_and_notify.upsert_availability_cache_for_frontend(
            conn,
            {
                "yongin:13707": {
                    "title": "아르피아 테니스장(1코트)_09월",
                    "_failed_dates": ["20260908"],
                }
            },
            {},
            commit=False,
        )

        rows = cursor.executemany.call_args.args[1]
        self.assertEqual("failed", rows[0][4])
        self.assertEqual("unknown", rows[0][5])
        self.assertIsNone(rows[0][6])
        self.assertIn("do update set", cursor.executemany.call_args.args[0])
        self.assertIn("slots_json", cursor.executemany.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
