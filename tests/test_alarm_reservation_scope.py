import os
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import refresh_and_notify as refresh


class AlarmReservationScopeTests(unittest.TestCase):
    def setUp(self):
        self.today = patch.object(refresh, "kst_today_yyyymmdd", return_value="20260907")
        self.today.start()
        self.addCleanup(self.today.stop)
        self.slot = {"timeContent": "18:00 ~ 20:00"}

    def test_other_cities_ignore_yongin_product_status_and_use_period(self):
        metadata = [
            {"applicationStatus": "closed"},
            {"application_status_label": "접수대기"},
            {"useStartDate": "2026-10-01"},
            {"use_end_date": "2026-08-31"},
        ]
        for facility_id in ("suwon:1", "goyang:1", "seongnam:1", "ggshare:hanam-1"):
            for meta in metadata:
                with self.subTest(facility_id=facility_id, meta=meta):
                    self.assertTrue(refresh.alarm_slot_is_current(meta, "20260908", self.slot, facility_id))

    def test_yongin_and_legacy_numeric_ids_keep_product_guards(self):
        for facility_id in ("yongin:12345", "12345"):
            with self.subTest(facility_id=facility_id):
                self.assertTrue(refresh.alarm_slot_is_current({"applicationStatus": "open"}, "20260908", self.slot, facility_id))
            for meta in (
                {},
                {"applicationStatus": "closed"},
                {"application_status_label": "접수대기"},
                {"applicationStatus": "open", "useStartDate": "2026-10-01"},
                {"application_status": "open", "use_end_date": "2026-08-31"},
            ):
                with self.subTest(facility_id=facility_id, meta=meta):
                    self.assertFalse(refresh.alarm_slot_is_current(meta, "20260908", self.slot, facility_id))

    def test_all_cities_still_reject_unavailable_slots_and_failed_dates(self):
        for facility_id in ("yongin:12345", "12345", "suwon:1", "seongnam:1"):
            meta = {"applicationStatus": "open"}
            for slot in (
                {**self.slot, "available": False},
                {**self.slot, "remaining": 0},
                {**self.slot, "status": "예약마감"},
            ):
                with self.subTest(facility_id=facility_id, slot=slot):
                    self.assertFalse(refresh.alarm_slot_is_current(meta, "20260908", slot, facility_id))
            for blocked in (
                {"_failed_dates": ["2026-09-08"]},
                {"_availability_status_by_date": {"20260908": "confirmed_empty"}},
                {"_availability_status_by_date": {"20260908": "closed"}},
            ):
                with self.subTest(facility_id=facility_id, blocked=blocked):
                    self.assertFalse(refresh.alarm_slot_is_current({**meta, **blocked}, "20260908", self.slot, facility_id))
            self.assertFalse(refresh.alarm_slot_is_current(meta, "20260906", self.slot, facility_id))

    def test_started_time_is_rejected_for_all_cities(self):
        with patch.object(refresh, "datetime") as clock:
            clock.now.return_value = datetime(2026, 9, 7, 18, 0, tzinfo=refresh.KST)
            for facility_id in ("yongin:12345", "12345", "suwon:1"):
                with self.subTest(facility_id=facility_id):
                    self.assertFalse(refresh.alarm_slot_is_current({"applicationStatus": "open"}, "20260907", self.slot, facility_id))


if __name__ == "__main__":
    unittest.main()
