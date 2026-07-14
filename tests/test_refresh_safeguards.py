import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class RefreshSafeguardTests(unittest.TestCase):
    def test_anseong_ggshare_city_is_not_collected_for_frontend(self):
        import refresh_and_notify

        payload = {
            "facilities": {"anseong-F0137-1230001": {"title": "고삼"}},
            "availability": {"anseong-F0137-1230001": {"20260712": []}},
        }
        with patch.dict(os.environ, {"RUN_TARGET": "anseong"}), patch.object(
            refresh_and_notify.crawl_ggshare, "crawl_ggshare", return_value=payload
        ) as crawl:
            facilities, availability = refresh_and_notify.crawl_all()

        crawl.assert_not_called()
        self.assertNotIn("anseong:F0137-1230001", facilities)
        self.assertNotIn("anseong:F0137-1230001", availability)

        from check_crawl_health import CITY_CONFIG
        self.assertNotIn("anseong", CITY_CONFIG)
        self.assertIn("uijeongbu", CITY_CONFIG)
        self.assertIn("yangpyeong", CITY_CONFIG)

    def test_hanam_partial_failure_is_excluded_from_cache_writes(self):
        import refresh_and_notify

        payload = {
            "facilities": {"misa-1": {"title": "미사"}},
            "availability": {"misa-1": {"20260712": [{"timeContent": "09:00"}]}},
            "partial_failure": True,
        }
        with patch.dict(os.environ, {"RUN_TARGET": "hanam"}), patch.object(
            refresh_and_notify.crawl_extra_cities, "crawl_hanam", return_value=payload
        ):
            refresh_and_notify.crawl_all()

        self.assertIn("hanam:", refresh_and_notify.LAST_FAILED_PREFIXES)

    def test_zero_slots_can_clear_cache_when_crawl_completed(self):
        from refresh_and_notify import should_protect_cache

        self.assertFalse(
            should_protect_cache(slot_count=0, partial_failure=False, protect_zero_slots=False)
        )

    def test_partial_failure_keeps_existing_cache_even_with_zero_slots(self):
        from refresh_and_notify import should_protect_cache

        self.assertTrue(
            should_protect_cache(slot_count=0, partial_failure=True, protect_zero_slots=False)
        )

    def test_successful_crawl_result_stamps_facility_updated_time(self):
        import refresh_and_notify

        result = {
            "facilities": {"rid": {"title": "court"}},
            "availability": {"rid": {"20260712": []}},
            "diagnostic": {
                "status": "normal",
                "last_success_at": "2026-07-12T12:34:56+00:00",
                "finished_at": "2026-07-12T12:35:00+00:00",
            },
        }

        stamped = refresh_and_notify._stamp_crawl_result(result)

        self.assertEqual(
            "2026-07-12T12:34:56+00:00",
            stamped["facilities"]["rid"]["_crawled_at"],
        )

    def test_frontend_facility_id_whitelist_blocks_unknown_and_anseong(self):
        import refresh_and_notify

        self.assertTrue(refresh_and_notify.is_frontend_facility_id("yongin:123"))
        self.assertTrue(refresh_and_notify.is_frontend_facility_id("uiwang:F0001"))
        self.assertFalse(refresh_and_notify.is_frontend_facility_id("anseong:F0137"))
        self.assertFalse(refresh_and_notify.is_frontend_facility_id("ggshare:anseong-F0137"))
        self.assertFalse(refresh_and_notify.is_frontend_facility_id("ggshare:uijeongbu-F0003-1130004"))
        self.assertFalse(refresh_and_notify.is_frontend_facility_id("unknown:123"))

    def test_sent_slot_key_separates_date_and_group(self):
        import refresh_and_notify

        morning = "09:00 ~ 10:00"
        self.assertNotEqual(
            refresh_and_notify.sent_slot_key("용인|A", "20260712", morning),
            refresh_and_notify.sent_slot_key("용인|A", "20260713", morning),
        )
        self.assertNotEqual(
            refresh_and_notify.sent_slot_key("용인|A", "20260712", morning),
            refresh_and_notify.sent_slot_key("용인|B", "20260712", morning),
        )


if __name__ == "__main__":
    unittest.main()
