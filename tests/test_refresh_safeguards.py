import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class RefreshSafeguardTests(unittest.TestCase):
    def test_registered_ggshare_city_target_is_executed_and_namespaced(self):
        import refresh_and_notify

        payload = {
            "facilities": {"anseong-F0137-1230001": {"title": "고삼"}},
            "availability": {"anseong-F0137-1230001": {"20260712": []}},
        }
        with patch.dict(os.environ, {"RUN_TARGET": "anseong"}), patch.object(
            refresh_and_notify.crawl_ggshare, "crawl_ggshare", return_value=payload
        ) as crawl:
            facilities, availability = refresh_and_notify.crawl_all()

        crawl.assert_called_once_with("anseong")
        self.assertIn("anseong:F0137-1230001", facilities)
        self.assertIn("anseong:F0137-1230001", availability)

        from check_crawl_health import CITY_CONFIG
        self.assertIn("anseong", CITY_CONFIG)
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


if __name__ == "__main__":
    unittest.main()
