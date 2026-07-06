import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class RefreshSafeguardTests(unittest.TestCase):
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
