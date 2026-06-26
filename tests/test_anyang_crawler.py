import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class AnyangCrawlerTests(unittest.TestCase):
    def test_parse_anyang_slots_groups_enabled_checkboxes_by_court_and_reservation_hours(self):
        from crawl_anyang import parse_anyang_slots

        html = """
        <table class="custom">
          <tr><td class="wide">06<span class="courtTime">:00</span> ~ 07<span class="courtTime">:00</span></td></tr>
          <tr><td class="wide">07<span class="courtTime">:00</span> ~ 08<span class="courtTime">:00</span></td></tr>
          <tr><td class="wide">20<span class="courtTime">:00</span> ~ 21<span class="courtTime">:00</span></td></tr>
          <tr><td class="wide">21<span class="courtTime">:00</span> ~ 22<span class="courtTime">:00</span></td></tr>
        </table>
        <table class="innerCustom">
          <tr><td class="courtTag">1 <span>Court</span></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="1" value="slot-before" /></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="1" value="slot-a" /></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="1" value="slot-b" disabled /></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="1" value="slot-after" /></td></tr>
        </table>
        <table class="innerCustom">
          <tr><td class="courtTag">2 <span>Court</span></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="2" value="slot-c" disabled /></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="2" value="slot-d" disabled /></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="2" value="slot-e" /></td></tr>
          <tr><td class="resTag"><input type="checkbox" data-court="2" value="slot-f" /></td></tr>
        </table>
        """

        self.assertEqual(
            parse_anyang_slots(html),
            {
                "1": [
                    {
                        "timeContent": "07:00 ~ 08:00",
                        "slotKey": "07:00~08:00",
                        "courtNo": "1",
                    }
                ],
                "2": [
                    {
                        "timeContent": "20:00 ~ 21:00",
                        "slotKey": "20:00~21:00",
                        "courtNo": "2",
                    }
                ],
            },
        )

    def test_crawl_all_includes_anyang_target_with_namespaced_ids(self):
        import refresh_and_notify

        fake = {
            "facilities": {
                "aytennis-1": {"title": "Anyang Cityhall Court", "location": "Anyang"},
            },
            "availability": {
                "aytennis-1": {
                    "20260629": [{"timeContent": "07:00 ~ 08:00"}],
                }
            },
        }

        with patch.dict(os.environ, {"RUN_TARGET": "anyang"}, clear=False), patch(
            "crawl_anyang.crawl_anyang", return_value=fake
        ):
            facilities, availability = refresh_and_notify.crawl_all()

        self.assertEqual(facilities["anyang:aytennis-1"]["title"], "Anyang Cityhall Court")
        self.assertEqual(
            availability["anyang:aytennis-1"]["20260629"],
            [{"timeContent": "07:00 ~ 08:00"}],
        )


if __name__ == "__main__":
    unittest.main()
