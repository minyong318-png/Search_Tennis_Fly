import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class PajuCrawlerTests(unittest.TestCase):
    def test_parse_paju_slots_groups_enabled_checkboxes_by_court_and_reservation_hours(self):
        from crawl_paju import parse_paju_slots

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
        """

        self.assertEqual(
            parse_paju_slots(html),
            {
                "1": [
                    {
                        "timeContent": "07:00 ~ 08:00",
                        "slotKey": "07:00~08:00",
                        "courtNo": "1",
                    }
                ],
            },
        )

    def test_crawl_all_includes_paju_target_with_namespaced_ids(self):
        import refresh_and_notify

        fake = {
            "facilities": {
                "pjtennis-1": {"title": "Paju Court", "location": "Paju"},
            },
            "availability": {
                "pjtennis-1": {
                    "20260629": [{"timeContent": "07:00 ~ 08:00"}],
                }
            },
        }

        with patch.dict(os.environ, {"RUN_TARGET": "paju"}, clear=False), patch(
            "crawl_paju.crawl_paju", return_value=fake
        ):
            facilities, availability = refresh_and_notify.crawl_all()

        self.assertEqual(facilities["paju:pjtennis-1"]["title"], "Paju Court")
        self.assertEqual(
            availability["paju:pjtennis-1"]["20260629"],
            [{"timeContent": "07:00 ~ 08:00"}],
        )

    def test_crawl_paju_slots_link_to_facility_page_not_date_page(self):
        import crawl_paju

        response = Mock()
        response.encoding = "utf-8"
        response.text = "<html></html>"
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.return_value = response

        with patch.object(crawl_paju, "_session", return_value=session), patch.object(
            crawl_paju, "_warmup"
        ), patch.object(crawl_paju, "_extract_calendar_dates", return_value=["2026-06-29"]), patch.object(
            crawl_paju, "_court_numbers_from_html", return_value=["1"]
        ), patch.object(
            crawl_paju,
            "_fetch_day",
            return_value=(
                1,
                "20260629",
                {"1": [{"timeContent": "07:00 ~ 08:00", "slotKey": "07:00~08:00", "courtNo": "1"}]},
            ),
        ):
            out = crawl_paju.crawl_paju()

        slot = out["availability"]["pjtennis-1-1"]["20260629"][0]
        self.assertEqual(slot["reserveUrl"], "https://www.pjtennis.or.kr/daily/1")


if __name__ == "__main__":
    unittest.main()
