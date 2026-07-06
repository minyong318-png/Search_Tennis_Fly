import os
import sys
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

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

    def test_parse_anyang_slots_skips_today_slots_that_already_started(self):
        from crawl_anyang import parse_anyang_slots

        html = """
        <table class="custom">
          <tr><td class="wide">17<span class="courtTime">:00</span> ~ 18<span class="courtTime">:00</span></td></tr>
          <tr><td class="wide">18<span class="courtTime">:00</span> ~ 19<span class="courtTime">:00</span></td></tr>
        </table>
        <table class="innerCustom">
          <tr><td class="courtTag">1 <span>Court</span></td></tr>
          <tr><td class="resTag"><input type="checkbox" value="2026-07-06|1|1|17|3000" /></td></tr>
          <tr><td class="resTag"><input type="checkbox" value="2026-07-06|1|1|18|3000" /></td></tr>
        </table>
        """

        self.assertEqual(
            parse_anyang_slots(html, ymd="20260706", now=datetime(2026, 7, 6, 17, 30)),
            {
                "1": [
                    {
                        "timeContent": "18:00 ~ 19:00",
                        "slotKey": "18:00~19:00",
                        "courtNo": "1",
                    }
                ],
            },
        )

    def test_anyang_default_worker_count_is_conservative_to_avoid_429(self):
        import crawl_anyang

        with patch.dict(os.environ, {}, clear=True):
            self.assertLessEqual(crawl_anyang._max_workers(), 1)

    def test_fetch_day_does_not_repeat_warmup_for_each_date(self):
        import crawl_anyang

        response = Mock()
        response.encoding = "utf-8"
        response.text = "<html></html>"
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.return_value = response

        with patch.object(crawl_anyang, "_session", return_value=session), patch.object(
            crawl_anyang, "_warmup"
        ) as warmup:
            crawl_anyang._fetch_day(1, "2026-07-06")

        warmup.assert_not_called()

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

    def test_crawl_anyang_slots_link_to_facility_page_not_date_page(self):
        import crawl_anyang

        response = Mock()
        response.encoding = "utf-8"
        response.text = "<html></html>"
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.return_value = response

        with patch.object(crawl_anyang, "_session", return_value=session), patch.object(
            crawl_anyang, "_warmup"
        ), patch.object(crawl_anyang, "_extract_calendar_dates", return_value=["2026-06-29"]), patch.object(
            crawl_anyang, "_court_numbers_from_html", return_value=["1"]
        ), patch.object(
            crawl_anyang,
            "_fetch_day",
            return_value=(
                1,
                "20260629",
                {"1": [{"timeContent": "07:00 ~ 08:00", "slotKey": "07:00~08:00", "courtNo": "1"}]},
            ),
        ):
            out = crawl_anyang.crawl_anyang()

        slot = out["availability"]["aytennis-1-1"]["20260629"][0]
        self.assertEqual(slot["reserveUrl"], "https://www.aytennis.or.kr/daily/1")


if __name__ == "__main__":
    unittest.main()
