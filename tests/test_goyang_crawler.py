import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class GoyangCrawlerTests(unittest.TestCase):
    def test_gytennis_html_matches_requested_dash_date(self):
        from crawl_goyang import gytennis_html_matches_date

        html = '<input type="hidden" name="cdate" value="2026-07-09">'

        self.assertTrue(gytennis_html_matches_date(html, "2026-07-09"))
        self.assertFalse(gytennis_html_matches_date(html, "2026-07-08"))

    def test_gytennis_html_matches_requested_compact_date(self):
        from crawl_goyang import gytennis_html_matches_date

        html = '<input type="hidden" name="cdate" value="20260709">'

        self.assertTrue(gytennis_html_matches_date(html, "2026-07-09"))
        self.assertFalse(gytennis_html_matches_date(html, "2026-07-08"))

    def test_parse_gytennis_slots_uses_public_empty_slots_only(self):
        from crawl_goyang import parse_gytennis_slots

        html = """
        <table class="custom">
          <tr><td class="wide">06:00 ~ 08:00</td></tr>
          <tr><td class="wide">08:00 ~ 10:00</td></tr>
          <tr><td class="wide">10:00 ~ 12:00</td></tr>
        </table>
        <table class="innerCustom">
          <tr><td class="courtTag">1 코트</td></tr>
          <tr><td class="resTag"><div class="public-tooltip-trigger" data-kind="R">예약자</div></td></tr>
          <tr><td class="resTag"><span class="public-empty-slot">&nbsp;</span></td></tr>
          <tr><td class="resTag"><input type="checkbox" disabled></td></tr>
        </table>
        """

        self.assertEqual(
            parse_gytennis_slots(html),
            {
                "1": [
                    {
                        "timeContent": "08:00 ~ 10:00",
                        "slotKey": "08:00~10:00",
                        "courtNo": "1",
                    }
                ]
            },
        )
