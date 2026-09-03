import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class GoyangCrawlerTests(unittest.TestCase):
    def test_gytennis_curl_ssl_error_retries_with_insecure_transport(self):
        import crawl_goyang

        class CurlCertificateError(Exception):
            pass

        class FakeResponse:
            encoding = "utf-8"
            status_code = 200
            url = "https://www.gytennis.or.kr/daily/1/2026-07-09"

            def __init__(self, text):
                self.text = text

        html = """
        <input type="hidden" name="cdate" value="2026-07-09">
        <form><input type="hidden" name="csrf" value="token"></form>
        <table class="custom"><tr><td class="wide">06:00 ~ 08:00</td></tr></table>
        <table class="innerCustom">
          <tr><td class="courtTag">1 코트</td></tr>
          <tr><td class="resTag"><span class="public-empty-slot"></span></td></tr>
        </table>
        """

        class FakeCurl:
            exceptions = SimpleNamespace(SSLError=CurlCertificateError)

            def get(self, _url, **kwargs):
                if kwargs.get("verify"):
                    raise CurlCertificateError("certificate verify failed")
                return FakeResponse(html)

            def post(self, _url, **kwargs):
                if kwargs.get("verify"):
                    raise CurlCertificateError("certificate verify failed")
                return FakeResponse(html)

        state = {}
        with patch.dict(os.environ, {"GYT_USE_CURL_CFFI": "1"}), patch.object(
            crawl_goyang, "curl_requests", FakeCurl()
        ):
            result = crawl_goyang.fetch_gytennis_day(
                crawl_goyang.make_gytennis_session(),
                1,
                "2026-07-09",
                state,
            )

        self.assertEqual(1, sum(len(slots) for slots in result.values()))
        self.assertTrue(state["use_insecure"])

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
