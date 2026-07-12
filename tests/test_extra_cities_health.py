import unittest
from datetime import date
from unittest.mock import Mock, patch

import crawl_extra_cities


class ExtraCitiesHealthTests(unittest.TestCase):
    @patch.object(crawl_extra_cities, "_session")
    def test_hanam_misa_retries_transient_404_once(self, session_factory):
        session = Mock()
        missing = Mock(status_code=404)
        missing.raise_for_status.side_effect = crawl_extra_cities.requests.HTTPError("404")
        ok = Mock(status_code=200, text='<table id="dynamicTbody"></table>')
        ok.raise_for_status.return_value = None
        session.get.side_effect = [missing, ok]
        session_factory.return_value = session

        crawl_extra_cities._fetch_hanam_day("TS02", "2", date(2026, 8, 5))

        self.assertEqual(2, session.get.call_count)

    @patch.object(crawl_extra_cities, "_session")
    def test_hanam_sports_uses_current_site_open_day(self, session_factory):
        session = Mock()
        session.get.return_value.raise_for_status.return_value = None
        session.get.return_value.text = '<input id="Rent_Open_Start_Day" value="15">'
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"rstate": "0", "play_name": "[]"}
        session.post.return_value = response
        session_factory.return_value = session

        crawl_extra_cities._fetch_hanam_sports_day("024", date(2026, 7, 12))

        self.assertEqual("15", session.post.call_args.kwargs["data"]["rent_open_start_day"])

    @patch.object(crawl_extra_cities, "_date_values", return_value=[date(2026, 7, 12)])
    @patch.object(crawl_extra_cities, "_fetch_hanam_day", return_value=("TS01", {}))
    @patch.object(crawl_extra_cities, "_fetch_hanam_sports_day", side_effect=RuntimeError("invalid access"))
    def test_hanam_reports_partial_failure_when_sports_source_is_rejected(
        self, _sports, _misa, _dates
    ):
        result = crawl_extra_cities.crawl_hanam()

        self.assertTrue(result["partial_failure"])
        self.assertIn("sports", result["error_message"])


if __name__ == "__main__":
    unittest.main()
