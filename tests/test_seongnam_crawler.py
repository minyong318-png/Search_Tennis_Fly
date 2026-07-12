import json
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

import crawl_seongnam


class SeongnamCrawlerTests(unittest.TestCase):
    def test_apply_storage_state_copies_matching_cookies_to_requests_session(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as fp:
            json.dump(
                {
                    "cookies": [
                        {
                            "name": "JSESSIONID",
                            "value": "abc",
                            "domain": "res.isdc.co.kr",
                            "path": "/",
                        },
                        {
                            "name": "OTHER",
                            "value": "ignored",
                            "domain": "example.com",
                            "path": "/",
                        },
                    ]
                },
                fp,
            )
            state_path = fp.name

        session = requests.Session()

        try:
            loaded = crawl_seongnam._apply_storage_state(session, state_path)
        finally:
            crawl_seongnam._remove_file_quietly(state_path)

        self.assertTrue(loaded)
        self.assertEqual("abc", session.cookies.get("JSESSIONID"))
        self.assertIsNone(session.cookies.get("OTHER"))

    @patch.object(crawl_seongnam, "_run_playwright_login", return_value=True)
    @patch.object(crawl_seongnam, "_validate_logged_in_session", side_effect=[False, True])
    @patch.object(crawl_seongnam, "_apply_storage_state", side_effect=[True, True])
    def test_ensure_authenticated_session_refreshes_only_when_saved_session_is_invalid(
        self, apply_state, validate_session, run_login
    ):
        session = requests.Session()

        authenticated = crawl_seongnam._ensure_authenticated_session(session)

        self.assertTrue(authenticated)
        self.assertEqual(2, apply_state.call_count)
        self.assertEqual(2, validate_session.call_count)
        run_login.assert_called_once()

    @patch.object(crawl_seongnam, "_run_playwright_login")
    @patch.object(crawl_seongnam, "_validate_logged_in_session", return_value=True)
    @patch.object(crawl_seongnam, "_apply_storage_state", return_value=True)
    def test_ensure_authenticated_session_reuses_valid_saved_session(
        self, apply_state, validate_session, run_login
    ):
        session = requests.Session()

        authenticated = crawl_seongnam._ensure_authenticated_session(session)

        self.assertTrue(authenticated)
        apply_state.assert_called_once()
        validate_session.assert_called_once()
        run_login.assert_not_called()

    def test_login_link_on_facility_page_is_not_treated_as_login_challenge(self):
        html = '<a href="/login.do">login</a><script src="/NetFunnel.js"></script>'

        self.assertFalse(crawl_seongnam._has_login_challenge(html))

    @patch.dict("os.environ", {"ISDC_ID": "", "ISDC_PW": ""})
    @patch.object(crawl_seongnam, "_login", return_value=False)
    @patch.object(crawl_seongnam, "_run_playwright_login")
    @patch.object(crawl_seongnam, "_apply_storage_state", return_value=False)
    def test_ensure_authenticated_session_skips_browser_login_without_credentials(
        self, apply_state, run_login, legacy_login
    ):
        session = requests.Session()

        authenticated = crawl_seongnam._ensure_authenticated_session(session)

        self.assertFalse(authenticated)
        apply_state.assert_called_once()
        run_login.assert_not_called()
        legacy_login.assert_called_once()

    @patch.object(crawl_seongnam, "_ensure_authenticated_session", return_value=False)
    @patch.object(crawl_seongnam, "_session")
    def test_all_group_requests_rejected_is_authentication_required(self, session_factory, _auth):
        session = Mock()
        listing = Mock()
        listing.text = '<input name="groupId" value="1"><div class="head-area">탄천</div>'
        listing.raise_for_status.return_value = None
        rejected = Mock()
        rejected.raise_for_status.side_effect = requests.HTTPError("400 Client Error")
        session.get.return_value = listing
        session.post.return_value = rejected
        session_factory.return_value = session

        result = crawl_seongnam.crawl_seongnam()

        self.assertTrue(result["login_required"])
        self.assertEqual("all facility group requests were rejected", result["error_message"])

    @patch.object(crawl_seongnam, "_ensure_authenticated_session", return_value=False)
    @patch.object(crawl_seongnam, "_session")
    def test_automation_blocked_auth_error_is_preserved(self, session_factory, _auth):
        crawl_seongnam._last_auth_error_type = "AutomationBlocked"
        session = Mock()
        listing = Mock()
        listing.text = '<input name="groupId" value="1"><div class="head-area">탄천</div>'
        listing.raise_for_status.return_value = None
        rejected = Mock()
        rejected.raise_for_status.side_effect = requests.HTTPError("400 Client Error")
        session.get.return_value = listing
        session.post.return_value = rejected
        session_factory.return_value = session

        try:
            result = crawl_seongnam.crawl_seongnam()
        finally:
            crawl_seongnam._last_auth_error_type = ""

        self.assertTrue(result["automation_blocked"])
        self.assertEqual("AutomationBlocked", result["error_type"])


if __name__ == "__main__":
    unittest.main()
