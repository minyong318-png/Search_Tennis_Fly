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

    def test_android_collector_result_is_normalized_to_existing_schema(self):
        payload = {
            "status": "ok",
            "facilities": [
                {"id": "FAC001", "title": "탄천 1번 코트", "reserveUrl": crawl_seongnam.LIST_URL}
            ],
            "slots": [
                {
                    "facilityId": "FAC001",
                    "date": "20260712",
                    "timeContent": "09:00 ~ 10:00",
                    "remaining": "2",
                    "reserveUrl": crawl_seongnam.LIST_URL,
                }
            ],
        }

        result = crawl_seongnam._normalize_android_result(payload)

        self.assertIn("FAC001", result["facilities"])
        self.assertEqual("탄천 1번 코트", result["facilities"]["FAC001"]["title"])
        self.assertEqual("09:00 ~ 10:00", result["availability"]["FAC001"]["20260712"][0]["timeContent"])
        self.assertEqual("2", result["availability"]["FAC001"]["20260712"][0]["remaining"])

    @patch.object(crawl_seongnam, "_run_android_collector", return_value={"status": "chrome_tab_not_found", "message": "missing"})
    def test_android_mode_failure_preserves_cache(self, _collector):
        result = crawl_seongnam.crawl_seongnam("android")

        self.assertEqual({}, result["facilities"])
        self.assertEqual({}, result["availability"])
        self.assertEqual("ChromeTabNotFound", result["error_type"])
        self.assertTrue(result["partial_failure"])

    def test_android_zero_slots_is_not_treated_as_successful_no_reservations(self):
        payload = {
            "status": "ok",
            "facilities": [{"id": "FAC001", "title": "탄천"}],
            "slots": [],
        }

        result = crawl_seongnam._normalize_android_result(payload)

        self.assertTrue(result["partial_failure"])
        self.assertEqual("network_capture_failed", result["android_status"])

    def test_android_partial_failure_status_preserves_cache(self):
        payload = {
            "status": "partial_failure",
            "message": "Android Chrome page fetch stopped",
        }

        result = crawl_seongnam._normalize_android_result(payload)

        self.assertEqual({}, result["facilities"])
        self.assertEqual({}, result["availability"])
        self.assertTrue(result["partial_failure"])
        self.assertEqual("PartialFailure", result["error_type"])

    def test_android_unavailable_dates_are_successful_empty_results(self):
        payload = {
            "status": "ok",
            "facilities": [{"id": "FAC001", "title": "court"}],
            "slots": [],
            "unavailable_dates": [{"facilityId": "FAC001", "date": "20260712"}],
        }

        result = crawl_seongnam._normalize_android_result(payload)

        self.assertNotIn("partial_failure", result)
        self.assertEqual([], result["availability"]["FAC001"]["20260712"])

    @patch.dict("os.environ", {"SEONGNAM_ANDROID_TIMEOUT_MS": "600000"}, clear=False)
    def test_android_collector_timeout_tracks_android_deadline(self):
        self.assertGreaterEqual(crawl_seongnam._android_collector_timeout(), 660)

    @patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}, clear=False)
    @patch.object(crawl_seongnam, "_ensure_authenticated_session")
    def test_github_actions_auto_mode_does_not_touch_seongnam_site(self, auth):
        result = crawl_seongnam.crawl_seongnam("auto")

        auth.assert_not_called()
        self.assertTrue(result["partial_failure"])
        self.assertEqual("AutomationBlocked", result["error_type"])

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

        result = crawl_seongnam.crawl_seongnam("playwright")

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
        session.get.return_value = listing
        session_factory.return_value = session

        try:
            result = crawl_seongnam.crawl_seongnam("playwright")
        finally:
            crawl_seongnam._last_auth_error_type = ""

        self.assertTrue(result["automation_blocked"])
        self.assertEqual("AutomationBlocked", result["error_type"])
        session.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
