import unittest
from unittest.mock import Mock, patch

import crawl_ggshare


class GgshareHealthTests(unittest.TestCase):
    @patch.object(crawl_ggshare, "_crawl_browser", side_effect=RuntimeError("credentials missing"))
    @patch.object(crawl_ggshare, "_session")
    def test_metadata_fallback_is_automation_blocked_not_no_reservations(self, session_factory, _browser):
        response = Mock()
        response.text = "facility"
        response.raise_for_status.return_value = None
        session_factory.return_value.get.return_value = response

        result = crawl_ggshare.crawl_ggshare("anseong")

        self.assertTrue(result["automation_blocked"])
        self.assertIn("credentials missing", result["error_message"])


if __name__ == "__main__":
    unittest.main()
