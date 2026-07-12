import unittest
from unittest.mock import Mock, patch

import requests

import crawl_seongnam


class SeongnamCrawlerTests(unittest.TestCase):
    @patch.object(crawl_seongnam, "_login", return_value=False)
    @patch.object(crawl_seongnam, "_session")
    def test_all_group_requests_rejected_is_authentication_required(self, session_factory, _login):
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


if __name__ == "__main__":
    unittest.main()
