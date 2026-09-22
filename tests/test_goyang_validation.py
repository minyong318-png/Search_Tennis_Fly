import os
import unittest
from unittest.mock import Mock, patch

import requests

import crawl_goyang as c


def day_html(date="2026-09-23", available=True):
    cell = '<span class="public-empty-slot"></span>' if available else '<span>예약완료</span>'
    return f'''<input name="cdate" value="{date}">
    <table class="custom"><tr><td class="wide">08:00 ~ 10:00</td></tr></table>
    <table class="innerCustom"><tr><td class="courtTag">1 코트</td></tr>
    <tr><td class="resTag">{cell}</td></tr></table>'''


def response(html, status=200):
    result = requests.Response()
    result.status_code = status
    result.url = 'https://www.gytennis.or.kr/daily/7/2026-09-23'
    result.encoding = 'utf-8'
    result._content = html.encode('utf-8')
    return result


def gys_html(date='20260923', selected='6', available=True):
    disabled = '' if available else ' disabled'
    options = ''.join(f'<option value="{n}"' + (' selected' if n == selected else '') + f'>{n} 코트</option>' for n in ['6', '7'])
    return f'''<input name="rent_date" value="{date}">
    <select name="place_opt">{options}</select>
    <table summary="이용신청 테이블"><tr><td>08:00 ~ 10:00</td>
    <td><input name="rent_chk[]" value="08"{disabled}></td></tr></table>'''


class GoyangValidationTests(unittest.TestCase):
    def fetch(self, reply):
        session = Mock()
        session.get.return_value = reply
        session.post.return_value = reply
        with patch.dict(os.environ, {"GYT_USE_CURL_CFFI": "0"}):
            result = c.fetch_gytennis_day(session, 7, '2026-09-23', {})
        return result, session

    def test_rate_limit_is_failure_not_empty_availability(self):
        with self.assertRaises(requests.HTTPError):
            self.fetch(response('<h1>요청 수 초과 안내</h1>', 429))

    def test_wrong_date_is_failure_not_empty_availability(self):
        with self.assertRaises(ValueError):
            self.fetch(response(day_html('2026-09-22')))

    def test_missing_timetable_is_failure_not_empty_availability(self):
        with self.assertRaises(ValueError):
            self.fetch(response('<input name="cdate" value="2026-09-23">'))

    def test_truncated_timetable_is_rejected(self):
        html = day_html().replace('<tr><td class="resTag"><span class="public-empty-slot"></span></td></tr>', '')
        with self.assertRaises(ValueError):
            self.fetch(response(html))

    def test_valid_direct_page_needs_only_one_request(self):
        result, session = self.fetch(response(day_html()))
        self.assertEqual(result['1'][0]['slotKey'], '08:00~10:00')
        self.assertEqual(session.get.call_count, 1)
        session.post.assert_not_called()

    def test_valid_fully_booked_page_keeps_court_inventory(self):
        result, _ = self.fetch(response(day_html(available=False)))
        self.assertEqual(result, {'1': []})

    def test_all_fully_booked_dates_are_successfully_collected(self):
        with patch.object(c, 'build_date_range_kst', return_value=(False, ['2026-09-23'])), \
             patch.object(c, 'fetch_gytennis_day', return_value={'1': []}), \
             patch.object(c, 'warmup_gytennis_session'), \
             patch('time.sleep'):
            result = c.crawl_gytennis()
        self.assertFalse(result.get('partial_failure'))
        self.assertEqual(result['availability']['gy-gytennis-7'], {'2026-09-23': []})
        self.assertEqual(result['facilities']['gy-gytennis-7']['_court_numbers'], ['1'])

    def test_daehwa_failed_page_marks_crawl_partial(self):
        with patch.object(c, 'build_date_range_kst', return_value=(False, ['2026-09-23'])), \
             patch.object(c, 'login_daehwa'), \
             patch.object(c, 'post_rent', side_effect=TimeoutError('upstream timeout')):
            result = c.crawl_daehwa()
        self.assertTrue(result.get('partial_failure'))

    def test_baekseok_missing_transport_marks_failure(self):
        with patch.object(c, 'curl_requests', None):
            result = c.crawl_baekseok()
        self.assertTrue(result.get('partial_failure'))

    def test_daehwa_invalid_pages_are_failures(self):
        for html, status in [(gys_html(), 429), ('<h1>점검중</h1>', 200), (gys_html('20260922'), 200)]:
            with self.subTest(status=status, html=html), \
                 patch.object(c, 'build_date_range_kst', return_value=(False, ['2026-09-23'])), \
                 patch.object(c, 'login_daehwa'), \
                 patch.object(c, 'post_rent', return_value=(html, c.DAEHWA_RENT, status)):
                self.assertTrue(c.crawl_daehwa().get('partial_failure'))

    def test_daehwa_verified_empty_dates_keep_all_courts(self):
        with patch.object(c, 'build_date_range_kst', return_value=(False, ['2026-09-23'])), \
             patch.object(c, 'login_daehwa'), \
             patch.object(c, 'post_rent', return_value=(gys_html(available=False), c.DAEHWA_RENT, 200)):
            result = c.crawl_daehwa()
        self.assertFalse(result.get('partial_failure'))
        self.assertEqual(result['availability']['gy-daehwa'], {'2026-09-23': []})
        self.assertEqual(result['facilities']['gy-daehwa']['_court_numbers'], ['1', '2', '3', '4'])

    def test_baekseok_discovers_every_court_on_first_day_without_default_duplicate(self):
        def post(_session, payload):
            return response(gys_html(payload['rent_date'], selected=payload['place_opt'] or '6'))
        with patch.object(c, 'build_date_range_kst', return_value=(False, ['2026-09-23', '2026-09-24'])), \
             patch.object(c, 'login_baekseok'), \
             patch.object(c, 'curl_requests', Mock()), \
             patch.object(c, '_baekseok_post', side_effect=post) as requests_mock:
            result = c.crawl_baekseok()
        self.assertFalse(result.get('partial_failure'))
        self.assertEqual(requests_mock.call_count, 4)
        self.assertEqual(result['facilities']['gy-baekseok']['_court_numbers'], ['6', '7'])
        for day in result['availability']['gy-baekseok'].values():
            self.assertEqual([s['courtNo'] for s in day], ['6', '7'])

    def test_baekseok_invalid_response_marks_failure(self):
        with patch.object(c, 'build_date_range_kst', return_value=(False, ['2026-09-23'])), \
             patch.object(c, 'login_baekseok'), \
             patch.object(c, 'curl_requests', Mock()), \
             patch.object(c, '_baekseok_post', return_value=response('<h1>Unavailable</h1>', 503)):
            self.assertTrue(c.crawl_baekseok().get('partial_failure'))


if __name__ == '__main__':
    unittest.main()
