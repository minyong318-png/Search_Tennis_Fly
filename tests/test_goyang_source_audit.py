import os
import sys
import unittest
from collections import Counter

from lxml import html

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scripts import audit_goyang_refresh as audit


def source_page(place="2", summary="이용신청 테이블", notice="", timetable=True, calendar=True):
    content = f'''<input name="rent_date" value="20260924">
    <select name="place_opt"><option value="" selected>:: 장소 선택 ::</option>
    <option value="{place}" selected>테니스1</option><option value="14">(TEST)점검코트</option></select>
    <p>{notice}</p>'''
    if calendar:
        content += '<table summary="행사 및 대관일정표입니다."></table>'
    if timetable:
        content += f'''<table summary="{summary}">
        <tr><td>08:00 ~ 10:00<input name="rent_chk[]" value="slot1"></td></tr>
        <tr><td>10:00 ~ 12:00<input name="rent_chk[]" value="slot2" disabled></td></tr>
        </table>'''
    return content


class GoyangSourceAuditTests(unittest.TestCase):
    def test_actual_chuseok_notice_is_verified_empty(self):
        doc = html.fromstring(source_page(notice="선택하신 날짜는 추석연휴 입니다.", timetable=False))

        result = audit.parse_gys(doc, "daehwa", "2026-09-24", "2")

        self.assertEqual({("goyang:daehwa:1", "2026-09-24"): Counter()}, result)

    def test_holiday_notice_requires_calendar_selected_court_and_matching_date(self):
        page = source_page(notice="선택하신 날짜는 추석연휴 입니다.", timetable=False)
        variants = [
            page.replace('summary="행사 및 대관일정표입니다."', 'summary="unrelated"'),
            page.replace('value="2" selected', 'value="2"'),
            page.replace('value="20260924"', 'value="20260923"'),
        ]
        for variant in variants:
            with self.subTest(page=variant):
                with self.assertRaises(ValueError):
                    audit.parse_gys(html.fromstring(variant), "daehwa", "2026-09-24", "2")

    def test_unexplained_missing_timetable_is_still_failure(self):
        doc = html.fromstring(source_page(notice="휴관일을 확인하세요.", timetable=False))
        with self.assertRaises(ValueError):
            audit.parse_gys(doc, "daehwa", "2026-09-24", "2")

    def test_baekseok_alternate_exact_table_summary(self):
        doc = html.fromstring(source_page(place="6", summary="대관신청 테이블"))

        result = audit.parse_gys(doc, "baekseok", "2026-09-24", "6")

        self.assertEqual({("goyang:baekseok:6", "2026-09-24"): Counter({"08:00~10:00": 1})}, result)

    def test_auth_failure_then_retry_success_clears_pending_failure(self):
        capture = audit.Audit()
        capture.capture("baekseok", '<input type="password" name="userPassword">', 200, "2026-09-24", "6")
        self.assertEqual(1, len(capture.failures))

        capture.capture("baekseok", source_page(place="6", summary="대관신청 테이블"), 200, "2026-09-24", "6")

        self.assertEqual({}, capture.failures)
        self.assertEqual(1, capture.expected[("goyang:baekseok:6", "2026-09-24")].total())

    def test_placeholder_and_numeric_both_selected_use_numeric(self):
        doc = html.fromstring(source_page(place="6"))
        self.assertEqual("6", audit.selected_place(doc))
        self.assertFalse(audit.blank_place_discovery(doc))
        self.assertEqual(["6"], [node.get("value") for node in audit.usable_place_options(doc)])

    def test_test_maintenance_court_cannot_be_published(self):
        doc = html.fromstring(source_page(place="14").replace('>테니스1</option>', '>(TEST)점검코트</option>'))
        with self.assertRaises(ValueError):
            audit.parse_gys(doc, "baekseok", "2026-09-24", "14")


if __name__ == "__main__":
    unittest.main()
