import unittest

import tennis_core


class TennisCoreUrlTests(unittest.TestCase):
    def test_facility_parser_and_slots_keep_original_reservation_url(self):
        html = '''
        <li class="reserve_box_item"><div class="reserve_title">용인 코트<div class="reserve_position">용인시</div></div>
        <div class="btn_wrap"><a href="/publicsports/sports/selectFcltyRceptResveViewU.do?resveId=123">예약</a></div></li>
        '''
        facilities = tennis_core.parse_facility_html(html)
        slots = {"20260712": [{"timeContent": "09:00 ~ 10:00"}]}

        tennis_core.attach_reserve_url(slots, facilities["123"]["reserveUrl"])

        self.assertIn("resveId=123", facilities["123"]["reserveUrl"])
        self.assertEqual(facilities["123"]["reserveUrl"], slots["20260712"][0]["reserveUrl"])


if __name__ == "__main__":
    unittest.main()
