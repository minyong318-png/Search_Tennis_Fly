import json
import subprocess
import textwrap
import unittest


def run_node_expr(source: str):
    completed = subprocess.run(
        ["node", "--input-type=module", "-e", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(completed.stdout)


class SeongnamAndroidCollectorTests(unittest.TestCase):
    def test_adb_devices_output_is_classified(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { parseAdbDevices } from './scripts/seongnam_android_collector.mjs';
                console.log(JSON.stringify({
                  device: parseAdbDevices('List of devices attached\\nabc123\\tdevice\\n'),
                  offline: parseAdbDevices('List of devices attached\\nabc123\\toffline\\n'),
                  unauthorized: parseAdbDevices('List of devices attached\\nabc123\\tunauthorized\\n'),
                  missing: parseAdbDevices('List of devices attached\\n')
                }));
                """
            )
        )

        self.assertEqual("device", result["device"]["status"])
        self.assertEqual("android_device_offline", result["offline"]["status"])
        self.assertEqual("android_unauthorized", result["unauthorized"]["status"])
        self.assertEqual("android_device_offline", result["missing"]["status"])

    def test_resolves_tennistown_adb_fallback(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { resolveAdbPath } from './scripts/seongnam_android_collector.mjs';
                console.log(JSON.stringify({adb: resolveAdbPath()}));
                """
            )
        )

        self.assertTrue(result["adb"].endswith("adb.exe") or result["adb"] == "adb")

    def test_number_env_uses_fallback_for_missing_or_empty_values(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { numberEnv } from './scripts/seongnam_android_collector.mjs';
                delete process.env.SEONGNAM_TEST_NUMBER;
                const missing = numberEnv('SEONGNAM_TEST_NUMBER', 8, 1, 10);
                process.env.SEONGNAM_TEST_NUMBER = '';
                const empty = numberEnv('SEONGNAM_TEST_NUMBER', 8, 1, 10);
                process.env.SEONGNAM_TEST_NUMBER = '5';
                const present = numberEnv('SEONGNAM_TEST_NUMBER', 8, 1, 10);
                console.log(JSON.stringify({ missing, empty, present }));
                """
            )
        )

        self.assertEqual(8, result["missing"])
        self.assertEqual(8, result["empty"])
        self.assertEqual(5, result["present"])

    def test_selects_non_auto_detect_seongnam_tab(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { selectSeongnamTab } from './scripts/seongnam_android_collector.mjs';
                const tabs = [
                  {url: 'https://res.isdc.co.kr/auto_detect.do', title: 'blocked', webSocketDebuggerUrl: 'ws://bad'},
                  {url: 'https://example.test/', title: 'other', webSocketDebuggerUrl: 'ws://other'},
                  {url: 'https://res.isdc.co.kr/facilityList.do?facType=29', title: '성남 예약', webSocketDebuggerUrl: 'ws://ok'}
                ];
                console.log(JSON.stringify(selectSeongnamTab(tabs)));
                """
            )
        )

        self.assertEqual("ws://ok", result["webSocketDebuggerUrl"])

    def test_detects_when_only_seongnam_tab_is_auto_detect(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { hasOnlyBlockedSeongnamTabs } from './scripts/seongnam_android_collector.mjs';
                const tabs = [
                  {url: 'https://res.isdc.co.kr/auto_detect.do', title: 'blocked', webSocketDebuggerUrl: 'ws://bad'}
                ];
                console.log(JSON.stringify({blocked: hasOnlyBlockedSeongnamTabs(tabs)}));
                """
            )
        )

        self.assertTrue(result["blocked"])

    def test_classifies_login_and_auto_detect_pages(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { classifyPage } from './scripts/seongnam_android_collector.mjs';
                console.log(JSON.stringify({
                  login: classifyPage('https://res.isdc.co.kr/login.do', '<input name="web_id"><input name="web_pw">'),
                  blocked: classifyPage('https://res.isdc.co.kr/auto_detect.do', '비정상 접근 탐지'),
                  ok: classifyPage('https://res.isdc.co.kr/facilityList.do?facType=29', '<input name="groupId" value="1">')
                }));
                """
            )
        )

        self.assertEqual("login_required", result["login"])
        self.assertEqual("automation_blocked", result["blocked"])
        self.assertEqual("ok", result["ok"])

    def test_parses_network_html_and_slots(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { parseFacilityListHtml, parseGroupHtml, parseTimetableHtml } from './scripts/seongnam_android_collector.mjs';
                const list = '<input name="groupId" value="1"><div class="head-area">탄천 테니스장</div>';
                const group = '<li id="FAC001" class="facilityInfo"><div class="head-area">1번 코트</div></li>';
                const table = '<tr><td><input name="rbTime" value="0900"></td><td></td><td>09:00 ~ 10:00</td><td></td></tr>';
                console.log(JSON.stringify({
                  groups: parseFacilityListHtml(list),
                  courts: parseGroupHtml(group, '탄천'),
                  slots: parseTimetableHtml(table)
                }));
                """
            )
        )

        self.assertEqual("1", result["groups"][0]["groupId"])
        self.assertEqual("FAC001", result["courts"][0]["facId"])
        self.assertEqual("09:00 ~ 10:00", result["slots"][0]["timeContent"])

    def test_dom_fallback_and_sanitized_headers(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { parseDomFallbackRows, sanitizeHeaders } from './scripts/seongnam_android_collector.mjs';
                const rows = parseDomFallbackRows([
                  {facility: '탄천', court: '1번', date: '2026-07-12', time: '09:00 ~ 10:00', status: '예약가능', remaining: '2'}
                ]);
                const headers = sanitizeHeaders({Cookie: 'secret', Authorization: 'bearer x', Accept: 'text/html'});
                console.log(JSON.stringify({rows, headers}));
                """
            )
        )

        self.assertEqual("20260712", result["rows"][0]["date"])
        self.assertNotIn("Cookie", result["headers"])
        self.assertNotIn("Authorization", result["headers"])
        self.assertEqual("text/html", result["headers"]["Accept"])

    def test_map_with_concurrency_limits_active_jobs(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { mapWithConcurrency } from './scripts/seongnam_android_collector.mjs';
                let active = 0;
                let maxActive = 0;
                const values = await mapWithConcurrency([1, 2, 3, 4, 5], 2, async (value) => {
                  active += 1;
                  maxActive = Math.max(maxActive, active);
                  await new Promise((resolve) => setTimeout(resolve, 20));
                  active -= 1;
                  return value * 2;
                });
                console.log(JSON.stringify({ values, maxActive }));
                """
            )
        )

        self.assertEqual([2, 4, 6, 8, 10], result["values"])
        self.assertLessEqual(result["maxActive"], 2)

    def test_retry_async_retries_transient_failures(self):
        result = run_node_expr(
            textwrap.dedent(
                """
                import { retryAsync } from './scripts/seongnam_android_collector.mjs';
                let attempts = 0;
                const value = await retryAsync(async () => {
                  attempts += 1;
                  if (attempts < 2) return { fetchError: 'temporary' };
                  return { ok: true, value: 42 };
                }, 1, 0);
                console.log(JSON.stringify({ attempts, value }));
                """
            )
        )

        self.assertEqual(2, result["attempts"])
        self.assertEqual(42, result["value"]["value"])


if __name__ == "__main__":
    unittest.main()
