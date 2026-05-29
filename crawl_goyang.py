import os
import re
import json
import calendar
import datetime as dt
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)
TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[~\-]\s*(\d{1,2}:\d{2})")

KST = dt.timezone(dt.timedelta(hours=9))


def kst_now() -> dt.datetime:
    return dt.datetime.now(tz=KST)


def build_date_range_kst(
    cutoff_day: int,
    cutoff_hour: int,
    cutoff_minute: int = 0,
    start_date: dt.date | None = None,
) -> Tuple[bool, List[str]]:
    now = kst_now()
    start = start_date or now.date()

    cutoff_passed = (
        (now.day > cutoff_day)
        or (now.day == cutoff_day and (now.hour > cutoff_hour or (now.hour == cutoff_hour and now.minute >= cutoff_minute)))
    )

    if cutoff_passed:
        year = now.year + (1 if now.month == 12 else 0)
        month = 1 if now.month == 12 else now.month + 1
        end_day = calendar.monthrange(year, month)[1]
        end = dt.date(year, month, end_day)
    else:
        end_day = calendar.monthrange(now.year, now.month)[1]
        end = dt.date(now.year, now.month, end_day)

    dates: List[str] = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)

    return cutoff_passed, dates


def yyyymmdd_from_ymd(ymd: str) -> str:
    return ymd.replace("-", "")


def yyyymmdd_parts(yyyymmdd: str) -> Tuple[str, str, str]:
    return yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:8]


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return s


def fix_encoding(r: requests.Response) -> str:
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    return r.text


GYT_BASE = "https://www.gytennis.or.kr/daily"
GYT_NAME = {
    1: "대화코트",
    2: "삼송유수지코트",
    3: "성라코트",
    4: "성사전천후코트",
    5: "성사실외코트",
    6: "중산코트",
    7: "충장코트",
    8: "킨텍스유수지코트",
    9: "토당코트",
    10: "화정코트",
}


def _normalize_time_label(text: str) -> str:
    s = " ".join(text.split()).strip()
    s = s.replace(" : ", ":").replace(": ", ":").replace(" :", ":")
    s = s.replace(" ~ ", "~").replace("~ ", "~").replace(" ~", "~")
    s = s.replace(" ", "")
    return s


def parse_gytennis_slots(html: str) -> Dict[str, List[dict]]:
    soup = BeautifulSoup(html, "lxml")

    time_labels: List[str] = []
    for td in soup.select("table.custom tr td.wide"):
        time_labels.append(_normalize_time_label(td.get_text()))

    out: Dict[str, List[dict]] = {}

    for tbl in soup.select("table.innerCustom"):
        tag = tbl.select_one("td.courtTag")
        if not tag:
            continue

        court_text = " ".join(tag.get_text().split())
        m = re.match(r"(\d+)", court_text)
        if not m:
            continue
        court_no = m.group(1)

        rows = tbl.select("tr")[1:]
        for idx, tr in enumerate(rows):
            td = tr.select_one("td.resTag")
            if not td:
                continue

            # 사이트에서 예약 가능 체크박스 name이 수시로 바뀌므로
            # disabled가 아닌 checkbox 기준으로 가용 슬롯을 판단한다.
            avail_cb = td.select_one('input[type="checkbox"]:not([disabled])')
            if not avail_cb:
                continue

            label = time_labels[idx] if idx < len(time_labels) else f"IDX:{idx}"
            if "~" in label:
                start, end = label.split("~", 1)
                time_content = f"{start} ~ {end}"
                slot_key = f"{start}~{end}"
            else:
                time_content = label
                slot_key = label

            out.setdefault(court_no, []).append(
                {"timeContent": time_content, "slotKey": slot_key, "courtNo": str(court_no)}
            )

    return out


def fetch_gytennis_day(
    session: requests.Session,
    courtvalue: int,
    ymd: str,
    ssl_fallback_state: dict | None = None,
) -> Dict[str, List[dict]]:
    url = f"{GYT_BASE}/{courtvalue}/{ymd}"
    url_alt = f"{GYT_BASE}/{courtvalue}/{ymd.replace('-', '')}"
    base_court_url = f"{GYT_BASE}/{courtvalue}"
    use_insecure = bool((ssl_fallback_state or {}).get("use_insecure"))
    prefer_curl = (os.getenv("GYT_USE_CURL_CFFI") or "0").strip() == "1"

    req_headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://www.gytennis.or.kr/daily",
    }

    def _get(u: str, insecure: bool):
        if prefer_curl and curl_requests is not None:
            return curl_requests.get(u, timeout=20, verify=(not insecure), impersonate="chrome", headers=req_headers)
        return session.get(u, timeout=20, verify=(not insecure), headers=req_headers)

    def _post(u: str, data: dict, insecure: bool):
        if prefer_curl and curl_requests is not None:
            return curl_requests.post(
                u,
                data=data,
                timeout=20,
                verify=(not insecure),
                impersonate="chrome",
                headers={**req_headers, "Referer": u},
            )
        return session.post(u, data=data, timeout=20, verify=(not insecure), headers={**req_headers, "Referer": u})

    def _valid_slots_html(h: str) -> bool:
        if not h:
            return False
        if "location.replace('https://www.gytennis.or.kr')" in h:
            return False
        soup = BeautifulSoup(h, "lxml")
        return bool(soup.select("table.custom") and soup.select("table.innerCustom"))

    def _fetch_via_form(insecure: bool):
        # ?? ?? ??: ?? ??? GET -> hidden ? ?? -> POST(cvalue/cdate/van_code)
        r0 = _get(base_court_url, insecure)
        if r0.status_code != 200:
            return r0
        html0 = fix_encoding(r0)
        soup = BeautifulSoup(html0, "lxml")
        payload: Dict[str, str] = {}
        for inp in soup.select("form input[name]"):
            name = inp.get("name")
            if not name:
                continue
            t = (inp.get("type") or "").lower()
            if t in ("checkbox", "radio"):
                continue
            if name.endswith("[]"):
                continue
            payload[name] = inp.get("value", "")
        payload["cvalue"] = str(courtvalue)
        payload["cdate"] = ymd
        return _post(base_court_url, payload, insecure)

    try:
        r = _fetch_via_form(use_insecure)
    except requests.exceptions.SSLError as e:
        if ssl_fallback_state is not None:
            ssl_fallback_state["use_insecure"] = True
        print(f"[GYT][SSL_WARN] switch to verify=False for gytennis session: first_fail cv={courtvalue} date={ymd} err={e}")
        r = _fetch_via_form(True)

    html = fix_encoding(r) if r.status_code == 200 else ""
    if r.status_code != 200 or not _valid_slots_html(html):
        # fallback 1: ?? URL GET
        try:
            r2 = _get(url, bool((ssl_fallback_state or {}).get("use_insecure")))
            html2 = fix_encoding(r2) if r2.status_code == 200 else ""
            if r2.status_code == 200 and _valid_slots_html(html2):
                html = html2
            else:
                # fallback 2: compact ?? URL
                r3 = _get(url_alt, bool((ssl_fallback_state or {}).get("use_insecure")))
                html3 = fix_encoding(r3) if r3.status_code == 200 else ""
                if r3.status_code == 200 and _valid_slots_html(html3):
                    html = html3
                else:
                    return {}
        except requests.exceptions.SSLError:
            if ssl_fallback_state is not None:
                ssl_fallback_state["use_insecure"] = True
            r2 = _get(url, True)
            html2 = fix_encoding(r2) if r2.status_code == 200 else ""
            if r2.status_code == 200 and _valid_slots_html(html2):
                html = html2
            else:
                r3 = _get(url_alt, True)
                html3 = fix_encoding(r3) if r3.status_code == 200 else ""
                if r3.status_code == 200 and _valid_slots_html(html3):
                    html = html3
                else:
                    return {}

    parsed = parse_gytennis_slots(html)
    best = parsed
    best_count = sum(len(v) for v in best.values()) if best else 0

    # 폼 응답과 URL 조회 결과를 비교해 더 많은 슬롯을 채택
    for u in (url, url_alt):
        try:
            rr = _get(u, bool((ssl_fallback_state or {}).get("use_insecure")))
        except requests.exceptions.SSLError:
            if ssl_fallback_state is not None:
                ssl_fallback_state["use_insecure"] = True
            rr = _get(u, True)
        if rr.status_code != 200:
            continue
        hh = fix_encoding(rr)
        if not _valid_slots_html(hh):
            continue
        parsed2 = parse_gytennis_slots(hh)
        cnt2 = sum(len(v) for v in parsed2.values()) if parsed2 else 0
        if cnt2 > best_count:
            best = parsed2
            best_count = cnt2

    return best


def warmup_gytennis_session(session: requests.Session, ssl_fallback_state: dict) -> None:
    for url in ("https://www.gytennis.or.kr/", "https://www.gytennis.or.kr/daily"):
        use_insecure = bool(ssl_fallback_state.get("use_insecure"))
        try:
            session.get(url, timeout=20, verify=(not use_insecure), headers={"Referer": "https://www.gytennis.or.kr/"})
        except requests.exceptions.SSLError as e:
            ssl_fallback_state["use_insecure"] = True
            print(f"[GYT][SSL_WARN] switch to verify=False during warmup: url={url} err={e}")
            try:
                session.get(url, timeout=20, verify=False, headers={"Referer": "https://www.gytennis.or.kr/"})
            except requests.RequestException as e2:
                print(f"[GYT][WARMUP_WARN] verify=False warmup failed: url={url} err={e2}")
        except requests.RequestException as e:
            print(f"[GYT][WARMUP_WARN] warmup failed: url={url} err={e}")


def crawl_gytennis() -> dict:
    cutoff_passed, dates = build_date_range_kst(cutoff_day=25, cutoff_hour=22, cutoff_minute=0)
    now = kst_now()
    print(f"[GYT] KST now={now:%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates)}")

    s = make_session()
    ssl_fallback_state = {"use_insecure": False}
    warmup_gytennis_session(s, ssl_fallback_state)

    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}
    avail_lock = threading.Lock()

    for cv in range(1, 11):
        fid = f"gy-gytennis-{cv}"
        facilities[fid] = {"title": f"고양테니스협회 {GYT_NAME.get(cv, f'courtvalue {cv}')}", "location": "고양시", "courtvalue": cv}
        availability[fid] = {}

    stats = {"total": 0, "ok": 0, "empty": 0, "fail": 0}
    debug_dump = (os.getenv("GYT_DEBUG_DUMP") or "0").strip() == "1"

    def _dump_gyt_debug_samples(tag: str, sample_dates: List[str]) -> None:
        if not debug_dump:
            return
        out_dir = Path("debug") / "gytennis"
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_lines = [f"tag={tag}", f"ts={kst_now().isoformat()}"]
        for cv in (1, 2):
            for ymd in sample_dates[:2]:
                url = f"{GYT_BASE}/{cv}/{ymd}"
                try:
                    use_insecure = bool(ssl_fallback_state.get("use_insecure"))
                    r = s.get(
                        url,
                        timeout=20,
                        verify=(not use_insecure),
                        headers={
                            "User-Agent": UA,
                            "Accept": "text/html,application/xhtml+xml",
                            "Referer": "https://www.gytennis.or.kr/daily",
                        },
                    )
                except requests.exceptions.SSLError:
                    ssl_fallback_state["use_insecure"] = True
                    r = s.get(
                        url,
                        timeout=20,
                        verify=False,
                        headers={
                            "User-Agent": UA,
                            "Accept": "text/html,application/xhtml+xml",
                            "Referer": "https://www.gytennis.or.kr/daily",
                        },
                    )
                html = fix_encoding(r)
                soup = BeautifulSoup(html, "lxml")
                enabled = len(soup.select('td.resTag input[type="checkbox"]:not([disabled])'))
                has_redirect = "location.replace('https://www.gytennis.or.kr')" in html
                name = f"{tag}_cv{cv}_{ymd}.html"
                (out_dir / name).write_text(html, encoding="utf-8")
                summary_lines.append(
                    f"{name} status={r.status_code} len={len(html)} enabled={enabled} redirect_script={has_redirect}"
                )
        (out_dir / f"{tag}_summary.txt").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    def _task(cv: int, ymd: str) -> Tuple[int, str, str, List[dict]]:
        try:
            court_slots = fetch_gytennis_day(s, cv, ymd, ssl_fallback_state)
            if not court_slots:
                return cv, ymd, "empty", []
            flat: List[dict] = []
            for _, slots in court_slots.items():
                flat.extend(slots)
            if not flat:
                return cv, ymd, "empty", []
            return cv, ymd, "ok", flat
        except Exception as e:
            print(f"[GYT][ERR] cv={cv} date={ymd} err={e}")
            return cv, ymd, "fail", []

    def _probe_session(sample_size: int = 5) -> bool:
        # 초기 샘플이 전부 empty면 현재 세션/환경이 막힌 상태로 판단
        checked = 0
        ok_found = 0
        for ymd in dates[: min(len(dates), 3)]:
            for cv in range(1, 11):
                checked += 1
                try:
                    court_slots = fetch_gytennis_day(s, cv, ymd, ssl_fallback_state)
                    flat_count = sum(len(v) for v in court_slots.values()) if court_slots else 0
                    if flat_count > 0:
                        ok_found += 1
                        return True
                except Exception as e:
                    print(f"[GYT][PROBE_ERR] cv={cv} date={ymd} err={e}")
                if checked >= sample_size:
                    return ok_found > 0
        return ok_found > 0

    probe_ok = _probe_session(sample_size=6)
    if not probe_ok:
        _dump_gyt_debug_samples("probe_fail_1", dates)
        print("[GYT][EARLY_ABORT] probe detected all-empty pattern; retry with new session")
        s = make_session()
        ssl_fallback_state = {"use_insecure": False}
        warmup_gytennis_session(s, ssl_fallback_state)
        probe_ok = _probe_session(sample_size=8)
        if not probe_ok:
            _dump_gyt_debug_samples("probe_fail_2", dates)
            print("[GYT][EARLY_ABORT] probe failed again; skip full gytennis crawl for this run")
            print(f"[GYT][STATS] total={stats['total']} ok={stats['ok']} empty={stats['empty']} fail={stats['fail']}")
            return {"facilities": facilities, "availability": availability}

    futures = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for ymd in dates:
            for cv in range(1, 11):
                futures.append(ex.submit(_task, cv, ymd))

        for fut in as_completed(futures):
            cv, ymd, status, flat = fut.result()
            stats["total"] += 1
            if status == "ok":
                fid = f"gy-gytennis-{cv}"
                with avail_lock:
                    availability[fid].setdefault(ymd, []).extend(flat)
                stats["ok"] += 1
            elif status == "empty":
                stats["empty"] += 1
            else:
                stats["fail"] += 1

    print(f"[GYT][STATS] total={stats['total']} ok={stats['ok']} empty={stats['empty']} fail={stats['fail']}")

    return {"facilities": facilities, "availability": availability}


DAEHWA_BASE = "https://daehwa.gys.or.kr:451"
DAEHWA_LOGIN = DAEHWA_BASE + "/member/login.php?preURL=%2Frent%2Ftennis_rent.php"
DAEHWA_RENT = DAEHWA_BASE + "/rent/tennis_rent.php"
DAEHWA_PLACE = {1: "2", 2: "7", 3: "8", 4: "9"}


def _daehwa_get(s: requests.Session, url: str, ssl_fallback_state: dict, **kwargs) -> requests.Response:
    use_insecure = bool(ssl_fallback_state.get("use_insecure"))
    try:
        return s.get(url, verify=(not use_insecure), **kwargs)
    except requests.exceptions.SSLError as e:
        ssl_fallback_state["use_insecure"] = True
        print(f"[DAEHWA][SSL_WARN] switch to verify=False (GET) url={url} err={e}")
        return s.get(url, verify=False, **kwargs)


def _daehwa_post(s: requests.Session, url: str, ssl_fallback_state: dict, **kwargs) -> requests.Response:
    use_insecure = bool(ssl_fallback_state.get("use_insecure"))
    try:
        return s.post(url, verify=(not use_insecure), **kwargs)
    except requests.exceptions.SSLError as e:
        ssl_fallback_state["use_insecure"] = True
        print(f"[DAEHWA][SSL_WARN] switch to verify=False (POST) url={url} err={e}")
        return s.post(url, verify=False, **kwargs)


def is_login_page(html: str, final_url: str = "") -> bool:
    if final_url and "member/login.php" in final_url:
        return True
    soup = BeautifulSoup(html, "lxml")
    return soup.select_one('input[type="password"]') is not None


def login_daehwa(s: requests.Session, ssl_fallback_state: dict) -> None:
    user_id = os.environ.get("GYS_ID")
    user_pw = os.environ.get("GYS_PW")
    if not user_id or not user_pw:
        raise RuntimeError("Set env vars GYS_ID / GYS_PW for daehwa login")

    r0 = _daehwa_get(s, DAEHWA_LOGIN, ssl_fallback_state, allow_redirects=True, timeout=20)
    html0 = fix_encoding(r0)

    if not is_login_page(html0, r0.url):
        return

    soup = BeautifulSoup(html0, "lxml")
    form = soup.find("form")
    if not form:
        raise RuntimeError("Login form not found in HTML")

    action = form.get("action") or ""
    post_url = DAEHWA_LOGIN if not action else urljoin(DAEHWA_LOGIN, action)

    payload: Dict[str, str] = {}
    for inp in form.select("input[name]"):
        name = inp.get("name")
        itype = (inp.get("type") or "").lower()
        if itype in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                payload[name] = inp.get("value", "on")
        elif itype != "password":
            payload[name] = inp.get("value", "")

    id_field = None
    pw_field = None
    pw = form.select_one('input[type="password"][name]')
    if pw:
        pw_field = pw.get("name")

    cand = form.select_one('input[name*="id" i], input[name*="user" i]')
    if cand and cand.get("name"):
        id_field = cand.get("name")
    if not id_field:
        txt = form.select_one('input[type="text"][name]')
        if txt:
            id_field = txt.get("name")

    if not id_field or not pw_field:
        raise RuntimeError(f"Cannot detect login fields: id={id_field}, pw={pw_field}")

    payload[id_field] = user_id
    payload[pw_field] = user_pw

    r1 = _daehwa_post(
        s,
        post_url,
        ssl_fallback_state=ssl_fallback_state,
        data=payload,
        allow_redirects=True,
        timeout=20,
        headers={"Origin": DAEHWA_BASE, "Referer": r0.url},
    )
    html1 = fix_encoding(r1)

    if is_login_page(html1, r1.url):
        raise RuntimeError("Daehwa login failed")


def build_payload_daehwa(place_opt: str, yyyymmdd: str) -> Dict[str, str]:
    y, m, d = yyyymmdd_parts(yyyymmdd)
    return {
        "rent_date": yyyymmdd,
        "regno": "",
        "com_nm": "",
        "use_tel": "",
        "use_hp": "",
        "use_fax": "",
        "use_zipcd": "",
        "use_addr": "",
        "use_event_name": "",
        "inwon": "",
        "etc": "",
        "rent_type": "50",
        "offline_yn": "",
        "use_concept": "",
        "sort_order": "",
        "stime": "",
        "etime": "",
        "rent_stime": "",
        "rent_etime": "",
        "rent_p_stime": "",
        "rent_p_etime": "",
        "min_time": "2",
        "use_time": "0",
        "time_gbn": "01",
        "observance": "",
        "addtime_type": "1001",
        "addtime_rate": "",
        "TempPay": "0",
        "etc01": "0",
        "etc02": "0",
        "rent_file": "",
        "nyear": y,
        "nmonth": m,
        "nday": d,
        "myReserveInfo": "",
        "part_opt": "02",
        "part_nm": "",
        "pay_opt": "",
        "account_no": "",
        "tel": "031-929-4863",
        "part_hp_no": "031-929-4863",
        "toMail": "",
        "place_nm": "",
        "place_opt": str(place_opt),
        "rent_gubun": "1001",
        "TotalPay": "0",
    }


def parse_slots_daehwa(html: str, summary_keyword: str = "이용신청 테이블") -> List[dict]:
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", attrs={"summary": re.compile(re.escape(summary_keyword))})
    if not table:
        for t in soup.find_all("table"):
            if t.select_one('input[name="rent_chk[]"]'):
                table = t
                break
    if not table:
        return []

    out = []
    for tr in table.find_all("tr"):
        cb = tr.select_one('input[name="rent_chk[]"]')
        if not cb:
            continue
        if cb.has_attr("disabled"):
            continue

        val = (cb.get("value") or "").strip()
        if re.fullmatch(r"\d{8}", val):
            continue

        txt = re.sub(r"\s+", " ", tr.get_text(" ", strip=True)).strip()
        m = TIME_RE.search(txt)
        if not m:
            continue

        start = m.group(1).zfill(5)
        end = m.group(2).zfill(5)

        out.append({"timeContent": f"{start} ~ {end}", "slotKey": f"{start}~{end}", "rent_chk": val})

    return out


def post_rent(s: requests.Session, payload: Dict[str, str], ssl_fallback_state: dict) -> Tuple[str, str, int]:
    r = _daehwa_post(
        s,
        DAEHWA_RENT,
        ssl_fallback_state=ssl_fallback_state,
        data=payload,
        allow_redirects=True,
        timeout=25,
        headers={"Origin": DAEHWA_BASE, "Referer": DAEHWA_RENT},
    )
    html = fix_encoding(r)
    return html, r.url, r.status_code


def crawl_daehwa() -> dict:
    cutoff_passed, dates_ymd = build_date_range_kst(cutoff_day=25, cutoff_hour=10, cutoff_minute=0)
    now = kst_now()
    print(f"[DAEHWA] KST now={now:%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates_ymd)}")

    s = make_session()
    ssl_fallback_state = {"use_insecure": False}
    login_daehwa(s, ssl_fallback_state)

    facility_id = "gy-daehwa"
    facilities = {facility_id: {"title": "고양 대화 테니스장", "location": "고양시"}}
    availability: Dict[str, Dict[str, List[dict]]] = {facility_id: {}}

    stats = {"total": 0, "ok": 0, "empty": 0, "fail": 0}

    for ymd in dates_ymd:
        yyyymmdd = yyyymmdd_from_ymd(ymd)

        day_slots: List[dict] = []
        for court_no, place_opt in DAEHWA_PLACE.items():
            stats["total"] += 1
            try:
                payload = build_payload_daehwa(place_opt, yyyymmdd)
                html, final_url, _status = post_rent(s, payload, ssl_fallback_state)

                if is_login_page(html, final_url):
                    login_daehwa(s, ssl_fallback_state)
                    html, final_url, _status = post_rent(s, payload, ssl_fallback_state)

                if is_login_page(html, final_url):
                    raise RuntimeError(f"daehwa login required after retry. final_url={final_url}")

                slots = parse_slots_daehwa(html)
                for sl in slots:
                    sl["courtNo"] = str(court_no)
                if slots:
                    day_slots.extend(slots)
                    stats["ok"] += 1
                else:
                    stats["empty"] += 1
            except Exception as e:
                stats["fail"] += 1
                print(f"[DAEHWA][ERR] date={ymd} court={court_no} place_opt={place_opt} err={e}")

        if day_slots:
            availability[facility_id].setdefault(ymd, []).extend(day_slots)

    print(f"[DAEHWA][STAT] total={stats['total']} ok={stats['ok']} empty={stats['empty']} fail={stats['fail']}")

    return {"facilities": facilities, "availability": availability}


def merge(a: dict, b: dict) -> dict:
    out = {
        "facilities": {**a.get("facilities", {}), **b.get("facilities", {})},
        "availability": {**a.get("availability", {}), **b.get("availability", {})},
    }
    return out


if __name__ == "__main__":
    out_gyt = crawl_gytennis()
    out_dae = crawl_daehwa()

    print(json.dumps(merge(out_gyt, out_dae), ensure_ascii=False, indent=2))
