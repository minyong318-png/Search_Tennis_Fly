import os
import re
import json
import calendar
import datetime as dt
import time
from typing import Dict, List, Tuple
from urllib.parse import quote, urljoin

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


def _is_ssl_error(exc: BaseException) -> bool:
    """Treat curl_cffi's certificate errors like requests SSL errors."""
    error_types = [requests.exceptions.SSLError]
    curl_exceptions = getattr(curl_requests, "exceptions", None)
    curl_ssl_error = getattr(curl_exceptions, "SSLError", None)
    if isinstance(curl_ssl_error, type):
        error_types.append(curl_ssl_error)
    return isinstance(exc, tuple(error_types))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)
TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[~\-]\s*(\d{1,2}:\d{2})")

KST = dt.timezone(dt.timedelta(hours=9))


def _bounded_float_env(name: str, default: float, min_value: float = 2.0, max_value: float = 20.0) -> float:
    try:
        value = float((os.getenv(name) or str(default)).strip())
    except ValueError:
        value = default
    return max(min_value, min(value, max_value))


def _gyt_timeout() -> float:
    return _bounded_float_env("GYT_TIMEOUT", 8.0)


def _gys_timeout() -> float:
    return _bounded_float_env("GYS_TIMEOUT", 8.0)


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


def make_gytennis_session() -> requests.Session:
    s = make_session()
    # The crawler owns bounded retries and rate-limit backoff.
    s.mount("https://", HTTPAdapter(max_retries=0))
    proxy_url = (os.getenv("GYT_PROXY_URL") or "").strip()
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})
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
        out.setdefault(court_no, [])

        rows = tbl.select("tr")[1:]
        for idx, tr in enumerate(rows):
            td = tr.select_one("td.resTag")
            if not td:
                continue

            # gytennis는 비로그인 현황 화면에서 빈 슬롯을 public-empty-slot으로 표시한다.
            if not td.select_one("span.public-empty-slot"):
                # 예전 로그인/예약 화면 호환: 활성 checkbox가 있으면 가용 슬롯으로 본다.
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


def _compact_ymd(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def gytennis_html_matches_date(html: str, ymd: str) -> bool:
    soup = BeautifulSoup(html or "", "lxml")
    expected = _compact_ymd(ymd)
    if not expected:
        return False
    values = [
        inp.get("value", "")
        for inp in soup.select('input[name="cdate"], input#cdate, input[name*="date" i]')
    ]
    for value in values:
        compact = _compact_ymd(value)
        if compact and compact == expected:
            return True
    return False


def fetch_gytennis_day(
    session: requests.Session,
    courtvalue: int,
    ymd: str,
    ssl_fallback_state: dict | None = None,
) -> Dict[str, List[dict]]:
    """Read one dated public timetable; invalid responses are never sold-out days."""
    url = f"{GYT_BASE}/{courtvalue}/{ymd}"
    state = ssl_fallback_state if ssl_fallback_state is not None else {}
    prefer_curl = (os.getenv("GYT_USE_CURL_CFFI") or "0").strip() == "1"
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://www.gytennis.or.kr/daily",
    }

    def get_page():
        state["_request_get"] = state.get("_request_get", 0) + 1
        kwargs = dict(timeout=_gyt_timeout(), verify=not state.get("use_insecure"), headers=headers)
        if prefer_curl and curl_requests is not None:
            return curl_requests.get(
                url, **kwargs, impersonate="chrome",
                proxy=(os.getenv("GYT_PROXY_URL") or "").strip() or None,
            )
        return session.get(url, **kwargs)

    try:
        response = get_page()
    except Exception as exc:
        if not _is_ssl_error(exc):
            raise
        state["use_insecure"] = True
        print("[GYT][SSL_WARN] using existing certificate fallback")
        response = get_page()

    if response.status_code != 200:
        raise requests.HTTPError(
            f"GYT HTTP {response.status_code}: court={courtvalue} date={ymd}",
            response=response,
        )
    html = fix_encoding(response)
    if not gytennis_html_matches_date(html, ymd):
        raise ValueError(f"GYT date mismatch: court={courtvalue} expected={ymd}")
    soup = BeautifulSoup(html, "lxml")
    times = soup.select("table.custom tr td.wide")
    courts = soup.select("table.innerCustom")
    if not times or not courts:
        raise ValueError(f"GYT missing timetable: court={courtvalue} date={ymd}")
    if any(not TIME_RE.fullmatch(_normalize_time_label(td.get_text())) for td in times):
        raise ValueError(f"GYT invalid time labels: court={courtvalue} date={ymd}")
    for court in courts:
        tag = court.select_one("td.courtTag")
        if not tag or not re.match(r"\d+", tag.get_text().strip()):
            raise ValueError(f"GYT missing court number: court={courtvalue} date={ymd}")
        if len(court.select("td.resTag")) != len(times):
            raise ValueError(f"GYT incomplete timetable: court={courtvalue} date={ymd}")
    return parse_gytennis_slots(html)


def warmup_gytennis_session(session: requests.Session, ssl_fallback_state: dict) -> None:
    for url in ("https://www.gytennis.or.kr/", "https://www.gytennis.or.kr/daily"):
        use_insecure = bool(ssl_fallback_state.get("use_insecure"))
        try:
            session.get(url, timeout=_gyt_timeout(), verify=(not use_insecure), headers={"Referer": "https://www.gytennis.or.kr/"})
        except requests.exceptions.SSLError as e:
            ssl_fallback_state["use_insecure"] = True
            print(f"[GYT][SSL_WARN] switch to verify=False during warmup: url={url} err={e}")
            try:
                session.get(url, timeout=_gyt_timeout(), verify=False, headers={"Referer": "https://www.gytennis.or.kr/"})
            except requests.RequestException as e2:
                print(f"[GYT][WARMUP_WARN] verify=False warmup failed: url={url} err={e2}")
        except requests.RequestException as e:
            print(f"[GYT][WARMUP_WARN] warmup failed: url={url} err={e}")


def crawl_gytennis() -> dict:
    cutoff_passed, dates = build_date_range_kst(cutoff_day=25, cutoff_hour=22, cutoff_minute=0)
    print(f"[GYT] KST now={kst_now():%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates)}")
    session = make_gytennis_session()
    state = {"use_insecure": False}
    facilities = {
        f"gy-gytennis-{cv}": {
            "title": f"고양테니스협회 {GYT_NAME[cv]}", "location": "고양시",
            "courtvalue": cv, "_court_numbers": [],
        }
        for cv in range(1, 11)
    }
    availability = {fid: {} for fid in facilities}
    stats = {"total": 0, "ok": 0, "empty": 0, "fail": 0}
    # One dated GET per page, at most one per second. The previous six workers
    # made 3-5 requests per page and silently treated HTTP 429 as no availability.
    delay = _bounded_float_env("GYT_REQUEST_DELAY_SECONDS", 1.0, 1.0, 10.0)
    error_message = ""
    for ymd in dates:
        for cv in range(1, 11):
            if stats["total"]:
                time.sleep(delay)
            stats["total"] += 1
            court_slots = None
            for attempt in range(3):
                try:
                    court_slots = fetch_gytennis_day(session, cv, ymd, state)
                    if not court_slots:
                        raise ValueError(f"GYT missing court inventory: court={cv} date={ymd}")
                    break
                except Exception as exc:
                    court_slots = None
                    error_message = str(exc)
                    if attempt == 2:
                        stats["fail"] += 1
                        print(f"[GYT][ERR] {error_message}")
                        break
                    response = getattr(exc, "response", None)
                    wait = 5.0 * (attempt + 1)
                    if response is not None and response.status_code == 429:
                        retry_after = (getattr(response, "headers", {}) or {}).get("Retry-After", "")
                        wait = max(60.0, float(retry_after)) if str(retry_after).isdigit() else 60.0
                    print(f"[GYT][RETRY] cv={cv} date={ymd} attempt={attempt + 1} wait={wait:g}s error={error_message}")
                    time.sleep(wait)
            if court_slots is None:
                # Stop repeated traffic after an exhausted retry, preserving the
                # existing Goyang cache through the region's partial-failure guard.
                break
            fid = f"gy-gytennis-{cv}"
            known_courts = set(facilities[fid]["_court_numbers"])
            known_courts.update(court_slots)
            facilities[fid]["_court_numbers"] = sorted(known_courts, key=int)
            flat = [slot for slots in court_slots.values() for slot in slots]
            availability[fid][ymd] = flat
            stats["ok" if flat else "empty"] += 1
        if stats["fail"]:
            break
    print(f"[GYT][STATS] total={stats['total']} ok={stats['ok']} empty={stats['empty']} fail={stats['fail']}")
    print(f"[GYT][REQUESTS] get={state.get('_request_get', 0)} post=0")
    return {
        "facilities": facilities, "availability": availability,
        "partial_failure": stats["fail"] > 0,
        "error_message": error_message if stats["fail"] else "",
    }


DAEHWA_BASE = "https://daehwa.gys.or.kr:451"
DAEHWA_LOGIN = DAEHWA_BASE + "/member/login.php?preURL=%2Frent%2Ftennis_rent.php"
DAEHWA_RENT = DAEHWA_BASE + "/rent/tennis_rent.php"
DAEHWA_PLACE = {1: "2", 2: "7", 3: "8", 4: "9"}

BAEKSEOK_BASE = "https://gbc.gys.or.kr:446"
BAEKSEOK_RENT = BAEKSEOK_BASE + "/rent/tennis_rent.php"
BAEKSEOK_PART_OPT = "07"


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

    r0 = _daehwa_get(s, DAEHWA_LOGIN, ssl_fallback_state, allow_redirects=True, timeout=_gys_timeout())
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
        timeout=_gys_timeout(),
        headers={"Origin": DAEHWA_BASE, "Referer": r0.url},
    )
    html1 = fix_encoding(r1)

    if is_login_page(html1, r1.url):
        raise RuntimeError("Daehwa login failed")


def build_payload_gys(place_opt: str, yyyymmdd: str, part_opt: str = "02") -> Dict[str, str]:
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
        "part_opt": str(part_opt),
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


def build_payload_daehwa(place_opt: str, yyyymmdd: str) -> Dict[str, str]:
    return build_payload_gys(place_opt, yyyymmdd, part_opt="02")


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
        timeout=_gys_timeout(),
        headers={"Origin": DAEHWA_BASE, "Referer": DAEHWA_RENT},
    )
    html = fix_encoding(r)
    return html, r.url, r.status_code


def _gys_places(soup: BeautifulSoup) -> Dict[str, str]:
    places = {}
    for option in soup.select('select[name="place_opt"] option[value]'):
        value = option.get("value", "").strip()
        label = option.get_text(" ", strip=True)
        if value.isdigit() and not re.search(r"TEST|점검", label, re.I):
            places[value] = label
    return places


def _gys_selected_place(soup: BeautifulSoup) -> str:
    for selected in soup.select('select[name="place_opt"] option[selected], input[name="place_opt"][value]'):
        value = selected.get("value", "").strip()
        if value:
            return value
    return ""


def validate_gys_page(html: str, status: int, yyyymmdd: str, allow_selection: bool = False) -> BeautifulSoup:
    if status != 200:
        raise ValueError(f"GYS HTTP {status} date={yyyymmdd}")
    soup = BeautifulSoup(html, "lxml")
    date_input = soup.select_one('input[name="rent_date"]')
    if date_input is None or re.sub(r"\D", "", date_input.get("value", "")) != yyyymmdd:
        raise ValueError(f"GYS date mismatch date={yyyymmdd}")
    table = soup.find("table", attrs={"summary": re.compile("이용신청 테이블")})
    if table is None or not TIME_RE.search(table.get_text(" ", strip=True)):
        if allow_selection and not _gys_selected_place(soup) and _gys_places(soup) and soup.find("table", summary="행사 및 대관일정표입니다."):
            return soup
        raise ValueError(f"GYS missing timetable date={yyyymmdd}")
    return soup


def crawl_daehwa() -> dict:
    cutoff_passed, dates_ymd = build_date_range_kst(cutoff_day=25, cutoff_hour=10, cutoff_minute=0)
    now = kst_now()
    print(f"[DAEHWA] KST now={now:%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates_ymd)}")

    s = make_session()
    ssl_fallback_state = {"use_insecure": False}
    login_daehwa(s, ssl_fallback_state)

    facility_id = "gy-daehwa"
    facilities = {facility_id: {"title": "고양 대화 테니스장", "location": "고양시", "_court_numbers": [str(n) for n in DAEHWA_PLACE]}}
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

                validate_gys_page(html, _status, yyyymmdd)
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

        if stats["fail"]:
            break
        availability[facility_id][ymd] = day_slots

    print(f"[DAEHWA][STAT] total={stats['total']} ok={stats['ok']} empty={stats['empty']} fail={stats['fail']}")

    return {"facilities": facilities, "availability": availability, "partial_failure": stats["fail"] > 0}


def _curl_text(resp) -> str:
    text = getattr(resp, "text", "") or ""
    if text:
        return text
    content = getattr(resp, "content", b"") or b""
    for enc in ("euc-kr", "cp949", "utf-8"):
        try:
            return content.decode(enc)
        except Exception:
            pass
    return content.decode("utf-8", errors="replace")


def login_baekseok(session) -> None:
    if curl_requests is None:
        raise RuntimeError("curl_cffi is required for Baekseok unified login")

    user_id = os.environ.get("GYS_ID")
    user_pw = os.environ.get("GYS_PW")
    if not user_id or not user_pw:
        raise RuntimeError("Set env vars GYS_ID / GYS_PW for baekseok login")

    returl = f"{BAEKSEOK_BASE}/member/login.php?preURL=%2Frent%2Ftennis_rent.php%3Fpart_opt%3D{BAEKSEOK_PART_OPT}"
    login_url = f"https://yeyak.gys.or.kr/fmcs/27?referer={quote(returl, safe='')}&login_check=skip"

    r0 = session.get(login_url, timeout=_gys_timeout(), verify=False)
    html0 = _curl_text(r0)
    soup = BeautifulSoup(html0, "lxml")
    form = soup.select_one("form#memberLoginForm")
    if not form:
        raise RuntimeError("Baekseok unified login form not found")

    payload: Dict[str, str] = {}
    for inp in form.select("input[name]"):
        name = inp.get("name")
        if name:
            payload[name] = inp.get("value", "")
    payload["userId"] = user_id
    payload["userPassword"] = user_pw

    post_url = urljoin(getattr(r0, "url", login_url), form.get("action") or "")
    r1 = session.post(post_url, data=payload, timeout=_gys_timeout(), verify=False, allow_redirects=True)
    html1 = _curl_text(r1)
    soup1 = BeautifulSoup(html1, "lxml")

    if soup1.select_one("form#memberLoginForm"):
        raise RuntimeError("Baekseok unified login failed")

    sso_form = soup1.select_one("form#form_sso_process")
    if not sso_form:
        raise RuntimeError("Baekseok SSO form not found after unified login")

    sso_payload: Dict[str, str] = {}
    for inp in sso_form.select("input[name]"):
        name = inp.get("name")
        if name:
            sso_payload[name] = inp.get("value", "")

    sso_url = sso_form.get("action") or ""
    session.post(
        sso_url,
        data=sso_payload,
        timeout=_gys_timeout(),
        verify=False,
        allow_redirects=True,
        headers={"Origin": "https://yeyak.gys.or.kr", "Referer": getattr(r1, "url", login_url)},
    )


def _baekseok_post(session, payload: Dict[str, str]):
    return session.post(
        BAEKSEOK_RENT,
        data=payload,
        timeout=_gys_timeout(),
        verify=False,
        allow_redirects=True,
        headers={"Origin": BAEKSEOK_BASE, "Referer": f"{BAEKSEOK_RENT}?part_opt={BAEKSEOK_PART_OPT}"},
    )


def _baekseok_is_login_required(html: str, final_url: str = "") -> bool:
    return "member/login.php" in (final_url or "") or "회원전용" in html or "로그인후 이용" in html


def crawl_baekseok() -> dict:
    cutoff_passed, dates_ymd = build_date_range_kst(cutoff_day=25, cutoff_hour=10, cutoff_minute=0)
    now = kst_now()
    print(f"[BAEKSEOK] KST now={now:%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates_ymd)}")

    facility_id = "gy-baekseok"
    facilities = {facility_id: {"title": "백석 테니스장", "location": "고양시", "_court_numbers": [], "_court_labels": {}}}
    availability: Dict[str, Dict[str, List[dict]]] = {facility_id: {}}
    stats = {"total": 0, "ok": 0, "empty": 0, "fail": 0, "login_required": 0}

    if curl_requests is None:
        print("[BAEKSEOK][WARN] curl_cffi unavailable; skip")
        print("[BAEKSEOK][STAT] total=0 ok=0 empty=0 fail=1 login_required=0")
        return {"facilities": facilities, "availability": availability, "partial_failure": True}

    session = curl_requests.Session(impersonate="chrome")
    session.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8"})

    login_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            login_baekseok(session)
            login_error = None
            break
        except Exception as e:
            login_error = e
            if attempt < 3:
                print(f"[BAEKSEOK][RETRY] login attempt={attempt} err={e}")
                session = curl_requests.Session(impersonate="chrome")
                session.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8"})

    if login_error is not None:
        stats["fail"] += 1
        stats["login_required"] += 1
        print(f"[BAEKSEOK][ERR] login failed: {login_error}")
        print(
            f"[BAEKSEOK][STAT] total={stats['total']} ok={stats['ok']} empty={stats['empty']} "
            f"fail={stats['fail']} login_required={stats['login_required']}"
        )
        return {"facilities": facilities, "availability": availability, "partial_failure": True}

    discovered_places = []

    for ymd in dates_ymd:
        yyyymmdd = yyyymmdd_from_ymd(ymd)
        day_slots: List[dict] = []
        places_for_day = list(discovered_places) or [""]
        completed_places = set()

        for place_opt in places_for_day:
            if place_opt in completed_places:
                continue
            stats["total"] += 1
            try:
                payload = build_payload_gys(place_opt, yyyymmdd, part_opt=BAEKSEOK_PART_OPT)
                html = ""
                final_url = ""
                resp = _baekseok_post(session, payload)
                html = _curl_text(resp)
                final_url = getattr(resp, "url", "")

                if _baekseok_is_login_required(html, final_url):
                    login_baekseok(session)
                    resp = _baekseok_post(session, payload)
                    html = _curl_text(resp)
                    final_url = getattr(resp, "url", "")

                if _baekseok_is_login_required(html, final_url):
                    stats["login_required"] += 1
                    raise RuntimeError(f"baekseok login required after retry. final_url={final_url}")

                soup = validate_gys_page(html, resp.status_code, yyyymmdd, allow_selection=not place_opt)
                for value, label in _gys_places(soup).items():
                    number = re.search(r"\d+\s*코트", label)
                    facilities[facility_id]["_court_labels"].setdefault(value, re.sub(r"\s", "", number.group()) if number else label)
                    if value and value not in discovered_places:
                        discovered_places.append(value)
                        # Visit discovered courts on this date too.
                        if value not in places_for_day:
                            places_for_day.append(value)

                selected_place = _gys_selected_place(soup)
                if not place_opt and not selected_place and discovered_places:
                    # This is the location picker, not a sold-out court.
                    continue
                actual_place = selected_place or place_opt or "1"
                if place_opt and selected_place and selected_place != place_opt:
                    raise ValueError(f"Baekseok court mismatch requested={place_opt} received={selected_place}")
                if actual_place in completed_places:
                    continue
                completed_places.add(actual_place)
                if actual_place not in facilities[facility_id]["_court_numbers"]:
                    facilities[facility_id]["_court_numbers"].append(actual_place)

                slots = parse_slots_daehwa(html)
                for sl in slots:
                    sl["courtNo"] = actual_place
                    sl["reserveUrl"] = f"{BAEKSEOK_RENT}?part_opt={BAEKSEOK_PART_OPT}"

                if slots:
                    day_slots.extend(slots)
                    stats["ok"] += 1
                else:
                    stats["empty"] += 1
            except Exception as e:
                stats["fail"] += 1
                print(f"[BAEKSEOK][ERR] date={ymd} place_opt={place_opt or '-'} err={e}")
                if "login required after retry" in str(e):
                    print("[BAEKSEOK][EARLY_ABORT] reservation page still requires login after SSO")
                    print(
                        f"[BAEKSEOK][STAT] total={stats['total']} ok={stats['ok']} empty={stats['empty']} "
                        f"fail={stats['fail']} login_required={stats['login_required']}"
                    )
                    return {"facilities": facilities, "availability": availability, "partial_failure": True}

        if stats["fail"]:
            break
        availability[facility_id][ymd] = day_slots

    print(
        f"[BAEKSEOK][STAT] total={stats['total']} ok={stats['ok']} empty={stats['empty']} "
        f"fail={stats['fail']} login_required={stats['login_required']}"
    )
    return {"facilities": facilities, "availability": availability, "partial_failure": stats["fail"] > 0}


def merge(a: dict, b: dict) -> dict:
    out = {
        "facilities": {**a.get("facilities", {}), **b.get("facilities", {})},
        "availability": {**a.get("availability", {}), **b.get("availability", {})},
    }
    return out


if __name__ == "__main__":
    out_gyt = crawl_gytennis()
    out_dae = crawl_daehwa()
    out_bae = crawl_baekseok()

    print(json.dumps(merge(merge(out_gyt, out_dae), out_bae), ensure_ascii=False, indent=2))
