import aiohttp
import asyncio
import re
import requests
import threading
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import calendar
import os
import urllib3
from urllib.parse import parse_qs, urljoin, urlparse

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 테니스 시설 목록 endpoint
BASE_URL = "https://publicsports.yongin.go.kr/publicsports/sports/selectFcltyRceptResveListU.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL
}

def get_connector():
    return aiohttp.TCPConnector(limit=get_time_concurrency(), ssl=False)


def get_time_concurrency():
    try:
        value = int(os.getenv("YONGIN_TIME_CONCURRENCY", "50"))
    except ValueError:
        value = 50
    return max(1, min(value, 60))


def get_time_request_timeout():
    try:
        value = float(os.getenv("YONGIN_TIME_TIMEOUT", "4"))
    except ValueError:
        value = 4.0
    return max(2.0, min(value, 15.0))


def get_time_request_retries():
    try:
        value = int(os.getenv("YONGIN_TIME_RETRIES", "1"))
    except ValueError:
        value = 1
    return max(0, min(value, 4))


CRAWL_STATS = {
    "facility_list_failed": 0,
    "time_ok": 0,
    "time_empty": 0,
    "time_failed": 0,
    "time_retried": 0,
}

RESERVATION_TYPE_CODES = ("RESIDENTRESVE", "CITIZENRESVE", "GNRLRESVE")
RESERVATION_TYPE_LABELS = {
    "RESIDENTRESVE": "구민우선",
    "CITIZENRESVE": "시민우선",
    "GNRLRESVE": "일반예약",
    "district_priority": "구민우선",
    "city_priority": "시민우선",
    "general": "일반예약",
}


def normalize_reservation_type(value):
    text = str(value or "").strip().replace(" ", "")
    if text in {"district_priority", "city_priority", "general", "unknown"}:
        return text
    if text in {"RESIDENTRESVE", "구민우선", "구민예약", "구민"}:
        return "district_priority"
    if text in {"CITIZENRESVE", "시민우선", "시민예약", "시민"}:
        return "city_priority"
    if text in {"GNRLRESVE", "일반예약", "일반"}:
        return "general"
    if "구민" in text:
        return "district_priority"
    if "시민" in text:
        return "city_priority"
    if "일반" in text:
        return "general"
    return "unknown"


def reservation_type_label(value, fallback=""):
    normalized = normalize_reservation_type(value)
    return RESERVATION_TYPE_LABELS.get(normalized) or RESERVATION_TYPE_LABELS.get(str(value or "").strip()) or str(fallback or "유형 확인 필요")


def normalize_application_status(value):
    text = str(value or "").strip().replace(" ", "").lower()
    if text in {"open", "closed", "not_open", "unknown"}:
        return text
    if not text:
        return "unknown"
    if any(token in text for token in ("접수마감", "예약마감", "마감", "closed", "full")):
        return "closed"
    if any(token in text for token in ("접수예정", "접수대기", "접수전", "예약예정", "예정", "notopen")):
        return "not_open"
    if any(token in text for token in ("접수중", "예약가능", "예약중", "open")):
        return "open"
    return "unknown"


def _period_range(text):
    values = re.findall(r"(\d{4}[./-]\d{1,2}[./-]\d{1,2})", str(text or ""))
    if len(values) < 2:
        return None, None
    normalized = []
    for value in values[:2]:
        year, month, day = re.split(r"[./-]", value)
        normalized.append(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
    return tuple(normalized)


def _query_reservation_type(href):
    try:
        return parse_qs(urlparse(href or "").query).get("searchResveType", [""])[0]
    except (AttributeError, ValueError):
        return ""


def _numeric_value(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value or "").strip()
    return float(text) if re.fullmatch(r"-?\d+(?:\.\d+)?", text) else None


def is_publishable_slot(slot):
    """Reject explicit unavailable/zero slots while preserving statusless source slots."""
    if not isinstance(slot, dict):
        return bool(str(slot or "").strip())
    available = slot.get("available")
    if available is False or available == 0 or str(available or "").strip().lower() in {"false", "no", "n", "0"}:
        return False
    for key in ("remaining", "remainingCount", "remain", "remainCount", "availableCount"):
        if key in slot:
            remaining = _numeric_value(slot.get(key))
            if remaining is None or remaining <= 0:
                return False
    status = str(slot.get("status") or slot.get("statusText") or slot.get("state") or "").strip().lower().replace(" ", "")
    if status and any(token in status for token in ("예약불가", "예약마감", "접수마감", "unavailable", "closed", "full", "reserved")):
        return False
    return bool(
        slot.get("timeContent")
        or slot.get("time")
        or slot.get("label")
        or slot.get("startTime")
        or slot.get("start_time")
        or slot.get("stime")
    )


def filter_publishable_slots(slots):
    return [slot for slot in (slots or []) if is_publishable_slot(slot)]

_thread_local = threading.local()


def _requests_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        _thread_local.session = session
    return session


# --------------------------------------------------------------
# ★ 자동 쿠키 갱신: 첫 요청에서 서버가 내려주는 쿠키를 session에 저장
# --------------------------------------------------------------
async def init_session(session):
    async with session.get(BASE_URL, params={"pageIndex":1}) as resp:
        set_cookie = resp.cookies.get("JSESSIONID")
        if set_cookie:
            session.cookie_jar.update_cookies({"JSESSIONID": set_cookie.value})
            print("[INFO] New JSESSIONID:", set_cookie.value)
        else:
            print("[WARN] 서버에서 쿠키를 내려주지 않음")


# --------------------------------------------------------------
# HTML 요청
# --------------------------------------------------------------
async def fetch_html(session, url, params=None):
    try:
        async with session.get(url, params=params) as resp:
            return await resp.text()
    except Exception as e:
        print("[ERROR] fetch_html:", e)
        return ""


# --------------------------------------------------------------
# 시설 HTML 파싱
# --------------------------------------------------------------
def parse_facility_html(html, reservation_type_code=""):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("li.reserve_box_item")
    results = {}

    for li in items:
        a = li.select_one("div.btn_wrap a[href*='selectFcltyRceptResveViewU.do']")
        if not a:
            continue

        href = a.get("href", "")
        m = re.search(r"resveId=(\d+)", href)
        if not m:
            continue

        rid = m.group(1)
        title_div = li.select_one("div.reserve_title")
        if not title_div:
            continue
        pos_div = title_div.select_one("div.reserve_position")

        location = pos_div.get_text(strip=True) if pos_div else ""
        if pos_div:
            pos_div.extract()

        title = title_div.get_text(" ", strip=True)
        type_node = li.select_one(".above_item")
        type_label = type_node.get_text(" ", strip=True) if type_node else ""
        type_code = reservation_type_code or _query_reservation_type(href)
        state_node = li.select_one(".reserve_state")
        state_label = state_node.get_text(" ", strip=True) if state_node else ""
        application_start = application_end = use_start = use_end = ""
        for item in li.select(".reserve_con > ul.bu li, ul.bu li"):
            text = item.get_text(" ", strip=True)
            if "접수기간" in text:
                application_start, application_end = _period_range(text)
            elif "이용기간" in text:
                use_start, use_end = _period_range(text)
        card_type = normalize_reservation_type(type_label)
        type_hint = type_label if card_type != "unknown" else (type_code or _query_reservation_type(href))
        reservation_type = normalize_reservation_type(type_hint)
        results[rid] = {
            "title": title,
            "location": location,
            "reserveUrl": urljoin(BASE_URL, href),
            "sourceUrl": urljoin(BASE_URL, href),
            "reservationType": reservation_type,
            "reservationTypeLabel": reservation_type_label(type_hint or reservation_type, type_label),
            "applicationStatus": normalize_application_status(state_label),
            "applicationStatusLabel": state_label,
            "applicationStartDate": application_start or "",
            "applicationEndDate": application_end or "",
            "useStartDate": use_start or "",
            "useEndDate": use_end or "",
        }

    return results


def attach_reserve_url(daymap, reserve_url):
    for slots in (daymap or {}).values():
        for slot in slots or []:
            if isinstance(slot, dict):
                slot.setdefault("reserveUrl", reserve_url)


# --------------------------------------------------------------
# ① 테니스 시설 전체 페이지 크롤링
# --------------------------------------------------------------
async def fetch_facilities(session):

    facilities = {}
    failed_requests = 0
    base_params = {
        "searchFcltyFieldNm": "ITEM_01",  # ★ 테니스 필터
        "pageUnit": 20,
        "pageIndex": 1,
        "checkSearchMonthNow": "false",
    }

    for reservation_type_code in RESERVATION_TYPE_CODES:
        type_params = dict(base_params)
        type_params["searchResveType"] = reservation_type_code
        html = await fetch_html(session, BASE_URL, params=type_params)
        if not html:
            failed_requests += 1
            print(f"[WARN] 시설 목록 조회 실패 type={reservation_type_code}")
            continue

        page_indices = re.findall(r"pageIndex=(\d+)", html)
        max_page = max(int(p) for p in page_indices) if page_indices else 1
        facilities.update(parse_facility_html(html, reservation_type_code))

        tasks = []
        for page in range(2, max_page + 1):
            params2 = dict(type_params)
            params2["pageIndex"] = page
            tasks.append(fetch_html(session, BASE_URL, params=params2))
        pages_html = await asyncio.gather(*tasks)
        for page_html in pages_html:
            if page_html:
                facilities.update(parse_facility_html(page_html, reservation_type_code))
            else:
                failed_requests += 1
        print(f"[INFO] 시설 목록 type={reservation_type_code} pages={max_page}")

    CRAWL_STATS["facility_list_failed"] = failed_requests
    return facilities


# --------------------------------------------------------------
# ② 날짜별 시간 조회
# --------------------------------------------------------------
async def fetch_times(session, date_val, rid, sem=None):
    attempts = get_time_request_retries() + 1

    for attempt in range(attempts):
        try:
            if sem:
                async with sem:
                    return await asyncio.to_thread(fetch_times_sync, date_val, rid)
            return await asyncio.to_thread(fetch_times_sync, date_val, rid)
        except Exception:
            if attempt < attempts - 1:
                CRAWL_STATS["time_retried"] += 1
                await asyncio.sleep(0.15 * (attempt + 1))
                continue
            return None
    return None


def fetch_times_sync(date_val, rid):
    url = "https://publicsports.yongin.go.kr/publicsports/sports/selectRegistTimeByChosenDateFcltyRceptResveApply.do"
    response = _requests_session().post(
        url,
        data={"dateVal": date_val, "resveId": rid},
        timeout=get_time_request_timeout(),
        verify=False,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or "resveTmList" not in payload:
        raise ValueError("reservation time response is missing resveTmList")
    times = payload["resveTmList"]
    if times is None:
        return []
    if not isinstance(times, list):
        raise ValueError("reservation time response has invalid resveTmList")
    return times


def get_facility_month(title):
    m = re.search(r"_(\d{1,2})월\s*$", title or "")
    if not m:
        return None
    month = int(m.group(1))
    return month if 1 <= month <= 12 else None


def build_target_dates(title, start):
    facility_month = get_facility_month(title)

    y, m = start.year, start.month
    current_dates = [
        f"{y}{m:02d}{d:02d}"
        for d in range(start.day, calendar.monthrange(y, m)[1] + 1)
    ]

    next_dt = start.replace(day=1) + timedelta(days=32)
    ny, nm = next_dt.year, next_dt.month
    next_dates = [
        f"{ny}{nm:02d}{d:02d}"
        for d in range(1, calendar.monthrange(ny, nm)[1] + 1)
    ]

    if facility_month == m:
        return current_dates
    if facility_month == nm:
        return next_dates
    return current_dates + next_dates


# --------------------------------------------------------------
# ③ 오늘 ~ 다음달 끝까지
# --------------------------------------------------------------
async def fetch_availability(session, rid, title="", sem=None, facility_meta=None, start_date=None):
    start = start_date or datetime.today()
    result = {
        "_checked_dates": [],
        "_failed_dates": [],
        "_availability_status_by_date": {},
    }
    target_dates = build_target_dates(title, start)
    application_status = normalize_application_status(
        (facility_meta or {}).get("applicationStatus")
        or (facility_meta or {}).get("application_status")
        or (facility_meta or {}).get("applicationStatusLabel")
        or (facility_meta or {}).get("application_status_label")
    )
    if application_status in {"closed", "not_open"}:
        for date_val in target_dates:
            result[date_val] = []
            result["_checked_dates"].append(date_val)
            result["_availability_status_by_date"][date_val] = application_status
        return result
    tasks = [fetch_times(session, date_val, rid, sem) for date_val in target_dates]

    times_list = await asyncio.gather(*tasks)

    for date_val, times in zip(target_dates, times_list):
        if times is None:
            result["_failed_dates"].append(date_val)
            continue
        result["_checked_dates"].append(date_val)
        filtered_times = filter_publishable_slots(times)
        if filtered_times:
            CRAWL_STATS["time_ok"] += 1
            result[date_val] = filtered_times
            result["_availability_status_by_date"][date_val] = "available"
        else:
            CRAWL_STATS["time_empty"] += 1
            result[date_val] = []
            result["_availability_status_by_date"][date_val] = "confirmed_empty"

    return result


# --------------------------------------------------------------
# 전체 실행
# --------------------------------------------------------------
async def run_all_async():
    for key in CRAWL_STATS:
        CRAWL_STATS[key] = 0

    async with aiohttp.ClientSession(
        connector=get_connector(),
        headers=HEADERS
    ) as session:

        # ★ 1) 세션 시작 → 자동 쿠키 갱신
        await init_session(session)

        # ★ 2) 전체 테니스 시설 크롤링
        facilities = await fetch_facilities(session)

        # ★ 3) 각 시설 날짜 데이터 병렬 처리
        sem = asyncio.Semaphore(get_time_concurrency())
        request_count = sum(
            len(build_target_dates(meta.get("title", ""), datetime.today()))
            for meta in facilities.values()
        )
        print(
            f"[INFO] Yongin time requests={request_count} "
            f"concurrency={get_time_concurrency()} timeout={get_time_request_timeout():.1f}s "
            f"retries={get_time_request_retries()}"
        )
        tasks = [
            fetch_availability(session, rid, meta.get("title", ""), sem, meta)
            for rid, meta in facilities.items()
        ]
        results = await asyncio.gather(*tasks)

        failed_dates = sum(len(data.get("_failed_dates", [])) for data in results if data)
        CRAWL_STATS["time_failed"] = failed_dates
        availability = {}
        for (rid, _), data in zip(facilities.items(), results):
            if not data:
                continue
            clean_data = {k: v for k, v in data.items() if not str(k).startswith("_")}
            facility_meta = dict(facilities[rid])
            facility_meta["_checked_dates"] = list(data.get("_checked_dates") or [])
            facility_meta["_failed_dates"] = list(data.get("_failed_dates") or [])
            facility_meta["_availability_status_by_date"] = dict(data.get("_availability_status_by_date") or {})
            observed_dates = data.get("_checked_dates") or data.get("_failed_dates") or []
            facility_meta["_scan_window"] = {
                "start": min(observed_dates) if observed_dates else "",
                "end": max(observed_dates) if observed_dates else "",
            }
            facilities[rid] = facility_meta
            if clean_data:
                attach_reserve_url(clean_data, facility_meta.get("reserveUrl", BASE_URL))
                availability[rid] = clean_data

        print(
            "[YONGIN][STATS] "
            f"facilities={len(facilities)} with_slots={len(availability)} "
            f"ok_dates={CRAWL_STATS['time_ok']} empty_dates={CRAWL_STATS['time_empty']} "
            f"failed_dates={CRAWL_STATS['time_failed']} retried={CRAWL_STATS['time_retried']}"
        )

        return facilities, availability


def run_all():
    return asyncio.run(run_all_async())
