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
    "time_ok": 0,
    "time_empty": 0,
    "time_failed": 0,
    "time_retried": 0,
}

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
def parse_facility_html(html):
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
        pos_div = title_div.select_one("div.reserve_position")

        location = pos_div.get_text(strip=True) if pos_div else ""
        if pos_div:
            pos_div.extract()

        title = title_div.get_text(strip=True)

        results[rid] = {"title": title, "location": location}

    return results


# --------------------------------------------------------------
# ① 테니스 시설 전체 페이지 크롤링
# --------------------------------------------------------------
async def fetch_facilities(session):

    facilities = {}

    base_params = {
        "searchFcltyFieldNm": "ITEM_01",  # ★ 테니스 필터
        "pageUnit": 20,
        "pageIndex": 1,
        "checkSearchMonthNow": "false"
    }

    # 1) 첫 페이지 요청 + 자동 쿠키 갱신 적용됨
    html = await fetch_html(session, BASE_URL, params=base_params)
    if not html:
        print("[ERROR] 첫 페이지 가져오기 실패")
        return facilities

    # 2) pageIndex=숫자 전체 추출 → 마지막 페이지 파악
    page_indices = re.findall(r"pageIndex=(\d+)", html)
    max_page = max(int(p) for p in page_indices) if page_indices else 1

    print(f"[INFO] 총 페이지 수: {max_page}")

    # 3) 첫 페이지 파싱
    facilities.update(parse_facility_html(html))

    # 4) 나머지 페이지 병렬 요청
    tasks = []
    for page in range(2, max_page + 1):
        params2 = dict(base_params)
        params2["pageIndex"] = page
        tasks.append(fetch_html(session, BASE_URL, params=params2))

    pages_html = await asyncio.gather(*tasks)
    for html in pages_html:
        if html:
            facilities.update(parse_facility_html(html))

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
    return response.json().get("resveTmList", [])


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
# ③ 내일 ~ 다음달 끝까지
# --------------------------------------------------------------
async def fetch_availability(session, rid, title="", sem=None):
    today = datetime.today()
    start = today + timedelta(days=1)  # ★ 오늘 제외

    result = {}
    target_dates = build_target_dates(title, start)
    tasks = [fetch_times(session, date_val, rid, sem) for date_val in target_dates]

    times_list = await asyncio.gather(*tasks)

    for date_val, times in zip(target_dates, times_list):
        if times is None:
            result.setdefault("_failed_dates", []).append(date_val)
            continue
        if times:
            CRAWL_STATS["time_ok"] += 1
            result[date_val] = times
        else:
            CRAWL_STATS["time_empty"] += 1

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
            len(build_target_dates(meta.get("title", ""), datetime.today() + timedelta(days=1)))
            for meta in facilities.values()
        )
        print(
            f"[INFO] Yongin time requests={request_count} "
            f"concurrency={get_time_concurrency()} timeout={get_time_request_timeout():.1f}s "
            f"retries={get_time_request_retries()}"
        )
        tasks = [
            fetch_availability(session, rid, meta.get("title", ""), sem)
            for rid, meta in facilities.items()
        ]
        results = await asyncio.gather(*tasks)

        failed_dates = sum(len(data.get("_failed_dates", [])) for data in results if data)
        CRAWL_STATS["time_failed"] = failed_dates
        availability = {}
        for (rid, _), data in zip(facilities.items(), results):
            if not data:
                continue
            clean_data = {k: v for k, v in data.items() if k != "_failed_dates"}
            if clean_data:
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
