import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import calendar
import os

# 테니스 시설 목록 endpoint
BASE_URL = "https://publicsports.yongin.go.kr/publicsports/sports/selectFcltyRceptResveListU.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL
}

def get_connector():
    return aiohttp.TCPConnector(limit=60, ssl=False)


def get_time_concurrency():
    try:
        value = int(os.getenv("YONGIN_TIME_CONCURRENCY", "40"))
    except ValueError:
        value = 40
    return max(1, min(value, 80))


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
    url = "https://publicsports.yongin.go.kr/publicsports/sports/selectRegistTimeByChosenDateFcltyRceptResveApply.do"
    data = {"dateVal": date_val, "resveId": rid}

    try:
        if sem:
            async with sem:
                async with session.post(url, data=data) as resp:
                    j = await resp.json()
                    return j.get("resveTmList", [])
        async with session.post(url, data=data) as resp:
            j = await resp.json()
            return j.get("resveTmList", [])
    except:
        return []


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
        if times:
            result[date_val] = times

    return result


# --------------------------------------------------------------
# 전체 실행
# --------------------------------------------------------------
async def run_all_async():
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
        print(f"[INFO] Yongin time requests={request_count} concurrency={get_time_concurrency()}")
        tasks = [
            fetch_availability(session, rid, meta.get("title", ""), sem)
            for rid, meta in facilities.items()
        ]
        results = await asyncio.gather(*tasks)

        availability = {
            rid: data
            for (rid, _), data in zip(facilities.items(), results)
            if data
        }

        return facilities, availability


def run_all():
    return asyncio.run(run_all_async())
