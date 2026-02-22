# goyang_combined_crawler.py
# - requests + bs4만 사용
# - (1) gytennis.or.kr (daily/{courtvalue}/{yyyy-mm-dd}) 조회
# - (2) 성저(대화) https://daehwa.gys.or.kr:451 성저(대화) 테니스장 조회
# - 백석 관련 코드는 전부 제거됨
#
# 조회 기간 규칙(KST):
# - gytennis: 매월 25일 22:00 이후면 익월까지, 아니면 이번달까지만
# - 성저(대화): 매월 25일 10:00 이후면 익월까지, 아니면 이번달까지만

import os
import re
import json
import calendar
import datetime as dt
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

UA = "Mozilla/5.0 (compatible; GoyangCombinedCrawler/1.0)"
TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[~\-]\s*(\d{1,2}:\d{2})")

KST = dt.timezone(dt.timedelta(hours=9))


# ============================================================
# KST 날짜 범위 유틸
# ============================================================
def kst_now() -> dt.datetime:
    return dt.datetime.now(tz=KST)


def build_date_range_kst(
    cutoff_day: int,
    cutoff_hour: int,
    cutoff_minute: int = 0,
    start_date: dt.date | None = None,
) -> Tuple[bool, List[str]]:
    """
    KST 기준:
      - cutoff_day cutoff_hour:cutoff_minute 이후면: (start) ~ (익월 말)까지
      - 그 이전이면: (start) ~ (이번달 말)까지
    start는 기본: 오늘
    반환: (cutoff_passed, ["YYYY-MM-DD", ...])
    """
    now = kst_now()
    start = start_date or now.date()

    cutoff_passed = (
        (now.day > cutoff_day)
        or (now.day == cutoff_day and (now.hour > cutoff_hour or (now.hour == cutoff_hour and now.minute >= cutoff_minute)))
    )

    if cutoff_passed:
        # next month end
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
    # "YYYY-MM-DD" -> "YYYYMMDD"
    return ymd.replace("-", "")


def yyyymmdd_parts(yyyymmdd: str) -> Tuple[str, str, str]:
    return yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:8]


# ============================================================
# 공통 세션/인코딩
# ============================================================
def make_session() -> requests.Session:
    s = requests.Session()
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


# ============================================================
# (A) gytennis.or.kr 파서/크롤러
# ============================================================
GYT_BASE = "https://www.gytennis.or.kr/daily"

# courtvalue -> 이름(네가 올린 메뉴 기준)
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
    # "06 :00 ~ 08 :00" -> "06:00~08:00"
    s = " ".join(text.split()).strip()
    s = s.replace(" : ", ":").replace(": ", ":").replace(" :", ":")
    s = s.replace(" ~ ", "~").replace("~ ", "~").replace(" ~", "~")
    s = s.replace(" ", "")
    return s


def parse_gytennis_slots(html: str) -> Dict[str, List[dict]]:
    """
    반환:
      { "1": [slot, ...], "2": [...], ... }
    slot = { timeContent, slotKey, courtNo }
    """
    soup = BeautifulSoup(html, "lxml")

    # 왼쪽 시간 라벨: table.custom td.wide
    time_labels: List[str] = []
    for td in soup.select("table.custom tr td.wide"):
        time_labels.append(_normalize_time_label(td.get_text()))

    # 각 코트: table.innerCustom
    out: Dict[str, List[dict]] = {}

    for tbl in soup.select("table.innerCustom"):
        tag = tbl.select_one("td.courtTag")
        if not tag:
            continue

        # "1 코트" -> "1"
        court_text = " ".join(tag.get_text().split())
        m = re.match(r"(\d+)", court_text)
        if not m:
            continue
        court_no = m.group(1)

        rows = tbl.select("tr")[1:]  # 첫 행은 헤더
        for idx, tr in enumerate(rows):
            td = tr.select_one("td.resTag")
            if not td:
                continue

            # 예약불가(disabled) 제외
            if td.select_one("input[disabled]") is not None:
                continue

            # ✅ 네가 확인한 방식: tooltip-trigger가 있으면 "이미 예약/정보 있음"으로 보고 제외
            has_tooltip = td.select_one(".ctooltip-trigger") is not None
            if has_tooltip:
                continue

            # 여기까지 오면 "빈칸 = 잔여"
            label = time_labels[idx] if idx < len(time_labels) else f"IDX:{idx}"
            # label: "20:00~22:00"
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


def fetch_gytennis_day(session: requests.Session, courtvalue: int, ymd: str) -> Dict[str, List[dict]]:
    url = f"{GYT_BASE}/{courtvalue}/{ymd}"
    r = session.get(
        url,
        timeout=20,
        headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"},
    )
    if r.status_code != 200:
        return {}
    html = fix_encoding(r)
    return parse_gytennis_slots(html)


def crawl_gytennis() -> dict:
    """
    gytennis는 courtvalue(1~10) 각각을 facility로 취급
    availability: facility_id -> date -> [slots...]
    """
    cutoff_passed, dates = build_date_range_kst(cutoff_day=25, cutoff_hour=22, cutoff_minute=0)
    now = kst_now()
    print(f"[GYT] KST now={now:%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates)}")

    s = make_session()

    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}

    for cv in range(1, 11):
        fid = f"gy-gytennis-{cv}"
        facilities[fid] = {"title": f"고양특례시테니스협회 {GYT_NAME.get(cv, f'courtvalue {cv}')}", "location": "고양시", "courtvalue": cv}
        availability[fid] = {}

    for ymd in dates:
        for cv in range(1, 11):
            fid = f"gy-gytennis-{cv}"
            court_slots = fetch_gytennis_day(s, cv, ymd)  # {"1":[...], "2":[...]}
            if not court_slots:
                continue
            # flat list로 합치기
            flat: List[dict] = []
            for _, slots in court_slots.items():
                flat.extend(slots)
            if flat:
                availability[fid].setdefault(ymd, []).extend(flat)

    return {"facilities": facilities, "availability": availability}


# ============================================================
# (B) 성저(대화.gys) 크롤러 (백석 제거 버전)
# ============================================================
DAEHWA_BASE = "https://daehwa.gys.or.kr:451"
DAEHWA_LOGIN = DAEHWA_BASE + "/member/login.php?preURL=%2Frent%2Ftennis_rent.php"
DAEHWA_RENT = DAEHWA_BASE + "/rent/tennis_rent.php"

# 성저(대화) 코트 매핑
DAEHWA_PLACE = {1: "2", 2: "7", 3: "8", 4: "9"}


def is_login_page(html: str, final_url: str = "") -> bool:
    if final_url and "member/login.php" in final_url:
        return True
    soup = BeautifulSoup(html, "lxml")
    return soup.select_one('input[type="password"]') is not None


def login_daehwa(s: requests.Session) -> None:
    user_id = os.environ.get("GYS_ID")
    user_pw = os.environ.get("GYS_PW")
    if not user_id or not user_pw:
        raise RuntimeError("Set env vars GYS_ID / GYS_PW for daehwa login")

    r0 = s.get(DAEHWA_LOGIN, verify=False, allow_redirects=True, timeout=20)
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

    # id/pw field 추정
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

    r1 = s.post(
        post_url,
        data=payload,
        verify=False,
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
        "place_opt": str(place_opt),  # ✅ 성저는 Form Data
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

        txt = re.sub(r"\s+", " ", tr.get_text(" ", strip=True)).strip()
        m = TIME_RE.search(txt)
        if not m:
            continue

        start = m.group(1).zfill(5)
        end = m.group(2).zfill(5)

        out.append(
            {
                "timeContent": f"{start} ~ {end}",
                "slotKey": f"{start}~{end}",
                "rent_chk": val,
            }
        )

    return out


def post_rent(s: requests.Session, payload: Dict[str, str]) -> Tuple[str, str, int]:
    r = s.post(
        DAEHWA_RENT,
        data=payload,
        verify=False,
        allow_redirects=True,
        timeout=25,
        headers={"Origin": DAEHWA_BASE, "Referer": DAEHWA_RENT},
    )
    html = fix_encoding(r)
    return html, r.url, r.status_code


def crawl_daehwa() -> dict:
    """
    성저(대화)는 하나의 facility로 유지(네 기존 구조 유지)
    날짜 범위: 25일 10:00 컷오프
    """
    cutoff_passed, dates_ymd = build_date_range_kst(cutoff_day=25, cutoff_hour=10, cutoff_minute=0)
    now = kst_now()
    print(f"[DAEHWA] KST now={now:%Y-%m-%d %H:%M} cutoffPassed={cutoff_passed} dates={len(dates_ymd)}")

    s = make_session()
    login_daehwa(s)

    facility_id = "gy-daehwa"
    facilities = {facility_id: {"title": "고양 성저(대화) 테니스장", "location": "고양시"}}
    availability: Dict[str, Dict[str, List[dict]]] = {facility_id: {}}

    for ymd in dates_ymd:
        yyyymmdd = yyyymmdd_from_ymd(ymd)

        day_slots: List[dict] = []
        for court_no, place_opt in DAEHWA_PLACE.items():
            payload = build_payload_daehwa(place_opt, yyyymmdd)
            html, final_url, status = post_rent(s, payload)

            if is_login_page(html, final_url):
                # 세션 만료 대비 1회 재로그인
                login_daehwa(s)
                html, final_url, status = post_rent(s, payload)

            if is_login_page(html, final_url):
                raise RuntimeError(f"daehwa login required after retry. final_url={final_url}")

            slots = parse_slots_daehwa(html)
            for sl in slots:
                sl["courtNo"] = str(court_no)
            if slots:
                day_slots.extend(slots)

        if day_slots:
            availability[facility_id].setdefault(ymd, []).extend(day_slots)

    return {"facilities": facilities, "availability": availability}


# ============================================================
# merge + main
# ============================================================
def merge(a: dict, b: dict) -> dict:
    # availability는 facility_id 단위로 합치기
    out = {
        "facilities": {**a.get("facilities", {}), **b.get("facilities", {})},
        "availability": {**a.get("availability", {}), **b.get("availability", {})},
    }
    return out


if __name__ == "__main__":
    # 성저(대화) 크롤링은 로그인 필요:
    # export GYS_ID="..."
    # export GYS_PW="..."
    out_gyt = crawl_gytennis()
    out_dae = crawl_daehwa()

    print(json.dumps(merge(out_gyt, out_dae), ensure_ascii=False, indent=2))