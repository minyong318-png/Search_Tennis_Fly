import html
import os
import re
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


JMPSS_BASE = "https://www.jmpss.or.kr"
YSFSMC_BASE = "https://www.ysfsmc.or.kr"

TITLE_RE = re.compile(
    r"(?P<year>\d{4})년\s*(?P<month>\d{1,2})월\s*(?P<day>\d{1,2})일\s*"
    r"(?P<start>\d{1,2}:\d{2})\s*~\s*(?P<end>\d{1,2}:\d{2})"
)


def _timeout() -> float:
    raw = (os.getenv("EXTRA_CITY_TIMEOUT") or "12").strip()
    try:
        return max(3.0, min(float(raw), 30.0))
    except ValueError:
        return 12.0


def _session() -> requests.Session:
    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 tennis-availability-crawler",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def _text(response: requests.Response) -> str:
    if not response.encoding or response.encoding.lower() in ("iso-8859-1", "ascii"):
        response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def _empty_daymap(days_ahead: int = 45) -> Dict[str, List[dict]]:
    start = date.today()
    return {(start + timedelta(days=offset)).strftime("%Y%m%d"): [] for offset in range(days_ahead + 1)}


def _month_values(months_ahead: int = 1) -> List[str]:
    months: List[str] = []
    current = date.today().replace(day=1)
    for _ in range(months_ahead + 1):
        months.append(current.strftime("%Y-%m"))
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def _slot_from_title(title: str, reserve_url: str, court_no: str = "") -> dict | None:
    match = TITLE_RE.search(html.unescape(title or ""))
    if not match:
        return None
    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day"))
    start = match.group("start")
    end = match.group("end")
    slot = {
        "timeContent": f"{start} ~ {end}",
        "slotKey": f"{start}~{end}",
        "reserveUrl": reserve_url,
    }
    if court_no:
        slot["courtNo"] = court_no
    return f"{year:04d}{month:02d}{day:02d}", slot


def _merge_daymap(dst: Dict[str, List[dict]], src: Dict[str, List[dict]]) -> None:
    for ymd, slots in src.items():
        dst.setdefault(ymd, []).extend(slots)


def _parse_reserve_links(html_text: str, base_url: str, court_no: str = "") -> Dict[str, List[dict]]:
    soup = BeautifulSoup(html_text, "lxml")
    out: Dict[str, List[dict]] = {}
    for link in soup.select("a[title]"):
        if "예약하기" not in link.get_text(" ", strip=True):
            continue
        parsed = _slot_from_title(link.get("title") or "", urljoin(base_url, link.get("href") or ""), court_no)
        if not parsed:
            continue
        ymd, slot = parsed
        out.setdefault(ymd, []).append(slot)
    for slots in out.values():
        slots.sort(key=lambda slot: slot.get("slotKey") or "")
    return out


def crawl_hanam() -> Dict[str, Any]:
    facilities = {
        "misa-hangang-2": {
            "title": "미사한강공원 2코트",
            "location": "하남시",
            "reserveUrl": "https://www.hanam.go.kr/www/selectMisaParkResveWeb.do?key=7465&yyyymm=202607&misaParkCode=TS02&searchCategoryCode=B1",
            "blockedReason": "하남시 미사한강공원은 날짜별 시간 조회 endpoint 추가 분석 후 슬롯 파싱 예정",
        },
        "hanam-sports-center": {
            "title": "하남국민체육센터",
            "location": "하남시",
            "reserveUrl": "https://rent.hanamsport.or.kr/hanam_rent_ms/",
            "blockedReason": "온라인대관서비스 화면 구조 분석 후 슬롯 파싱 예정",
        },
    }
    availability = {fid: _empty_daymap() for fid in facilities}
    print(f"[HANAM][STATS] facilities={len(facilities)} slots=0 pending_parser=2")
    return {"facilities": facilities, "availability": availability}


def crawl_uiwang() -> Dict[str, Any]:
    facilities = {
        "uuc-tennis": {
            "title": "의왕도시공사 테니스장",
            "location": "의왕시",
            "reserveUrl": "https://reserve.uuc.or.kr/fmcs/4?facilities_type=C&base_date=20260705&center=UUC02&part=15&place=30",
            "blockedReason": "FMCS 대관현황 상세 파라미터 확인 후 내손/고천/부곡/산빛/오전/의왕 코트 분리 예정",
        },
        "ggshare-cheonggye-soft": {
            "title": "청계소프트테니스장",
            "location": "의왕시",
            "reserveUrl": "https://share.gg.go.kr/facilityListS11?searchArea=4143&searchType=S1_1&searchType2=%ED%85%8C%EB%8B%88%EC%8A%A4%EC%9E%A5",
            "blockedReason": "경기공유 상세 시설 ID 확인 및 CAPTCHA 경계 확인 필요",
        },
        "ggshare-cheonggye-park": {
            "title": "청계체육공원 테니스장",
            "location": "의왕시",
            "reserveUrl": "https://share.gg.go.kr/facilityListS11?searchArea=4143&searchType=S1_1&searchType2=%ED%85%8C%EB%8B%88%EC%8A%A4%EC%9E%A5",
            "blockedReason": "경기공유 상세 시설 ID 확인 및 CAPTCHA 경계 확인 필요",
        },
    }
    availability = {fid: _empty_daymap() for fid in facilities}
    print(f"[UIWANG][STATS] facilities={len(facilities)} slots=0 pending_parser=3")
    return {"facilities": facilities, "availability": availability}


def _crawl_jmpss(session: requests.Session, months: Iterable[str]) -> tuple[Dict[str, dict], Dict[str, Dict[str, List[dict]]]]:
    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}
    for seq in range(1, 10):
        court_no = str(seq)
        title = f"제물포구 테니스장 {court_no}코트"
        fid = f"jmpss-{seq}"
        facilities[fid] = {
            "title": title,
            "location": "인천시 제물포구",
            "reserveUrl": f"{JMPSS_BASE}/main/business/tennis/reservation_{seq:02d}.jsp",
        }
        daymap: Dict[str, List[dict]] = _empty_daymap()
        for month in months:
            url = f"{JMPSS_BASE}/tennis/tennisScheduleMonth.do?rental_seq={seq}&sch_ym={month}"
            try:
                response = session.get(url, timeout=_timeout(), headers={"Referer": facilities[fid]["reserveUrl"]})
                response.raise_for_status()
                _merge_daymap(daymap, _parse_reserve_links(_text(response), JMPSS_BASE, court_no))
            except Exception as exc:
                print(f"[INCHEON][JMPSS][WARN] seq={seq} month={month} error={exc}")
        availability[fid] = daymap
    return facilities, availability


def _crawl_ysfsmc(session: requests.Session, months: Iterable[str]) -> tuple[Dict[str, dict], Dict[str, Dict[str, List[dict]]]]:
    pages = [
        (1, "문화공원 A코트", "/business/culture/park_tennis2.jsp"),
        (2, "문화공원 B코트", "/business/culture/park_tennis2_2.jsp"),
        (3, "문화공원 C코트", "/business/culture/park_tennis2_3.jsp"),
        (4, "연수체육공원 A코트", "/business/culture/park_tennis2_4.jsp"),
        (5, "연수체육공원 B코트", "/business/culture/park_tennis2_5.jsp"),
    ]
    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}
    for seq, title, path in pages:
        fid = f"ysfsmc-{seq}"
        url = urljoin(YSFSMC_BASE, path)
        facilities[fid] = {"title": title, "location": "인천시 연수구", "reserveUrl": url}
        daymap: Dict[str, List[dict]] = _empty_daymap()
        tennis_seq = str(seq)
        for month in months:
            month_url = f"{YSFSMC_BASE}/tennis/tennisScheduleMonth.do?tennis_seq={tennis_seq}&sch_ym={month}"
            try:
                response = session.get(month_url, timeout=_timeout(), headers={"Referer": url})
                response.raise_for_status()
                _merge_daymap(daymap, _parse_reserve_links(_text(response), YSFSMC_BASE, title.rsplit(" ", 1)[-1].replace("코트", "")))
            except Exception as exc:
                print(f"[INCHEON][YSFSMC][WARN] seq={seq} month={month} error={exc}")
        availability[fid] = daymap
    return facilities, availability


def crawl_incheon() -> Dict[str, Any]:
    session = _session()
    months = _month_values(1)
    facilities: Dict[str, dict] = {
        "insiseol-skyculture": {
            "title": "하늘문화센터",
            "location": "인천시",
            "reserveUrl": "https://res.insiseol.or.kr/rent/rentalSchedule?up_id=08",
            "blockedReason": "인천시설공단 통합예약 세부 대관 페이지 분석 필요",
        },
        "insiseol-songdo": {
            "title": "송도공원사업단",
            "location": "인천시",
            "reserveUrl": "https://res.insiseol.or.kr/rent/rentalSchedule?up_id=07",
            "blockedReason": "인천시설공단 통합예약 세부 대관 페이지 분석 필요",
        },
        "insiseol-gongchon": {
            "title": "공촌유수지 체육시설",
            "location": "인천시",
            "reserveUrl": "https://res.insiseol.or.kr/rent/rentalSchedule?up_id=09",
            "blockedReason": "인천시설공단 통합예약 세부 대관 페이지 분석 필요",
        },
        "gajwa": {
            "title": "가좌테니스장",
            "location": "인천시",
            "reserveUrl": "https://www.icsports.or.kr/fmcs/6?action=list&facilities_type=C&base_date=20260801&center=ICGYM&type=&part=02&place=18",
            "blockedReason": "인천광역시체육회 FMCS 대관 목록 상세 파라미터 분석 필요",
        },
        "michuhol-hagik": {
            "title": "학익배수지",
            "location": "인천시 미추홀구",
            "reserveUrl": "https://tongtongtong.kr/page/?pid=hiringArena2_new&cs_idx=22&cote=1",
            "blockedReason": "통통통 대관 목록 상세 파라미터 분석 필요",
        },
        "michuhol-yonghyeon": {
            "title": "용현학익1블럭 임시체육시설",
            "location": "인천시 미추홀구",
            "reserveUrl": "https://tongtongtong.kr/page/?pid=hiringArena2_new&cs_idx=22&cote=1",
            "blockedReason": "통통통 대관 목록 상세 파라미터 분석 필요",
        },
    }
    availability: Dict[str, Dict[str, List[dict]]] = {fid: _empty_daymap() for fid in facilities}

    j_fac, j_av = _crawl_jmpss(session, months)
    y_fac, y_av = _crawl_ysfsmc(session, months)
    facilities.update(j_fac)
    facilities.update(y_fac)
    availability.update(j_av)
    availability.update(y_av)

    slot_count = sum(len(slots) for daymap in availability.values() for slots in daymap.values())
    print(f"[INCHEON][STATS] facilities={len(facilities)} slots={slot_count} months={','.join(months)}")
    return {"facilities": facilities, "availability": availability}
