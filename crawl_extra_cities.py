import html
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


HANAM_BASE = "https://www.hanam.go.kr"
HANAM_SPORTS_BASE = "https://rent.hanamsport.or.kr/hanam_rent_ms"
UIWANG_BASE = "https://reserve.uuc.or.kr"
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


def _date_values(days_ahead: int = 45) -> List[date]:
    start = date.today()
    return [start + timedelta(days=offset) for offset in range(days_ahead + 1)]


def _month_values(months_ahead: int = 1) -> List[str]:
    months: List[str] = []
    current = date.today().replace(day=1)
    for _ in range(months_ahead + 1):
        months.append(current.strftime("%Y-%m"))
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def _yyyymm_values(months_ahead: int = 1) -> List[str]:
    return [month.replace("-", "") for month in _month_values(months_ahead)]


def _merge_daymap(dst: Dict[str, List[dict]], src: Dict[str, List[dict]]) -> None:
    for ymd, slots in src.items():
        dst.setdefault(ymd, []).extend(slots)


def _sort_daymap(daymap: Dict[str, List[dict]]) -> None:
    for slots in daymap.values():
        slots.sort(key=lambda slot: slot.get("slotKey") or "")


def _slot_from_title(title: str, reserve_url: str, court_no: str = "") -> tuple[str, dict] | None:
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
    _sort_daymap(out)
    return out


def _parse_hanam_day(html_text: str, reserve_url: str, court_no: str) -> Dict[str, List[dict]]:
    soup = BeautifulSoup(html_text, "lxml")
    out: Dict[str, List[dict]] = {}
    for row in soup.select("#dynamicTbody tr"):
        cells = row.select("td")
        if len(cells) < 3:
            continue
        if "예약하기" not in cells[2].get_text(" ", strip=True):
            continue
        ymd = re.sub(r"\D", "", cells[0].get_text(" ", strip=True))
        time_text = " ".join(cells[1].get_text(" ", strip=True).split())
        match = re.search(r"(\d{1,2}:\d{2})\s*[~\-]\s*(\d{1,2}:\d{2})", time_text)
        if not ymd or not match:
            continue
        start, end = match.group(1), match.group(2)
        out.setdefault(ymd, []).append(
            {
                "timeContent": f"{start} ~ {end}",
                "slotKey": f"{start}~{end}",
                "courtNo": court_no,
                "reserveUrl": reserve_url,
            }
        )
    _sort_daymap(out)
    return out


def _fetch_hanam_day(court_code: str, court_no: str, day: date) -> tuple[str, Dict[str, List[dict]]]:
    session = _session()
    ymd = day.strftime("%Y%m%d")
    yyyymm = day.strftime("%Y%m")
    url = (
        f"{HANAM_BASE}/www/selectMisaParkResveWeb.do"
        f"?key=7465&yyyymm={yyyymm}&misaParkCode={court_code}"
        f"&searchCategoryCode=B1&searchResveDate={ymd}"
    )
    response = session.get(url, timeout=_timeout())
    response.raise_for_status()
    return court_code, _parse_hanam_day(_text(response), url, court_no)


def _parse_hanam_sports_payload(payload: dict, ymd: str, reserve_url: str) -> Dict[str, Dict[str, List[dict]]]:
    out: Dict[str, Dict[str, List[dict]]] = {}
    try:
        courts = json.loads(payload.get("play_name") or "[]")
    except (TypeError, ValueError):
        courts = []
    for item in courts if isinstance(courts, list) else []:
        place_code = str(item.get("place_code") or "").strip()
        play_code = str(item.get("play_code") or "").strip()
        event_code = str(item.get("event_code") or "").strip()
        raw_court = str(item.get("play_name") or "").strip()
        court_no = re.sub(r"\D+", "", raw_court) or raw_court or play_code
        fid = f"sports-center-{place_code}-{play_code}"
        soup = BeautifulSoup(str(item.get("htmlx") or ""), "lxml")
        for block in soup.select("div.chk_d"):
            checkbox = block.select_one('input[name="ct_chk[]"]')
            if not checkbox or checkbox.has_attr("disabled"):
                continue
            time_node = block.select_one(".chk_t")
            match = re.search(r"(\d{1,2}:\d{2})\s*[~\-]\s*(\d{1,2}:\d{2})", time_node.get_text(" ", strip=True) if time_node else "")
            if not match:
                continue
            start, end = match.group(1), match.group(2)
            slot = {
                "timeContent": f"{start} ~ {end}",
                "slotKey": f"{start}~{end}",
                "courtNo": court_no,
                "reserveUrl": reserve_url,
            }
            if place_code and event_code and play_code:
                slot["courtKey"] = f"{place_code}{event_code}{play_code}"
            out.setdefault(fid, {}).setdefault(ymd, []).append(slot)
    for daymap in out.values():
        _sort_daymap(daymap)
    return out


def _fetch_hanam_sports_day(place_code: str, day: date) -> tuple[str, Dict[str, Dict[str, List[dict]]]]:
    session = _session()
    ymd = day.strftime("%Y%m%d")
    reserve_url = f"{HANAM_SPORTS_BASE}/?place_code={place_code}"
    session.get(reserve_url, timeout=_timeout()).raise_for_status()
    response = session.post(
        f"{HANAM_SPORTS_BASE}/center/ajax.day.rent.list.php",
        data={
            "center_id": "01",
            "rdate": ymd,
            "rent_open_start_day": "10",
            "place_code": place_code,
            "Rstep": "",
        },
        timeout=_timeout(),
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": reserve_url,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if str(payload.get("rstate") or "") == "9":
        raise RuntimeError(str(payload.get("error") or "invalid access"))
    return place_code, _parse_hanam_sports_payload(payload, ymd, reserve_url)


def crawl_hanam() -> Dict[str, Any]:
    courts = {"TS01": "1", "TS02": "2", "TS03": "3", "TS04": "4"}
    facilities = {
        f"misa-hangang-{court_no}": {
            "title": f"미사한강공원 {court_no}코트",
            "location": "하남시",
            "reserveUrl": f"{HANAM_BASE}/www/selectMisaParkResveWeb.do?key=7465&yyyymm={date.today().strftime('%Y%m')}&misaParkCode={code}&searchCategoryCode=B1",
        }
        for code, court_no in courts.items()
    }
    facilities["hanam-sports-center"] = {
        "title": "하남국민체육센터",
        "location": "하남시",
        "reserveUrl": "https://rent.hanamsport.or.kr/hanam_rent_ms/",
        "blockedReason": "온라인대관서비스 화면 구조 분석 후 슬롯 파싱 예정",
    }
    facilities.pop("hanam-sports-center", None)
    sports_places = {
        "024": "\ud558\ub0a8\uad6d\ubbfc\uccb4\uc721\uc13c\ud130 \uc81c1\ud14c\ub2c8\uc2a4\uc7a5",
        "025": "\ud558\ub0a8\uad6d\ubbfc\uccb4\uc721\uc13c\ud130 \uc81c2\ud14c\ub2c8\uc2a4\uc7a5",
    }
    for place_code, title in sports_places.items():
        for idx in range(1, 9):
            play_code = f"{idx:02d}"
            facilities[f"sports-center-{place_code}-{play_code}"] = {
                "title": f"{title} {idx}\uba74",
                "location": "\ud558\ub0a8\uc2dc",
                "reserveUrl": f"{HANAM_SPORTS_BASE}/?place_code={place_code}",
            }
    availability = {fid: _empty_daymap() for fid in facilities}
    tasks = [(code, court_no, day) for code, court_no in courts.items() for day in _date_values()]
    stats = {"ok": 0, "fail": 0}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_hanam_day, code, court_no, day): (code, court_no, day) for code, court_no, day in tasks}
        for future in as_completed(futures):
            code, court_no, day = futures[future]
            try:
                _code, daymap = future.result()
                _merge_daymap(availability[f"misa-hangang-{court_no}"], daymap)
                stats["ok"] += 1
            except Exception as exc:
                stats["fail"] += 1
                print(f"[HANAM][WARN] code={code} date={day:%Y%m%d} error={exc}")
    sports_tasks = [(place_code, day) for place_code in sports_places for day in _date_values()]
    sports_stats = {"ok": 0, "fail": 0}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_fetch_hanam_sports_day, place_code, day): (place_code, day) for place_code, day in sports_tasks}
        for future in as_completed(futures):
            place_code, day = futures[future]
            try:
                _place_code, facility_daymaps = future.result()
                for fid, daymap in facility_daymaps.items():
                    if fid in availability:
                        _merge_daymap(availability[fid], daymap)
                sports_stats["ok"] += 1
            except Exception as exc:
                sports_stats["fail"] += 1
                print(f"[HANAM][WARN] sports_place={place_code} date={day:%Y%m%d} error={exc}")
    for daymap in availability.values():
        _sort_daymap(daymap)
    slot_count = sum(len(slots) for daymap in availability.values() for slots in daymap.values())
    print(
        f"[HANAM][STATS] facilities={len(facilities)} slots={slot_count} "
        f"misa_ok={stats['ok']} misa_fail={stats['fail']} "
        f"sports_ok={sports_stats['ok']} sports_fail={sports_stats['fail']}"
    )
    return {"facilities": facilities, "availability": availability}


def crawl_uiwang() -> Dict[str, Any]:
    session = _session()
    min_ymd = date.today().strftime("%Y%m%d")
    max_ymd = (date.today() + timedelta(days=45)).strftime("%Y%m%d")
    places = [
        ("30", "내손테니스장", "내손"),
        ("31", "고천테니스장", "고천"),
        ("32", "부곡테니스장", "부곡"),
        ("35", "산빛테니스장", "산빛"),
        ("36", "오전테니스장", "오전"),
        ("43", "의왕테니스장 1코트", "1"),
        ("44", "의왕테니스장 2코트", "2"),
        ("45", "의왕테니스장 3코트", "3"),
        ("46", "의왕테니스장 4코트", "4"),
    ]
    facilities = {
        f"uuc-{place_cd}": {
            "title": title,
            "location": "의왕시",
            "reserveUrl": f"{UIWANG_BASE}/fmcs/4?facilities_type=L&base_date={date.today().strftime('%Y%m%d')}&rent_type=1001&center=UUC02&part=15&place={place_cd}",
        }
        for place_cd, title, _court_no in places
    }
    facilities.update(
        {
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
    )
    availability = {fid: _empty_daymap() for fid in facilities}
    stats = {"ok": 0, "fail": 0}
    for place_cd, _title, court_no in places:
        fid = f"uuc-{place_cd}"
        for yyyymm in _yyyymm_values(1):
            url = (
                f"{UIWANG_BASE}/rest/facilities/place_month_time_state_list"
                f"?company_code=UUC02&part_code=15&place_code={place_cd}"
                f"&base_date={yyyymm}01&rent_type=1001&mem_no="
            )
            try:
                response = session.get(
                    url,
                    timeout=_timeout(),
                    headers={
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{UIWANG_BASE}/fmcs/4?facilities_type=L&rent_type=1001&center=UUC02&part=15&place={place_cd}",
                    },
                )
                response.raise_for_status()
                rows = response.json()
                for row in rows if isinstance(rows, list) else []:
                    ymd = str(row.get("date") or "").replace("-", "")
                    if not re.fullmatch(r"\d{8}", ymd):
                        continue
                    if ymd < min_ymd or ymd > max_ymd:
                        continue
                    if int(row.get("rent_no") or 0) != 0:
                        continue
                    if str(row.get("use_yn") or "").upper() not in ("N", "Y"):
                        continue
                    start = str(row.get("start_time") or "").strip()
                    end = str(row.get("end_time") or "").strip()
                    time_no = str(row.get("time_no") or "").strip()
                    time_nm = str(row.get("time_nm") or "").strip()
                    if not start or not end or not time_no:
                        continue
                    time_value = quote(f"{time_no};{time_nm};{start.replace(':', '')};{end.replace(':', '')};1")
                    reserve_url = (
                        f"{UIWANG_BASE}/fmcs/4?facilities_type=L&rent_type=1001&center=UUC02&part=15&base_date={ymd}"
                        f"&action=write&place={place_cd}&comcd=UUC02&part_cd=15&place_cd={place_cd}"
                        f"&time_no={time_value}&rent_date={ymd}"
                    )
                    availability[fid].setdefault(ymd, []).append(
                        {
                            "timeContent": f"{start} ~ {end}",
                            "slotKey": f"{start}~{end}",
                            "courtNo": court_no,
                            "reserveUrl": reserve_url,
                        }
                    )
                stats["ok"] += 1
            except Exception as exc:
                stats["fail"] += 1
                print(f"[UIWANG][WARN] place={place_cd} month={yyyymm} error={exc}")
    for daymap in availability.values():
        _sort_daymap(daymap)
    try:
        import crawl_ggshare

        out_gg = crawl_ggshare.crawl_ggshare("uiwang")
        for raw_fid, meta in (out_gg.get("facilities") or {}).items():
            facilities[f"ggshare-{raw_fid}"] = meta
        for raw_fid, daymap in (out_gg.get("availability") or {}).items():
            availability[f"ggshare-{raw_fid}"] = daymap or {}
    except Exception as exc:
        print(f"[UIWANG][GGSHARE][WARN] {exc}")
    slot_count = sum(len(slots) for daymap in availability.values() for slots in daymap.values())
    print(f"[UIWANG][STATS] facilities={len(facilities)} slots={slot_count} ok={stats['ok']} fail={stats['fail']}")
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
        _sort_daymap(daymap)
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
        _sort_daymap(daymap)
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
