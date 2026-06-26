import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.aytennis.or.kr"
DAILY_URL = f"{BASE_URL}/daily"
COURTS = {
    1: "시청코트",
    2: "종합코트",
    3: "자유코트",
    4: "중앙코트",
    5: "호계코트",
}
TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[~\-]\s*(\d{1,2}:\d{2})")
LAST_PARTIAL_FAILURE = False
RESERVABLE_START_MINUTE = 7 * 60
RESERVABLE_END_MINUTE = 21 * 60


def _request_timeout() -> float:
    raw = (os.getenv("ANYANG_TIMEOUT") or "8").strip()
    try:
        return max(2.0, min(float(raw), 20.0))
    except ValueError:
        return 8.0


def _max_workers() -> int:
    raw = (os.getenv("ANYANG_MAX_WORKERS") or "8").strip()
    try:
        return max(1, min(int(raw), 16))
    except ValueError:
        return 8


def _session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=0.6,
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


def _normalize_time_label(text: str) -> str:
    label = " ".join((text or "").split()).strip()
    label = label.replace(" : ", ":").replace(": ", ":").replace(" :", ":")
    label = label.replace(" ~ ", "~").replace("~ ", "~").replace(" ~", "~")
    label = label.replace(" ", "")
    return label


def _time_to_minutes(value: str) -> int:
    hour, minute = value.split(":", 1)
    return int(hour) * 60 + int(minute)


def _is_reservable_time_label(label: str) -> bool:
    match = TIME_RE.search(label or "")
    if not match:
        return False
    start = _time_to_minutes(match.group(1))
    end = _time_to_minutes(match.group(2))
    return RESERVABLE_START_MINUTE <= start and end <= RESERVABLE_END_MINUTE


def parse_anyang_slots(html: str) -> Dict[str, List[dict]]:
    soup = BeautifulSoup(html, "lxml")

    time_labels: List[str] = []
    for td in soup.select("table.custom tr td.wide"):
        label = _normalize_time_label(td.get_text())
        if TIME_RE.search(label):
            time_labels.append(label)

    out: Dict[str, List[dict]] = {}
    for table in soup.select("table.innerCustom"):
        tag = table.select_one("td.courtTag")
        if not tag:
            continue
        match = re.match(r"\s*(\d+)", tag.get_text(" ", strip=True))
        if not match:
            continue
        court_no = match.group(1)

        for idx, row in enumerate(table.select("tr")[1:]):
            cell = row.select_one("td.resTag")
            if not cell:
                continue
            if not cell.select_one('input[type="checkbox"]:not([disabled])'):
                continue
            label = time_labels[idx] if idx < len(time_labels) else f"IDX:{idx}"
            if not _is_reservable_time_label(label):
                continue
            match_time = TIME_RE.search(label)
            if match_time:
                start, end = match_time.group(1), match_time.group(2)
                time_content = f"{start} ~ {end}"
                slot_key = f"{start}~{end}"
            else:
                time_content = label
                slot_key = label
            out.setdefault(court_no, []).append(
                {"timeContent": time_content, "slotKey": slot_key, "courtNo": court_no}
            )
    return out


def _extract_calendar_dates(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    dates: List[str] = []
    ensdat = soup.select_one("#ensdat")
    if ensdat:
        try:
            for item in json.loads(ensdat.get("value") or "[]"):
                ymd = str(item.get("date") or "").strip()
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ymd):
                    dates.append(ymd)
        except (TypeError, ValueError):
            pass
    for link in soup.select('a[href*="/daily/"]'):
        href = link.get("href") or ""
        match = re.search(r"/daily/\d+/(\d{4}-\d{2}-\d{2})", href)
        if match:
            dates.append(match.group(1))
    return sorted(dict.fromkeys(dates))


def _warmup(session: requests.Session) -> None:
    session.get(BASE_URL + "/", timeout=_request_timeout()).raise_for_status()
    session.get(DAILY_URL, timeout=_request_timeout(), headers={"Referer": BASE_URL + "/"}).raise_for_status()


def _fetch_day(court_value: int, ymd: str) -> tuple[int, str, Dict[str, List[dict]]]:
    session = _session()
    _warmup(session)
    url = f"{DAILY_URL}/{court_value}/{ymd}"
    response = session.get(url, timeout=_request_timeout(), headers={"Referer": f"{DAILY_URL}/{court_value}"})
    response.raise_for_status()
    html = _text(response)
    if "location.replace" in html and len(html) < 500:
        raise RuntimeError(f"redirect script returned for {url}")
    return court_value, ymd.replace("-", ""), parse_anyang_slots(html)


def crawl_anyang() -> Dict[str, Any]:
    global LAST_PARTIAL_FAILURE
    started_at = time.perf_counter()
    session = _session()
    _warmup(session)

    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}
    tasks: List[tuple[int, str]] = []

    for court_value, title in COURTS.items():
        response = session.get(
            f"{DAILY_URL}/{court_value}",
            timeout=_request_timeout(),
            headers={"Referer": DAILY_URL},
        )
        response.raise_for_status()
        html = _text(response)
        if "location.replace" in html and len(html) < 500:
            raise RuntimeError(f"redirect script returned for {DAILY_URL}/{court_value}")
        dates = _extract_calendar_dates(html) or [date.today().strftime("%Y-%m-%d")]
        for ymd in dates:
            tasks.append((court_value, ymd))
        for court_no in _court_numbers_from_html(html):
            raw_id = f"aytennis-{court_value}-{court_no}"
            facilities[raw_id] = {
                "title": f"{title} {court_no}코트",
                "location": "안양시",
                "reserveUrl": f"{DAILY_URL}/{court_value}",
            }
            availability[raw_id] = {d.replace("-", ""): [] for d in dates}

    stats = {"total": len(tasks), "ok": 0, "empty": 0, "fail": 0}
    with ThreadPoolExecutor(max_workers=_max_workers()) as executor:
        futures = {executor.submit(_fetch_day, cv, ymd): (cv, ymd) for cv, ymd in tasks}
        for future in as_completed(futures):
            court_value, ymd = futures[future]
            try:
                cv, yyyymmdd, slots_by_court = future.result()
                if slots_by_court:
                    stats["ok"] += 1
                else:
                    stats["empty"] += 1
                for court_no, slots in slots_by_court.items():
                    raw_id = f"aytennis-{cv}-{court_no}"
                    if raw_id not in facilities:
                        title = COURTS.get(cv, f"courtvalue {cv}")
                        facilities[raw_id] = {
                            "title": f"{title} {court_no}코트",
                            "location": "안양시",
                            "reserveUrl": f"{DAILY_URL}/{cv}",
                        }
                        availability[raw_id] = {}
                    for slot in slots:
                        slot["reserveUrl"] = f"{DAILY_URL}/{cv}"
                    availability[raw_id].setdefault(yyyymmdd, []).extend(slots)
            except Exception as exc:
                stats["fail"] += 1
                print(f"[ANYANG][WARN] courtvalue={court_value} date={ymd} error={exc}")

    slots = sum(len(items) for daymap in availability.values() for items in daymap.values())
    print(
        f"[ANYANG][STATS] total={stats['total']} ok={stats['ok']} empty={stats['empty']} "
        f"fail={stats['fail']} courts={len(facilities)} slots={slots}"
    )
    print(f"[ANYANG][ELAPSED] seconds={time.perf_counter() - started_at:.2f}")
    LAST_PARTIAL_FAILURE = stats["fail"] > 0
    return {"facilities": facilities, "availability": availability, "partial_failure": stats["fail"] > 0}


def _court_numbers_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    courts: List[str] = []
    for tag in soup.select("td.courtTag"):
        match = re.match(r"\s*(\d+)", tag.get_text(" ", strip=True))
        if match:
            courts.append(match.group(1))
    return sorted(dict.fromkeys(courts), key=lambda value: int(value))


if __name__ == "__main__":
    print(json.dumps(crawl_anyang(), ensure_ascii=False, indent=2))
