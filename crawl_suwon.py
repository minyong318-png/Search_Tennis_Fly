import re
import os
from datetime import date, timedelta
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CALENDAR_URL = "https://mpsports.or.kr/base/facility/calendar1"
RESERVE_URL = (
    "https://mpsports.or.kr/base/facility/calendar1"
    "?facilityCategoryNo=1&menuLevel=2&menuNo=20"
)


def _session() -> requests.Session:
    retry = Retry(
        total=2,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "Mozilla/5.0 tennis-availability-crawler"})
    return session


def _request_timeout() -> float:
    try:
        value = float(os.getenv("SUWON_TIMEOUT", "8"))
    except ValueError:
        value = 8.0
    return max(2.0, min(value, 20.0))


def _month_starts(start: date, end: date) -> List[date]:
    current = start.replace(day=1)
    months = []
    while current <= end:
        months.append(current)
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def _text(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _parse_month(html: str, year: int, month: int, start: date, end: date) -> Dict[str, List[dict]]:
    daymap: Dict[str, List[dict]] = {}
    chunks = re.split(r'<td\s+class=["\']day["\']\s*>', html, flags=re.I)
    for chunk in chunks[1:]:
        match = re.search(r'<span\s+class=["\']day["\']>\s*(\d{1,2})\s*</span>', chunk, re.I)
        if not match:
            continue
        try:
            current = date(year, month, int(match.group(1)))
        except ValueError:
            continue
        if current < start or current > end:
            continue

        slots = []
        for body in re.findall(r'<a\s+class=["\']c1["\'][^>]*>(.*?)</a>', chunk, re.I | re.S):
            label = _text(body)
            if re.match(r"^\d{2}:\d{2}\s*\(", label):
                slots.append({"timeContent": label, "reserveUrl": RESERVE_URL})
        daymap[current.strftime("%Y%m%d")] = slots
    return daymap


def crawl_suwon(days_ahead: int = 45) -> Dict[str, Any]:
    start = date.today()
    end = start + timedelta(days=days_ahead)
    daymap: Dict[str, List[dict]] = {}
    total = ok = empty = fail = 0

    session = _session()
    for month_start in _month_starts(start, end):
        total += 1
        try:
            response = session.get(
                CALENDAR_URL,
                params={
                    "yearMonthDay": month_start.strftime("%Y-%m-%d"),
                    "menuLevel": "2",
                    "menuNo": "20",
                    "facilityCategoryNo": "1",
                },
                timeout=_request_timeout(),
            )
            response.raise_for_status()
            parsed = _parse_month(response.text, month_start.year, month_start.month, start, end)
            daymap.update(parsed)
            if any(parsed.values()):
                ok += 1
            else:
                empty += 1
        except Exception as exc:
            fail += 1
            print(f"[SUWON][WARN] month={month_start:%Y-%m} error={exc}")

    slots = sum(len(items) for items in daymap.values())
    print(f"[SUWON][STATS] total={total} ok={ok} empty={empty} fail={fail} slots={slots}")
    print("[SUWON][INFO] share.gg.go.kr timetable skipped: login and NetFunnel required")
    return {
        "facilities": {
            "mpsports": {
                "title": "망포복합체육센터 테니스장",
                "location": "수원시 영통구",
            }
        },
        "availability": {"mpsports": daymap},
    }


if __name__ == "__main__":
    crawl_suwon()
