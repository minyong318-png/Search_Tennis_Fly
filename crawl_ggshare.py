import re
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://share.gg.go.kr"

FACILITIES = [
    {
        "city": "anseong",
        "label": "안성",
        "facility_id": "F0137",
        "insti_code": "1230001",
        "title": "고삼테니스장 1코트",
    },
    {
        "city": "anseong",
        "label": "안성",
        "facility_id": "F0142",
        "insti_code": "1230001",
        "title": "팜랜드 물류단지공원 테니스장",
    },
    {
        "city": "uijeongbu",
        "label": "의정부",
        "facility_id": "F0003",
        "insti_code": "1130004",
        "title": "모두의 운동장 테니스장 A코트",
    },
    {
        "city": "yangpyeong",
        "label": "양평",
        "facility_id": "F0004",
        "insti_code": "4170037",
        "title": "강하테니스장",
    },
]


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
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 tennis-availability-crawler",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return session


def _view_url(item: dict) -> str:
    return (
        f"{BASE_URL}/facilityListO/view"
        f"?eshare=1&facilityId={item['facility_id']}&instiCode={item['insti_code']}"
    )


def _strip_html(html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_address(text: str) -> str:
    match = re.search(r"(경기\s+[^\s]+(?:시|군)\s+[^\n]*?)(?:예약문의|예약 안내|찾아오시는 길|$)", text)
    return match.group(1).strip()[:120] if match else ""


def _empty_daymap(days_ahead: int = 45) -> Dict[str, List[dict]]:
    start = date.today()
    return {(start + timedelta(days=offset)).strftime("%Y%m%d"): [] for offset in range(days_ahead + 1)}


def _sort_daymap(daymap: Dict[str, List[dict]]) -> None:
    for slots in daymap.values():
        slots.sort(key=lambda slot: str(slot.get("timeContent") or ""))


def _crawl_browser(target_city: str = "") -> Dict[str, Any]:
    if os.getenv("GGSHARE_BROWSER", "1") == "0":
        raise RuntimeError("GGSHARE_BROWSER disabled")
    if not os.getenv("GGSHARE_ID") or not os.getenv("GGSHARE_PW"):
        raise RuntimeError("GGSHARE_ID/GGSHARE_PW env missing")
    script = Path(__file__).resolve().parent / "scripts" / "crawl_ggshare_slots.mjs"
    proc = subprocess.run(
        ["node", str(script), target_city],
        cwd=str(Path(__file__).resolve().parent),
        text=True,
        capture_output=True,
        timeout=int(os.getenv("GGSHARE_BROWSER_TIMEOUT", "240")),
        check=False,
    )
    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or "").strip().splitlines()[-1:]
        raise RuntimeError(message[0] if message else f"browser crawler failed: {proc.returncode}")
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    for daymap in (payload.get("availability") or {}).values():
        _sort_daymap(daymap)
    stats = payload.get("stats") or {}
    print(
        "[GGSHARE_BROWSER][STATS] "
        f"target={target_city or 'all'} ok={stats.get('ok', 0)} "
        f"fail={stats.get('fail', 0)} slots={stats.get('slots', 0)}"
    )
    return {"facilities": payload.get("facilities") or {}, "availability": payload.get("availability") or {}}


def crawl_ggshare(target_city: str = "") -> Dict[str, Any]:
    selected_city = (target_city or "").strip().lower()
    try:
        return _crawl_browser(selected_city)
    except Exception as exc:
        print(f"[GGSHARE_BROWSER][WARN] fallback to metadata-only crawl: {exc}", file=sys.stderr)

    items = [item for item in FACILITIES if not selected_city or item["city"] == selected_city]

    session = _session()
    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}
    stats = {"total": len(items), "ok": 0, "fail": 0}

    for item in items:
        raw_id = f"{item['city']}-{item['facility_id']}-{item['insti_code']}"
        url = _view_url(item)
        location = f"{item['label']}시" if item["city"] != "yangpyeong" else "양평군"
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            text = _strip_html(response.text)
            address = _extract_address(text)
            stats["ok"] += 1
        except Exception as exc:
            address = ""
            stats["fail"] += 1
            print(f"[GGSHARE][WARN] {raw_id} error={exc}")

        facilities[raw_id] = {
            "title": item["title"],
            "location": address or location,
            "reserveUrl": url,
            "source": "경기공유서비스",
            "blockedReason": "경기공유 예약 진입 단계 확인이 필요해 현재 잔여 슬롯 자동 조회는 보류",
        }
        availability[raw_id] = _empty_daymap()

    label = selected_city or "all"
    print(f"[GGSHARE][STATS] target={label} total={stats['total']} ok={stats['ok']} fail={stats['fail']}")
    return {"facilities": facilities, "availability": availability}


if __name__ == "__main__":
    crawl_ggshare()
