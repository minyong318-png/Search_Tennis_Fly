import re
from datetime import date, timedelta
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
        "title": "농협물류단지공원 테니스장",
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
    match = re.search(r"(경기\s+[^\s]+시[^\n]*?|경기\s+[^\s]+군[^\n]*?)(?:예약문의|예약 안내|찾아오시는 길|$)", text)
    if not match:
        return ""
    return match.group(1).strip()[:120]


def _empty_daymap(days_ahead: int = 45) -> Dict[str, List[dict]]:
    start = date.today()
    return {(start + timedelta(days=offset)).strftime("%Y%m%d"): [] for offset in range(days_ahead + 1)}


def crawl_ggshare() -> Dict[str, Any]:
    session = _session()
    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, List[dict]]] = {}
    stats = {"total": len(FACILITIES), "ok": 0, "fail": 0}

    for item in FACILITIES:
        raw_id = f"{item['city']}-{item['facility_id']}-{item['insti_code']}"
        url = _view_url(item)
        title = item["title"]
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
            "title": title,
            "location": address or location,
            "reserveUrl": url,
            "source": "경기공유서비스",
            "blockedReason": "경기공유 예약 진입 단계는 CAPTCHA가 있어 잔여 시간 자동 조회는 보류",
        }
        availability[raw_id] = _empty_daymap()

    print(f"[GGSHARE][STATS] total={stats['total']} ok={stats['ok']} fail={stats['fail']}")
    return {"facilities": facilities, "availability": availability}


if __name__ == "__main__":
    crawl_ggshare()
