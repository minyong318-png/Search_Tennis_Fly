import re
from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://res.isdc.co.kr"
LIST_URL = f"{BASE_URL}/facilityList.do?facType=29"


def _session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "Mozilla/5.0 tennis-availability-crawler"})
    return session


def crawl_seongnam() -> Dict[str, Any]:
    session = _session()
    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, list]] = {}
    total = ok = empty = fail = 0

    try:
        response = session.get(LIST_URL, timeout=20)
        response.raise_for_status()
        group_ids = sorted(set(re.findall(r'name=["\']groupId["\']\s+value=["\'](\d+)["\']', response.text)))
    except Exception as exc:
        print(f"[SEONGNAM][WARN] facility list error={exc}")
        print("[SEONGNAM][STATS] total=0 ok=0 empty=0 fail=1 courts=0 login_required=1")
        return {"facilities": {}, "availability": {}, "login_required": True}

    for group_id in group_ids:
        total += 1
        try:
            response = session.post(f"{BASE_URL}/tennisList.do", data={"groupId": group_id}, timeout=20)
            response.raise_for_status()
            items = re.findall(
                r'<li\s+id=["\'](FAC\d+)["\'][^>]*class=["\']facilityInfo["\'].*?'
                r'<div\s+class=["\']head-area["\']>\s*(.*?)\s*</div>',
                response.text,
                re.I | re.S,
            )
            if not items:
                empty += 1
                continue
            ok += 1
            for fac_id, raw_title in items:
                title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", raw_title)).strip()
                facilities[fac_id] = {
                    "title": title,
                    "location": "성남시",
                    "reserveUrl": LIST_URL,
                    "availabilityStatus": "login_required",
                }
                availability[fac_id] = {}
        except Exception as exc:
            fail += 1
            print(f"[SEONGNAM][WARN] group={group_id} error={exc}")

    login_required = True
    try:
        check = session.post(f"{BASE_URL}/checkOpenTime_Tennis.do", timeout=20)
        login_required = check.text.strip() == "session"
    except Exception as exc:
        print(f"[SEONGNAM][WARN] login check error={exc}")

    print(
        f"[SEONGNAM][STATS] total={total} ok={ok} empty={empty} fail={fail} "
        f"courts={len(facilities)} login_required={int(login_required)}"
    )
    if login_required:
        print("[SEONGNAM][INFO] public court list collected; availability requires login")
    return {
        "facilities": facilities,
        "availability": availability,
        "login_required": login_required,
    }


if __name__ == "__main__":
    crawl_seongnam()
