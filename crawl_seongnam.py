import argparse
import json
import os
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://res.isdc.co.kr"
LIST_URL = f"{BASE_URL}/facilityList.do?facType=29"
_thread_local = threading.local()
_last_auth_error_type = ""

ANDROID_FAILURE_ERROR_TYPES = {
    "login_required": "LoginRequired",
    "automation_blocked": "AutomationBlocked",
    "android_device_offline": "AndroidDeviceOffline",
    "android_unauthorized": "AndroidUnauthorized",
    "android_cdp_unavailable": "AndroidCdpUnavailable",
    "chrome_tab_not_found": "ChromeTabNotFound",
    "network_capture_failed": "NetworkCaptureFailed",
    "parse_failed": "ParseFailed",
}


def _session() -> requests.Session:
    retry = Retry(
        total=2,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "Mozilla/5.0 tennis-availability-crawler"})
    return session


def _request_timeout() -> float:
    raw = (os.getenv("SEONGNAM_TIMEOUT") or "8").strip()
    try:
        return max(2.0, min(float(raw), 20.0))
    except ValueError:
        return 8.0


def _storage_state_path() -> str:
    return os.getenv("SEONGNAM_STORAGE_STATE") or os.path.join(
        os.getcwd(), ".cache", "seongnam_storage_state.json"
    )


def _remove_file_quietly(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        print(f"[SEONGNAM][AUTH] storage cleanup failed path={path} error={exc}")


def _has_credentials() -> bool:
    return bool((os.getenv("ISDC_ID") or "").strip() and (os.getenv("ISDC_PW") or ""))


def _collector_mode(explicit: str | None = None) -> str:
    raw = (explicit or os.getenv("SEONGNAM_COLLECTOR_MODE") or "auto").strip().lower()
    return raw if raw in {"auto", "android", "playwright"} else "auto"


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value or "")).strip()


def _login(session: requests.Session) -> bool:
    user_id = (os.getenv("ISDC_ID") or "").strip()
    password = os.getenv("ISDC_PW") or ""
    if not user_id or not password:
        print("[SEONGNAM][AUTH] credentials missing")
        return False

    try:
        session.get(f"{BASE_URL}/login.do", timeout=_request_timeout()).raise_for_status()
        response = session.post(
            f"{BASE_URL}/rest_loginCheck.do",
            data={"web_id": user_id, "web_pw": password},
            timeout=_request_timeout(),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[SEONGNAM][AUTH] login request failed error={exc}")
        return False
    success = response.text.strip() == "success"
    if success:
        session.get(LIST_URL, timeout=_request_timeout()).raise_for_status()
    print(f"[SEONGNAM][AUTH] login={'ok' if success else 'fail'}")
    return success


def _apply_storage_state(session: requests.Session, path: str | None = None) -> bool:
    state_path = path or _storage_state_path()
    try:
        with open(state_path, "r", encoding="utf-8") as fp:
            state = json.load(fp)
    except (OSError, json.JSONDecodeError):
        return False

    loaded = 0
    for cookie in state.get("cookies") or []:
        domain = str(cookie.get("domain") or "")
        if "res.isdc.co.kr" not in domain:
            continue
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if not name:
            continue
        session.cookies.set(
            name,
            value,
            domain=domain.lstrip(".") or "res.isdc.co.kr",
            path=str(cookie.get("path") or "/"),
        )
        loaded += 1
    if loaded:
        print(f"[SEONGNAM][AUTH] storage_state=loaded cookies={loaded}")
    return loaded > 0


def _has_login_challenge(html: str) -> bool:
    lowered = (html or "").lower()
    return any(
        marker in lowered
        for marker in (
            "rest_logincheck.do",
            'name="web_id"',
            "name='web_id'",
            'id="web_id"',
            'name="web_pw"',
            "name='web_pw'",
            'id="web_pw"',
        )
    )


def _validate_logged_in_session(session: requests.Session) -> bool:
    try:
        response = session.get(LIST_URL, timeout=_request_timeout())
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[SEONGNAM][AUTH] storage_state=invalid error={exc}")
        return False
    valid = bool(re.search(r'name=["\']groupId["\']\s+value=["\']\d+', response.text)) and not _has_login_challenge(response.text)
    print(f"[SEONGNAM][AUTH] storage_state={'valid' if valid else 'invalid'}")
    return valid


def _run_playwright_login() -> bool:
    global _last_auth_error_type
    _last_auth_error_type = ""
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "seongnam_session.mjs")
    state_path = _storage_state_path()
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    timeout = int(max(30, min(_request_timeout() * 20, 180)))
    env = os.environ.copy()
    env["SEONGNAM_STORAGE_STATE"] = state_path
    try:
        completed = subprocess.run(
            ["node", script_path],
            cwd=os.path.dirname(__file__),
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"[SEONGNAM][AUTH] playwright login unavailable error={exc}")
        return False
    if completed.stdout.strip():
        print(completed.stdout.strip())
    if completed.returncode != 0:
        output = (completed.stderr or completed.stdout or "").strip()
        if "auto_detect.do" in output or "automation blocked" in output.lower():
            _last_auth_error_type = "AutomationBlocked"
        message = output.splitlines()[-1:]
        print(f"[SEONGNAM][AUTH] playwright login failed error={' '.join(message)}")
        return False
    return True


def _ensure_authenticated_session(session: requests.Session) -> bool:
    if _apply_storage_state(session) and _validate_logged_in_session(session):
        return True

    if not _has_credentials():
        return _login(session)

    _remove_file_quietly(_storage_state_path())
    if _run_playwright_login():
        session.cookies.clear()
        if _apply_storage_state(session) and _validate_logged_in_session(session):
            return True

    if _last_auth_error_type == "AutomationBlocked":
        return False

    return _login(session)


def _android_collector_timeout() -> int:
    raw_timeout = (os.getenv("SEONGNAM_ANDROID_RUN_TIMEOUT") or "").strip()
    if raw_timeout:
        try:
            return max(45, min(int(float(raw_timeout)), 3600))
        except ValueError:
            pass
    raw_deadline_ms = (os.getenv("SEONGNAM_ANDROID_TIMEOUT_MS") or "").strip()
    if raw_deadline_ms:
        try:
            return max(45, min(int(float(raw_deadline_ms) / 1000) + 60, 3600))
        except ValueError:
            pass
    return int(max(45, min(_request_timeout() * 30, 900)))


def _run_android_collector() -> dict:
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "seongnam_android_collector.mjs")
    try:
        completed = subprocess.run(
            ["node", script_path],
            cwd=os.path.dirname(__file__),
            env=os.environ.copy(),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=_android_collector_timeout(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "status": "android_cdp_unavailable",
            "message": f"Android collector unavailable: {exc}",
        }
    try:
        return json.loads((completed.stdout or "").strip())
    except json.JSONDecodeError:
        return {
            "status": "android_cdp_unavailable",
            "message": (completed.stderr or completed.stdout or "Android collector returned no JSON").strip()[-500:],
        }


def _android_error_result(payload: dict) -> Dict[str, Any]:
    status = str(payload.get("status") or "parse_failed")
    error_type = ANDROID_FAILURE_ERROR_TYPES.get(status, "ParseFailed")
    return {
        "facilities": {},
        "availability": {},
        "partial_failure": True,
        "login_required": status == "login_required",
        "automation_blocked": status == "automation_blocked",
        "error_type": error_type,
        "error_message": str(payload.get("message") or status),
        "android_status": status,
    }


def _normalize_android_result(payload: dict) -> Dict[str, Any]:
    if (payload or {}).get("status") != "ok":
        return _android_error_result(payload or {})

    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, list]] = {}
    for item in payload.get("facilities") or []:
        raw_id = str(item.get("id") or item.get("facilityId") or "").strip()
        if not raw_id:
            continue
        facilities[raw_id] = {
            "title": item.get("title") or raw_id,
            "location": item.get("location") or "성남",
            "reserveUrl": item.get("reserveUrl") or LIST_URL,
            "availabilityStatus": "android_chrome_cdp",
        }
        availability.setdefault(raw_id, {})

    for slot in payload.get("slots") or []:
        raw_id = str(slot.get("facilityId") or slot.get("id") or "").strip()
        ymd = re.sub(r"\D", "", str(slot.get("date") or slot.get("ymd") or ""))[:8]
        time_content = str(slot.get("timeContent") or slot.get("time") or "").strip()
        if not raw_id or len(ymd) != 8 or not time_content:
            continue
        if raw_id not in facilities:
            facilities[raw_id] = {
                "title": slot.get("facilityName") or slot.get("courtName") or raw_id,
                "location": "성남",
                "reserveUrl": slot.get("reserveUrl") or LIST_URL,
                "availabilityStatus": "android_chrome_cdp",
            }
        normalized_slot = {
            "timeContent": time_content,
            "reserveUrl": slot.get("reserveUrl") or LIST_URL,
            "source": "android_chrome_cdp",
        }
        for key in ("remaining", "available", "statusText", "courtName"):
            if slot.get(key) not in (None, ""):
                normalized_slot[key] = slot.get(key)
        availability.setdefault(raw_id, {}).setdefault(ymd, []).append(normalized_slot)

    slot_count = sum(len(slots or []) for daymap in availability.values() for slots in daymap.values())
    result: Dict[str, Any] = {
        "facilities": facilities,
        "availability": availability,
        "login_required": False,
        "android_status": "ok",
    }
    if slot_count == 0:
        result.update(
            {
                "partial_failure": True,
                "android_status": "network_capture_failed",
                "error_type": "NetworkCaptureFailed",
                "error_message": "android collector returned zero slots; cache protected until confirmed no-reservation crawl exists",
            }
        )
    return result


def _crawl_seongnam_android() -> Dict[str, Any]:
    payload = _run_android_collector()
    result = _normalize_android_result(payload)
    print(
        "[SEONGNAM][ANDROID] "
        f"status={payload.get('status')} facilities={len(result.get('facilities') or {})} "
        f"slots={sum(len(slots or []) for daymap in (result.get('availability') or {}).values() for slots in daymap.values())}"
    )
    return result


def _github_actions_skip_result() -> Dict[str, Any]:
    return {
        "facilities": {},
        "availability": {},
        "partial_failure": True,
        "automation_blocked": True,
        "android_status": "automation_blocked",
        "error_type": "AutomationBlocked",
        "error_message": "GitHub Actions auto mode does not access Seongnam directly; use local Android collector",
    }


def _request(session: requests.Session, method: str, path: str, **kwargs) -> requests.Response:
    response = session.request(method, f"{BASE_URL}/{path}", **kwargs)
    if response.status_code == 404:
        print(f"[SEONGNAM][AUTH] session expired path={path}; retry authentication")
        if _ensure_authenticated_session(session):
            response = session.request(method, f"{BASE_URL}/{path}", **kwargs)
    response.raise_for_status()
    return response


def _parse_timetable(html: str) -> list[dict]:
    slots = []
    soup = BeautifulSoup(html, "lxml")
    for row in soup.select("tr"):
        radio = row.select_one('input[name="rbTime"]')
        cells = row.select("td")
        if not radio or len(cells) < 3:
            continue
        if radio.has_attr("disabled") or not (radio.get("value") or "").strip():
            continue
        status_text = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
        if status_text:
            continue
        time_content = cells[2].get_text(" ", strip=True)
        if time_content:
            slots.append({"timeContent": time_content, "reserveUrl": LIST_URL})
    return slots


def _use_date_status_precheck() -> bool:
    raw = (os.getenv("SEONGNAM_DATE_STATUS_PRECHECK") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _reservation_status(session: requests.Session, fac_id: str, request_date: str) -> str:
    response = _request(
        session,
        "GET",
        "getReservationInfoByDate.do",
        params={"facId": fac_id, "resdate": request_date},
        timeout=_request_timeout(),
    )
    return response.text.strip().lower()


def _is_skippable_date_status(status: str) -> bool:
    return status in {"closed", "empty", "full"}


def _days_ahead() -> int:
    raw = (os.getenv("SEONGNAM_DAYS_AHEAD") or "45").strip()
    try:
        return max(0, min(int(raw), 90))
    except ValueError:
        return 45


def _max_workers() -> int:
    raw = (os.getenv("SEONGNAM_MAX_WORKERS") or "16").strip()
    try:
        return max(1, min(int(raw), 32))
    except ValueError:
        return 16


def _require_login() -> bool:
    raw = (os.getenv("SEONGNAM_REQUIRE_LOGIN") or "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _thread_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = _session()
        user_id = (os.getenv("ISDC_ID") or "").strip()
        password = os.getenv("ISDC_PW") or ""
        if (user_id or password) and not _ensure_authenticated_session(session) and _require_login():
            raise RuntimeError("seongnam thread login failed")
        _thread_local.session = session
    return session


def _crawl_court(session: requests.Session, fac_id: str, days_ahead: int) -> Dict[str, list]:
    daymap: Dict[str, list] = {}
    use_precheck = _use_date_status_precheck()
    for offset in range(days_ahead + 1):
        current = date.today() + timedelta(days=offset)
        request_date = f"{current.year}-{current.month}-{current.day}"
        yyyymmdd = current.strftime("%Y%m%d")
        try:
            if use_precheck:
                status = _reservation_status(session, fac_id, request_date)
                if _is_skippable_date_status(status):
                    daymap[yyyymmdd] = []
                    continue
            timetable = _request(
                session,
                "POST",
                "getTimeTableByDate.do",
                data={"facId": fac_id, "resdate": request_date},
                timeout=_request_timeout(),
            )
            daymap[yyyymmdd] = _parse_timetable(timetable.text)
        except Exception as exc:
            print(f"[SEONGNAM][WARN] court={fac_id} date={request_date} error={exc}")
            daymap[yyyymmdd] = []
    return daymap


def _crawl_court_threaded(fac_id: str, days_ahead: int) -> Dict[str, list]:
    return _crawl_court(_thread_session(), fac_id, days_ahead)


def crawl_seongnam(collector_mode: str | None = None) -> Dict[str, Any]:
    mode = _collector_mode(collector_mode)
    if mode == "auto" and os.getenv("GITHUB_ACTIONS"):
        return _github_actions_skip_result()
    if mode == "android":
        return _crawl_seongnam_android()
    if mode == "auto" and not os.getenv("GITHUB_ACTIONS"):
        android = _crawl_seongnam_android()
        if android.get("android_status") not in {"android_device_offline", "AndroidDeviceOffline"} and not (
            android.get("error_type") == "AndroidDeviceOffline"
        ):
            return android

    started_at = time.perf_counter()
    session = _session()
    facilities: Dict[str, dict] = {}
    availability: Dict[str, Dict[str, list]] = {}
    total = ok = empty = fail = 0
    login_ok = _ensure_authenticated_session(session)
    login_required = _require_login() and not login_ok

    try:
        response = session.get(LIST_URL, timeout=_request_timeout())
        response.raise_for_status()
        group_ids = sorted(set(re.findall(r'name=["\']groupId["\']\s+value=["\'](\d+)["\']', response.text)))
        group_titles = {
            group_id: _clean_text(title).replace("테니스장", "")
            for group_id, title in re.findall(
                r'name=["\']groupId["\']\s+value=["\'](\d+)["\'].*?'
                r'<div\s+class=["\']head-area["\']>\s*(.*?)\s*</div>',
                response.text,
                re.I | re.S,
            )
        }
    except Exception as exc:
        print(f"[SEONGNAM][WARN] facility list error={exc}")
        print("[SEONGNAM][STATS] total=0 ok=0 empty=0 fail=1 courts=0 slots=0 login_required=1")
        return {"facilities": {}, "availability": {}, "login_required": True}

    if _last_auth_error_type == "AutomationBlocked":
        print("[SEONGNAM][AUTH] automation blocked; skip facility group requests")
        return {
            "facilities": {},
            "availability": {},
            "login_required": True,
            "automation_blocked": True,
            "error_type": "AutomationBlocked",
            "error_message": "automation blocked before facility group requests",
        }

    for group_id in group_ids:
        try:
            response = session.post(f"{BASE_URL}/tennisList.do", data={"groupId": group_id}, timeout=_request_timeout())
            response.raise_for_status()
            items = re.findall(
                r'<li\s+id=["\'](FAC\d+)["\'][^>]*class=["\']facilityInfo["\'].*?'
                r'<div\s+class=["\']head-area["\']>\s*(.*?)\s*</div>',
                response.text,
                re.I | re.S,
            )
            for fac_id, raw_title in items:
                title = _clean_text(raw_title)
                group_title = group_titles.get(group_id, "")
                if group_title and not title.startswith(group_title):
                    title = f"{group_title} {title}"
                facilities[fac_id] = {
                    "title": title,
                    "location": "성남시",
                    "reserveUrl": LIST_URL,
                    "availabilityStatus": "authenticated" if login_ok else "public_timetable",
                }
                availability[fac_id] = {}
        except Exception as exc:
            fail += 1
            print(f"[SEONGNAM][WARN] group={group_id} error={exc}")

    if group_ids and not facilities and fail == len(group_ids):
        print("[SEONGNAM][AUTH] all facility group requests rejected")
        automation_blocked = _last_auth_error_type == "AutomationBlocked"
        return {
            "facilities": {},
            "availability": {},
            "login_required": True,
            "automation_blocked": automation_blocked,
            "error_type": "AutomationBlocked" if automation_blocked else "AuthenticationRequired",
            "error_message": "all facility group requests were rejected",
        }

    if not login_required:
        days_ahead = _days_ahead()
        max_workers = min(_max_workers(), max(1, len(facilities)))
        print(f"[SEONGNAM][DAYS] days_ahead={days_ahead} dates_per_court={days_ahead + 1}")
        print(f"[SEONGNAM][REQUESTS] courts={len(facilities)} dates={days_ahead + 1} workers={max_workers}")
        print(f"[SEONGNAM][PRECHECK] date_status={int(_use_date_status_precheck())}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_crawl_court_threaded, fac_id, days_ahead): fac_id
                for fac_id in facilities
            }
            for future in as_completed(futures):
                fac_id = futures[future]
                total += 1
                try:
                    daymap = future.result()
                    availability[fac_id] = daymap
                    if any(daymap.values()):
                        ok += 1
                    else:
                        empty += 1
                except Exception as exc:
                    fail += 1
                    availability[fac_id] = {}
                    print(f"[SEONGNAM][WARN] court={fac_id} error={exc}")

    slots = sum(len(items) for daymap in availability.values() for items in daymap.values())
    print(
        f"[SEONGNAM][STATS] total={total} ok={ok} empty={empty} fail={fail} "
        f"courts={len(facilities)} slots={slots} login_required={int(login_required)}"
    )
    print(f"[SEONGNAM][ELAPSED] seconds={time.perf_counter() - started_at:.2f}")
    if login_required:
        print("[SEONGNAM][INFO] public court list collected; availability requires login")
    elif not login_ok:
        print("[SEONGNAM][INFO] login failed; continued with public timetable endpoints")
    return {
        "facilities": facilities,
        "availability": availability,
        "login_required": login_required,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--collector", choices=["auto", "android", "playwright"], default=None)
    args = parser.parse_args()
    print(json.dumps(crawl_seongnam(args.collector), ensure_ascii=False))
