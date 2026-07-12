import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict


_write_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _counts(result: Dict[str, Any]) -> tuple[int, int, int, int]:
    facilities = result.get("facilities") or {}
    availability = result.get("availability") or {}
    dates = set()
    slots = 0
    for daymap in availability.values():
        for ymd, items in (daymap or {}).items():
            dates.add(str(ymd))
            slots += len(items or [])
    return len(facilities), len(dates), slots, slots


def _status(result: Dict[str, Any], facilities: int, dates: int, slots: int) -> str:
    if result.get("login_required"):
        return "authentication_required"
    if result.get("automation_blocked"):
        return "automation_blocked"
    if result.get("partial_failure"):
        return "partial_failure"
    if facilities == 0 or dates == 0:
        return "parse_failure"
    if slots == 0:
        return "no_reservations"
    return "normal"


def _append(record: dict, diagnostics_path: str | Path | None) -> None:
    raw_path = diagnostics_path or os.getenv("CRAWL_DIAGNOSTICS_PATH", "")
    if not raw_path:
        return
    path = Path(raw_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _write_lock, path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(record, ensure_ascii=False) + "\n")


def run_crawler(
    region: str,
    crawler: str,
    source_url: str,
    crawl: Callable[[], Dict[str, Any]],
    *,
    facility: str = "*",
    diagnostics_path: str | Path | None = None,
) -> Dict[str, Any]:
    started_at = _now()
    try:
        result = crawl() or {}
        facilities, requested_dates, slots, available = _counts(result)
        status = _status(result, facilities, requested_dates, slots)
        error_type = str(result.get("error_type") or "")
        error_message = str(result.get("error_message") or "")
    except Exception as exc:
        result = {"facilities": {}, "availability": {}, "partial_failure": True}
        facilities = requested_dates = slots = available = 0
        status = "connection_failure" if isinstance(exc, (ConnectionError, TimeoutError)) else "parse_failure"
        error_type = type(exc).__name__
        error_message = str(exc)
    finished_at = _now()
    last_success_at = finished_at if status in {"normal", "no_reservations"} else None
    diagnostic = {
        "region": region,
        "facility": facility,
        "crawler": crawler,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "requested_dates": requested_dates,
        "discovered_courts": facilities,
        "discovered_slots": slots,
        "available_slots": available,
        "saved_rows": 0,
        "last_success_at": last_success_at,
        "error_type": error_type,
        "error_message": error_message,
        "source_url": source_url,
    }
    result["diagnostic"] = diagnostic
    _append(diagnostic, diagnostics_path)
    print("[CRAWL_DIAGNOSTIC] " + json.dumps(diagnostic, ensure_ascii=False))
    return result
