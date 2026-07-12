import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List

import psycopg
import requests
from psycopg.rows import dict_row


KST = timezone(timedelta(hours=9))

CITY_CONFIG = {
    "yongin": {"label": "용인", "min_facilities": 1, "max_age_hours": 30},
    "goyang": {"label": "고양", "min_facilities": 1, "max_age_hours": 30},
    "suwon": {"label": "수원", "min_facilities": 1, "max_age_hours": 30},
    "seongnam": {"label": "성남", "min_facilities": 1, "max_age_hours": 30},
    "anyang": {"label": "안양", "min_facilities": 1, "max_age_hours": 30},
    "paju": {"label": "파주", "min_facilities": 1, "max_age_hours": 30},
    "hanam": {"label": "하남", "min_facilities": 1, "max_age_hours": 30},
    "uiwang": {"label": "의왕", "min_facilities": 1, "max_age_hours": 30},
    "incheon": {"label": "인천", "min_facilities": 1, "max_age_hours": 30},
    "anseong": {"label": "안성", "min_facilities": 1, "max_age_hours": 30},
    "uijeongbu": {"label": "의정부", "min_facilities": 1, "max_age_hours": 30},
    "yangpyeong": {"label": "양평", "min_facilities": 1, "max_age_hours": 30},
}


def kst_now() -> datetime:
    return datetime.now(tz=KST)


def yyyymmdd(value: datetime) -> str:
    return value.strftime("%Y%m%d")


def parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def slot_count(slots: Any) -> int:
    if slots is None:
        return 0
    if isinstance(slots, str):
        try:
            slots = json.loads(slots)
        except json.JSONDecodeError:
            return 0
    return len(slots) if isinstance(slots, list) else 0


def city_from_facility_id(facility_id: str) -> str | None:
    city = str(facility_id or "").split(":", 1)[0]
    return city if city in CITY_CONFIG else None


def load_from_database(database_url: str, days_ahead: int) -> Dict[str, Dict[str, Any]]:
    today = kst_now().date()
    end = today + timedelta(days=days_ahead)
    rows: List[dict] = []
    facility_rows: List[dict] = []
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select facility_id, title, updated_at
                  from public.facilities
                 where split_part(facility_id, ':', 1) = any(%s)
                """,
                (list(CITY_CONFIG.keys()),),
            )
            facility_rows = list(cur.fetchall())
            cur.execute(
                """
                select facility_id, to_char(date_ymd, 'YYYYMMDD') as date_ymd, slots_json, updated_at
                  from public.availability_cache
                 where split_part(facility_id, ':', 1) = any(%s)
                   and date_ymd between %s and %s
                """,
                (list(CITY_CONFIG.keys()), today, end),
            )
            rows = list(cur.fetchall())
    return summarize_rows(facility_rows, rows)


def load_from_public_rpc(url: str, anon_key: str, days_ahead: int) -> Dict[str, Dict[str, Any]]:
    endpoint = f"{url.rstrip('/')}/rest/v1/rpc/get_public_data"
    response = requests.post(
        endpoint,
        headers={
            "apikey": anon_key,
            "authorization": f"Bearer {anon_key}",
            "content-type": "application/json",
        },
        json={"p_days_ahead": days_ahead},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    facilities = payload.get("facilities") or {}
    availability = payload.get("availability") or {}
    updated_at = payload.get("updated_at")

    facility_rows = [
        {"facility_id": fid, "title": meta.get("title") if isinstance(meta, dict) else str(meta), "updated_at": updated_at}
        for fid, meta in facilities.items()
    ]
    rows = []
    for fid, day_map in availability.items():
        for date_ymd, slots in (day_map or {}).items():
            rows.append({"facility_id": fid, "date_ymd": date_ymd, "slots_json": slots, "updated_at": updated_at})
    return summarize_rows(facility_rows, rows)


def summarize_rows(facility_rows: Iterable[dict], cache_rows: Iterable[dict]) -> Dict[str, Dict[str, Any]]:
    now = kst_now()
    today = yyyymmdd(now)
    result: Dict[str, Dict[str, Any]] = {
        city: {
            "city": city,
            "label": config["label"],
            "status": "error",
            "reasons": [],
            "facility_count": 0,
            "cache_row_count": 0,
            "slot_count": 0,
            "non_empty_days": 0,
            "date_min": None,
            "date_max": None,
            "latest_updated_at": None,
            "age_hours": None,
        }
        for city, config in CITY_CONFIG.items()
    }

    facility_ids_by_city: Dict[str, set] = defaultdict(set)
    for row in facility_rows:
        fid = str(row.get("facility_id") or "")
        city = city_from_facility_id(fid)
        if not city:
            continue
        facility_ids_by_city[city].add(fid)
        updated = parse_ts(row.get("updated_at"))
        if updated:
            current = parse_ts(result[city]["latest_updated_at"])
            if current is None or updated > current:
                result[city]["latest_updated_at"] = updated.isoformat()

    non_empty_dates_by_city: Dict[str, set] = defaultdict(set)
    all_dates_by_city: Dict[str, List[str]] = defaultdict(list)
    for row in cache_rows:
        fid = str(row.get("facility_id") or "")
        city = city_from_facility_id(fid)
        if not city:
            continue
        result[city]["cache_row_count"] += 1
        date_ymd = str(row.get("date_ymd") or "").replace("-", "")
        if date_ymd:
            all_dates_by_city[city].append(date_ymd)
        count = slot_count(row.get("slots_json"))
        result[city]["slot_count"] += count
        if count > 0 and date_ymd >= today:
            non_empty_dates_by_city[city].add(date_ymd)
        updated = parse_ts(row.get("updated_at"))
        if updated:
            current = parse_ts(result[city]["latest_updated_at"])
            if current is None or updated > current:
                result[city]["latest_updated_at"] = updated.isoformat()

    for city, config in CITY_CONFIG.items():
        item = result[city]
        item["facility_count"] = len(facility_ids_by_city[city])
        dates = sorted(set(all_dates_by_city[city]))
        if dates:
            item["date_min"] = dates[0]
            item["date_max"] = dates[-1]
        item["non_empty_days"] = len(non_empty_dates_by_city[city])

        updated = parse_ts(item["latest_updated_at"])
        if updated:
            item["age_hours"] = round((now - updated.astimezone(KST)).total_seconds() / 3600, 2)

        reasons = item["reasons"]
        if item["facility_count"] < config["min_facilities"]:
            reasons.append("no_facilities")
        if item["cache_row_count"] == 0:
            reasons.append("no_cache_rows")
        if updated is None:
            reasons.append("no_update_time")
        elif item["age_hours"] is not None and item["age_hours"] > config["max_age_hours"]:
            reasons.append(f"stale>{config['max_age_hours']}h")
        if item["date_max"] and item["date_max"] < today:
            reasons.append("no_future_dates")

        if any(reason in reasons for reason in ("no_facilities", "no_cache_rows", "no_update_time", "no_future_dates")):
            item["status"] = "error"
        elif any(reason.startswith("stale>") for reason in reasons) or item["slot_count"] == 0:
            if item["slot_count"] == 0:
                reasons.append("zero_slots")
            item["status"] = "warning"
        else:
            item["status"] = "ok"

    return result


def print_summary(summary: Dict[str, Dict[str, Any]]) -> None:
    print("city,status,facilities,cache_rows,slots,non_empty_days,date_min,date_max,age_hours,reasons")
    for city in CITY_CONFIG:
        item = summary[city]
        print(
            ",".join(
                [
                    item["label"],
                    item["status"],
                    str(item["facility_count"]),
                    str(item["cache_row_count"]),
                    str(item["slot_count"]),
                    str(item["non_empty_days"]),
                    str(item["date_min"] or ""),
                    str(item["date_max"] or ""),
                    str(item["age_hours"] if item["age_hours"] is not None else ""),
                    "|".join(item["reasons"]),
                ]
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Check city crawler health from frontend cache data.")
    parser.add_argument("--days-ahead", type=int, default=int(os.getenv("HEALTH_DAYS_AHEAD", "45")))
    parser.add_argument("--json", dest="json_path", default=os.getenv("HEALTH_JSON_OUT", ""))
    parser.add_argument("--warn-only", action="store_true", default=os.getenv("HEALTH_WARN_ONLY", "0") == "1")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL", "").strip()
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    supabase_anon_key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    if database_url:
        summary = load_from_database(database_url, args.days_ahead)
    elif supabase_url and supabase_anon_key:
        summary = load_from_public_rpc(supabase_url, supabase_anon_key, args.days_ahead)
    else:
        raise RuntimeError("Set DATABASE_URL or SUPABASE_URL + SUPABASE_ANON_KEY")

    print_summary(summary)
    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as fp:
            json.dump({"checked_at": kst_now().isoformat(), "cities": summary}, fp, ensure_ascii=False, indent=2)

    has_error = any(item["status"] == "error" for item in summary.values())
    return 0 if args.warn_only or not has_error else 1


if __name__ == "__main__":
    sys.exit(main())
