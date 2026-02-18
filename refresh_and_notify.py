import os
import json
from datetime import datetime, date
from typing import Dict, List, Set, Tuple, Optional, Iterable

import psycopg
from psycopg.rows import tuple_row
from pywebpush import webpush, WebPushException

import tennis_core


# -------------------------
# Utils
# -------------------------
def utcnow() -> datetime:
    return datetime.utcnow()


def ymd_str_to_date(yyyymmdd: str) -> date:
    return date(int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))


def slot_key_from_time(t) -> str:
    """
    tennis_core 결과 슬롯이 str이 아니라 dict로 오는 케이스 대응.
    가능한 키 후보:
      - time / startTime / start_time / hhmm / label
      - court / courtNo / court_name 같이 코트 구분
    최종 slot_key는 "TIME" 또는 "TIME|COURT" 형태로 만든다.
    """
    if t is None:
        return ""

    # 1) 문자열이면 그대로
    if isinstance(t, str):
        return t.strip()

    # 2) dict면 time 필드 뽑기
    if isinstance(t, dict):
        # 시간 후보 키들
        time_val = (
            t.get("time")
            or t.get("startTime")
            or t.get("start_time")
            or t.get("stime")
            or t.get("label")
        )

        # 시간이 또 dict로 오면(드문 케이스) 문자열화
        if isinstance(time_val, dict):
            time_val = time_val.get("time") or time_val.get("label")

        time_str = str(time_val).strip() if time_val is not None else ""

        # 코트 후보 키들(있으면 붙여서 slot_id를 더 고유하게)
        court_val = (
            t.get("court")
            or t.get("courtNo")
            or t.get("court_no")
            or t.get("courtName")
            or t.get("court_name")
        )
        court_str = str(court_val).strip() if court_val is not None else ""

        if court_str and time_str:
            return f"{time_str}|{court_str}"
        return time_str

    # 3) 그 외는 문자열로
    return str(t).strip()



# -------------------------
# DB schema (minimal)
# -------------------------
SCHEMA_SQL = """
create table if not exists push_endpoints (
  user_id text primary key,
  endpoint text not null,
  p256dh text not null,
  auth text not null,
  updated_at timestamptz not null default now()
);

create table if not exists subscriptions (
  id bigserial primary key,
  user_id text not null,
  facility_id text not null,
  date_ymd date not null,
  enabled boolean not null default true,
  created_at timestamptz not null default now()
);

create index if not exists idx_subscriptions_fac_date
  on subscriptions (facility_id, date_ymd)
  where enabled = true;

create table if not exists slots_snapshot (
  facility_id text not null,
  date_ymd date not null,
  slot_key text not null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  primary key (facility_id, date_ymd, slot_key)
);

create index if not exists idx_slots_snapshot_fac_date
  on slots_snapshot (facility_id, date_ymd);

create table if not exists sent_log (
  user_id text not null,
  facility_id text not null,
  date_ymd date not null,
  slot_key text not null,
  sent_at timestamptz not null default now(),
  primary key (user_id, facility_id, date_ymd, slot_key)
);
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


# -------------------------
# Load data from DB (batch)
# -------------------------
def load_all_endpoints(conn: psycopg.Connection) -> Dict[str, dict]:
    """
    returns: user_id -> subscription_info dict for pywebpush
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("select user_id, endpoint, p256dh, auth from push_endpoints")
        rows = cur.fetchall()

    m = {}
    for user_id, endpoint, p256dh, auth in rows:
        m[user_id] = {"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth}}
    return m


def load_all_subscriptions(conn: psycopg.Connection) -> Dict[Tuple[str, date], List[str]]:
    """
    returns: (facility_id, date_ymd) -> [user_id...]
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("""
            select facility_id, date_ymd, user_id
            from subscriptions
            where enabled = true
        """)
        rows = cur.fetchall()

    m: Dict[Tuple[str, date], List[str]] = {}
    for facility_id, date_ymd, user_id in rows:
        key = (str(facility_id), date_ymd)
        m.setdefault(key, []).append(user_id)
    return m


def load_snapshot_map(
    conn: psycopg.Connection,
    facility_ids: List[str],
    date_list: List[date],
) -> Dict[Tuple[str, date], Set[str]]:
    """
    returns: (facility_id, date_ymd) -> set(slot_key)
    batch로 전부 읽어와 메모리에서 diff
    """
    if not facility_ids or not date_list:
        return {}

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("""
            select facility_id, date_ymd, slot_key
            from slots_snapshot
            where facility_id = any(%s) and date_ymd = any(%s)
        """, (facility_ids, date_list))
        rows = cur.fetchall()

    m: Dict[Tuple[str, date], Set[str]] = {}
    for facility_id, date_ymd, slot_key in rows:
        key = (str(facility_id), date_ymd)
        m.setdefault(key, set()).add(slot_key)
    return m


def upsert_snapshot(conn: psycopg.Connection, facility_id: str, date_ymd: date, slots: Set[str]) -> None:
    ts = utcnow()
    if not slots:
        # 슬롯이 없는 날도 last_seen을 남기고 싶으면 별도 테이블로 관리하는 편이 낫다.
        return

    with conn.cursor() as cur:
        for sk in slots:
            cur.execute("""
                insert into slots_snapshot (facility_id, date_ymd, slot_key, first_seen_at, last_seen_at)
                values (%s, %s, %s, %s, %s)
                on conflict (facility_id, date_ymd, slot_key)
                do update set last_seen_at = excluded.last_seen_at
            """, (facility_id, date_ymd, sk, ts, ts))
    conn.commit()


def load_already_sent(conn: psycopg.Connection, user_id: str, facility_id: str, date_ymd: date, slot_keys: List[str]) -> Set[str]:
    if not slot_keys:
        return set()
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("""
            select slot_key
            from sent_log
            where user_id = %s and facility_id = %s and date_ymd = %s and slot_key = any(%s)
        """, (user_id, facility_id, date_ymd, slot_keys))
        rows = cur.fetchall()
    return {r[0] for r in rows}


def mark_sent(conn: psycopg.Connection, user_id: str, facility_id: str, date_ymd: date, slot_keys: List[str]) -> None:
    if not slot_keys:
        return
    ts = utcnow()
    with conn.cursor() as cur:
        for sk in slot_keys:
            cur.execute("""
                insert into sent_log (user_id, facility_id, date_ymd, slot_key, sent_at)
                values (%s, %s, %s, %s, %s)
                on conflict (user_id, facility_id, date_ymd, slot_key) do nothing
            """, (user_id, facility_id, date_ymd, sk, ts))
    conn.commit()


# -------------------------
# Push
# -------------------------
def send_push(subscription_info: dict, title: str, body: str) -> None:
    vapid_private = os.environ["VAPID_PRIVATE_KEY"]
    vapid_subject = os.environ["VAPID_SUBJECT"]

    payload = json.dumps({"title": title, "body": body}, ensure_ascii=False)
    webpush(
        subscription_info=subscription_info,
        data=payload,
        vapid_private_key=vapid_private,
        vapid_claims={"sub": vapid_subject},
    )


# -------------------------
# Crawl adapter (repo dependent)
# -------------------------
def crawl_all() -> Tuple[Dict[str, str], Dict[str, Dict[str, List[str]]]]:
    """
    Search_Tennis_Fly의 tennis_core.run_all()을 기준으로 작성.
    기대 형태:
      facilities: { rid(str): "시설명" }
      availability: { rid(str): { "YYYYMMDD": [ "HH:MM", ... ] } }
    """
    res = tennis_core.run_all()

    # run_all()이 (facilities, availability) 튜플이면 그대로
    if isinstance(res, tuple) and len(res) == 2:
        facilities, availability = res
        return facilities, availability

    # 혹시 dict로 한 번에 주는 형태면 여기에서 맞춰주기
    # (필요하면 너 run_all 형태 알려주면 더 정확히 맞춰줄게)
    raise RuntimeError("tennis_core.run_all() return shape not supported. Expected (facilities, availability).")


# -------------------------
# Main
# -------------------------
def main():
    # 필수 env
    database_url = os.environ["DATABASE_URL"]
    # public은 페이지에서 쓰는 경우가 많아 env로 없어도 되지만, 세트로 관리하는 편이 좋음
    _ = os.environ.get("VAPID_PUBLIC_KEY", "")  # optional for server-side sending
    _ = os.environ["VAPID_PRIVATE_KEY"]
    _ = os.environ["VAPID_SUBJECT"]
    snapshot_rows = []  # (facility_id, date_ymd, slot_key, ts, ts)
    sent_rows = []      # (user_id, facility_id, date_ymd, slot_key, ts)
    ts_now = utcnow()

    with psycopg.connect(database_url) as conn:
        ensure_schema(conn)

        # 1) 크롤링 (전체)
        facilities, availability = crawl_all()
        print(f"[INFO] crawled facilities={len(availability)}")

        # 2) 이번 크롤에서 등장하는 facility/date 목록 뽑기
        facility_ids = sorted([str(fid) for fid in availability.keys()])
        date_keys: Set[str] = set()
        for _, day_map in availability.items():
            for dk in day_map.keys():
                if dk and len(dk) == 8:
                    date_keys.add(dk)
        date_list = sorted([ymd_str_to_date(dk) for dk in date_keys])
        print(f"[INFO] snapshot preload facility_ids={len(facility_ids)} dates={len(date_list)}")

        # 3) 스냅샷 preload(배치)
        old_map = load_snapshot_map(conn, facility_ids, date_list)

        # 4) 구독/엔드포인트 preload
        subs_map = load_all_subscriptions(conn)
        endpoints = load_all_endpoints(conn)

        # 5) 전체를 돌면서 diff 계산
        total_added_pairs = 0
        total_added_slots = 0
        total_push_requests = 0

        for facility_id, day_map in availability.items():
            fid = str(facility_id)
            fname = facilities.get(fid, f"RID {fid}")

            for date_key, times in day_map.items():
                if not date_key or len(date_key) != 8:
                    continue
                d = ymd_str_to_date(date_key)

                new_slots = {slot_key_from_time(t) for t in (times or [])}
                key = (fid, d)
                old_slots = old_map.get(key, set())

                # ✅ 첫 실행(스냅샷 없음): baseline만 저장하고 알림 스킵
                if not old_slots:
                    if new_slots:
                        for sk in new_slots:
                            snapshot_rows.append((fid, d, sk, ts_now, ts_now))
                    old_map[key] = set(new_slots)
                    continue

                added = new_slots - old_slots

                # 변화 없음: snapshot만 갱신(기본은 upsert로 last_seen 갱신)
                if not added:
                    if new_slots:
                        for sk in new_slots:
                            snapshot_rows.append((fid, d, sk, ts_now, ts_now))
                    old_map[key] = set(new_slots)
                    continue

                # ✅ added가 있을 때만 구독자 매칭
                users = subs_map.get(key, [])
                if not users:
                    # 구독자가 없으면 스냅샷만 갱신
                    if new_slots:
                        for sk in new_slots:
                            snapshot_rows.append((fid, d, sk, ts_now, ts_now))
                    old_map[key] = set(new_slots)

                    continue

                total_added_pairs += 1
                total_added_slots += len(added)

                added_list_sorted = sorted(list(added))

                for user_id in users:
                    sub_info = endpoints.get(user_id)
                    if not sub_info:
                        continue

                    already_sent = load_already_sent(conn, user_id, fid, d, added_list_sorted)
                    to_send = [sk for sk in added_list_sorted if sk not in already_sent]
                    if not to_send:
                        continue

                    preview = ", ".join(to_send[:6])
                    more = "" if len(to_send) <= 6 else f" 외 {len(to_send)-6}개"
                    title = "🎾 예약 오픈"
                    body = f"{fname} {d.strftime('%m/%d')} 신규 슬롯: {preview}{more}"

                    try:
                        send_push(sub_info, title, body)
                        mark_sent(conn, user_id, fid, d, to_send)
                        total_push_requests += 1
                    except WebPushException as e:
                        # 실패해도 snapshot은 계속 갱신해야 다음 diff가 정상 동작
                        code = getattr(getattr(e, "response", None), "status_code", None)
                        print(f"[PUSH_FAIL] user={user_id} fid={fid} date={date_key} status={code} err={e}")

                # 스냅샷 갱신(새 슬롯 포함)
                if new_slots:
                    for sk in new_slots:
                        snapshot_rows.append((fid, d, sk, ts_now, ts_now))
                old_map[key] = set(new_slots)
                print(f"[DIFF] {fid} {date_key} old={len(old_slots)} new={len(new_slots)} added={len(added)} users={len(users)}")
        # --- flush snapshots (bulk upsert) ---
        if snapshot_rows:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into slots_snapshot (facility_id, date_ymd, slot_key, first_seen_at, last_seen_at)
                    values (%s, %s, %s, %s, %s)
                    on conflict (facility_id, date_ymd, slot_key)
                    do update set last_seen_at = excluded.last_seen_at
                    """,
                    snapshot_rows
                )

        # --- flush sent_log (bulk insert) ---
        if sent_rows:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into sent_log (user_id, facility_id, date_ymd, slot_key, sent_at)
                    values (%s, %s, %s, %s, %s)
                    on conflict (user_id, facility_id, date_ymd, slot_key)
                    do nothing
                    """,
                    sent_rows
                )

        conn.commit()

        print(f"[SUMMARY] added_pairs={total_added_pairs} added_slots={total_added_slots} push_requests={total_push_requests}")


if __name__ == "__main__":
    main()
