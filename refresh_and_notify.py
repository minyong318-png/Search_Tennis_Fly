import os
import json
import hashlib
from datetime import datetime, date
from typing import Dict, List, Set, Tuple

import psycopg
from pywebpush import webpush, WebPushException

import tennis_core


def now_utc():
    return datetime.utcnow()


def ensure_schema(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists push_endpoints (
          user_id text primary key,
          endpoint text not null,
          p256dh text not null,
          auth text not null,
          updated_at timestamptz not null default now()
        );
        """)
        cur.execute("""
        create table if not exists subscriptions (
          id bigserial primary key,
          user_id text not null,
          facility_id text not null,
          facility_name text,
          date_ymd date not null,
          enabled boolean not null default true,
          created_at timestamptz not null default now()
        );
        """)
        cur.execute("""
        create index if not exists idx_subscriptions_fac_date
          on subscriptions (facility_id, date_ymd)
          where enabled = true;
        """)

        cur.execute("""
        create table if not exists slots_snapshot (
          facility_id text not null,
          date_ymd date not null,
          slot_key text not null,
          first_seen_at timestamptz not null default now(),
          last_seen_at timestamptz not null default now(),
          primary key (facility_id, date_ymd, slot_key)
        );
        """)
        cur.execute("""
        create index if not exists idx_slots_snapshot_fac_date
          on slots_snapshot (facility_id, date_ymd);
        """)

        cur.execute("""
        create table if not exists sent_log (
          user_id text not null,
          facility_id text not null,
          date_ymd date not null,
          slot_key text not null,
          sent_at timestamptz not null default now(),
          primary key (user_id, facility_id, date_ymd, slot_key)
        );
        """)
    conn.commit()


def ymd_to_key(d: date) -> str:
    return d.strftime("%Y%m%d")


def slot_key_from_time(t: str) -> str:
    # tennis_core.py의 시간 문자열을 그대로 키로 사용(코트 구분이 있다면 여기서 합치면 됨)
    return t.strip()


def get_needed_targets(conn) -> List[Tuple[str, date, str]]:
    """
    반환: (facility_id, date_ymd, facility_name)
    """
    with conn.cursor() as cur:
        cur.execute("""
          select facility_id, date_ymd, coalesce(facility_name,'')
          from subscriptions
          where enabled = true
        """)
        rows = cur.fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def load_old_slots(conn, facility_id: str, date_ymd: date) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("""
          select slot_key
          from slots_snapshot
          where facility_id = %s and date_ymd = %s
        """, (facility_id, date_ymd))
        return {r[0] for r in cur.fetchall()}


def upsert_snapshot(conn, facility_id: str, date_ymd: date, new_slots: Set[str]):
    ts = now_utc()
    with conn.cursor() as cur:
        for sk in new_slots:
            cur.execute("""
              insert into slots_snapshot (facility_id, date_ymd, slot_key, first_seen_at, last_seen_at)
              values (%s, %s, %s, %s, %s)
              on conflict (facility_id, date_ymd, slot_key)
              do update set last_seen_at = excluded.last_seen_at
            """, (facility_id, date_ymd, sk, ts, ts))
    conn.commit()


def filter_unsent(conn, user_id: str, facility_id: str, date_ymd: date, added: Set[str]) -> List[str]:
    if not added:
        return []
    added_list = list(added)
    with conn.cursor() as cur:
        cur.execute("""
          select slot_key
          from sent_log
          where user_id = %s and facility_id = %s and date_ymd = %s and slot_key = any(%s)
        """, (user_id, facility_id, date_ymd, added_list))
        sent = {r[0] for r in cur.fetchall()}
    return [sk for sk in added_list if sk not in sent]


def mark_sent(conn, user_id: str, facility_id: str, date_ymd: date, slot_keys: List[str]):
    if not slot_keys:
        return
    ts = now_utc()
    with conn.cursor() as cur:
        for sk in slot_keys:
            cur.execute("""
              insert into sent_log (user_id, facility_id, date_ymd, slot_key, sent_at)
              values (%s, %s, %s, %s, %s)
              on conflict (user_id, facility_id, date_ymd, slot_key) do nothing
            """, (user_id, facility_id, date_ymd, sk, ts))
    conn.commit()


def get_push_endpoint(conn, user_id: str):
    with conn.cursor() as cur:
        cur.execute("""
          select endpoint, p256dh, auth
          from push_endpoints
          where user_id = %s
        """, (user_id,))
        row = cur.fetchone()
    if not row:
        return None
    endpoint, p256dh, auth = row
    return {"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth}}


def get_subscribers(conn, facility_id: str, date_ymd: date) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("""
          select distinct user_id
          from subscriptions
          where enabled = true and facility_id = %s and date_ymd = %s
        """, (facility_id, date_ymd))
        return [r[0] for r in cur.fetchall()]


def send_push(subscription: dict, title: str, body: str):
    vapid_public = os.environ["VAPID_PUBLIC_KEY"]
    vapid_private = os.environ["VAPID_PRIVATE_KEY"]
    vapid_subject = os.environ["VAPID_SUBJECT"]

    payload = json.dumps({"title": title, "body": body}, ensure_ascii=False)

    return webpush(
        subscription_info=subscription,
        data=payload,
        vapid_private_key=vapid_private,
        vapid_claims={"sub": vapid_subject},
    )


def main():
    database_url = os.environ["DATABASE_URL"]
    conn = psycopg.connect(database_url)
    ensure_schema(conn)

    targets = get_needed_targets(conn)
    if not targets:
        print("[INFO] no subscriptions. exit.")
        return

    # 구독에 등장하는 facility_id만 크롤링하도록 최적화
    needed_rids = sorted({facility_id for (facility_id, _, _) in targets})

    print(f"[INFO] crawl targets rids={len(needed_rids)} subs={len(targets)}")

    # tennis_core는 기본 run_all()이 전체 시설을 다 도는 구조라서,
    # 여기서는 일단 run_all()로 facilities+availability를 받아오고
    # 필요한 rid만 골라 쓰는 형태로 간다. (나중에 부분크롤링 함수 추가 가능)
    facilities, availability = tennis_core.run_all()

    # facility_id가 rid라고 가정
    # availability: { rid: { "YYYYMMDD": [times...] } }
    total_push = 0
    total_added = 0

    for facility_id, date_ymd, facility_name in targets:
        date_key = ymd_to_key(date_ymd)

        day_map = availability.get(str(facility_id), {})
        times = day_map.get(date_key, [])
        new_slots = {slot_key_from_time(t) for t in times}

        old_slots = load_old_slots(conn, str(facility_id), date_ymd)
        added = new_slots - old_slots

        # ✅ 첫 실행(기준선 없음)일 때도 "added"가 생기는데,
        # 알림 스팸 방지하려면: old_slots가 비어있으면 baseline만 저장하고 알림은 건너뛴다.
        if not old_slots:
            upsert_snapshot(conn, str(facility_id), date_ymd, new_slots)
            print(f"[BASELINE] {facility_id} {date_key} baseline={len(new_slots)} (skip notify)")
            continue

        if added:
            subscribers = get_subscribers(conn, str(facility_id), date_ymd)
            for user_id in subscribers:
                subscription = get_push_endpoint(conn, user_id)
                if not subscription:
                    continue

                to_send = filter_unsent(conn, user_id, str(facility_id), date_ymd, added)
                if not to_send:
                    continue

                # 메시지 구성
                # 시설명은 subscriptions에 저장된 걸 쓰되, 비어있으면 facilities 맵에서 추정
                fname = facility_name.strip() or facilities.get(str(facility_id), f"RID {facility_id}")
                times_preview = ", ".join(sorted(to_send)[:6])
                more = "" if len(to_send) <= 6 else f" 외 {len(to_send)-6}개"
                title = "🎾 예약 오픈"
                body = f"{fname} {date_ymd.strftime('%m/%d')} 신규 슬롯: {times_preview}{more}"

                try:
                    send_push(subscription, title, body)
                    mark_sent(conn, user_id, str(facility_id), date_ymd, to_send)
                    total_push += 1
                    total_added += len(to_send)
                except WebPushException as e:
                    # 410 Gone 같은 경우는 구독이 죽은 것 → 정리(간단 처리)
                    print(f"[PUSH_FAIL] user={user_id} status={getattr(e.response,'status_code',None)} err={e}")

            # snapshot 갱신(새 슬롯 포함)
            upsert_snapshot(conn, str(facility_id), date_ymd, new_slots)
            print(f"[DIFF] {facility_id} {date_key} old={len(old_slots)} new={len(new_slots)} added={len(added)}")
        else:
            # 변화 없으면 snapshot만 last_seen 갱신해도 되는데, 일단 new_slots upsert
            upsert_snapshot(conn, str(facility_id), date_ymd, new_slots)
            print(f"[NOCHANGE] {facility_id} {date_key} slots={len(new_slots)}")

    print(f"[SUMMARY] pushes={total_push} added_slots={total_added}")


if __name__ == "__main__":
    main()
