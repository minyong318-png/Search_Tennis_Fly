from flask import Flask, jsonify, request, send_file, redirect, session, send_from_directory
from datetime import datetime,timezone,timedelta
from collections import defaultdict
import os, json, traceback, requests, re
import threading
import time
import queue
from pywebpush import webpush
import json
import psycopg2
from psycopg2.extras import RealDictCursor

from tennis_core import run_all
import refresh_and_notify




# =========================
# Flask 기본 설정
# =========================
app = Flask(__name__)
#app.secret_key = os.environ.get("FLASK_SECRET", "tennis-secret")

# =========================
# 환경변수 설정
# =========================
DATABASE_URL = os.environ["DATABASE_URL"]
VAPID_PRIVATE_KEY = os.environ["VAPID_PRIVATE_KEY"]
KST = timezone(timedelta(hours=9))
MIN_REFRESH_INTERVAL = timedelta(minutes=5)
db_initialized = False

# =========================
# 데이터베이스 연결
# =========================
def get_db():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

# =========================
# 데이터베이스 초기화
# =========================
def init_db():
    print("🔥 init_db CALLED")
    with get_db() as conn:
        with conn.cursor() as cur:
            # alarms 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alarms (
                    id SERIAL PRIMARY KEY,
                    subscription_id TEXT NOT NULL,
                    court_group TEXT NOT NULL,
                    date TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (subscription_id, court_group, date)
                );
            """)

            # 🔥 push_subscriptions 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS push_subscriptions (
                    id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    p256dh TEXT NOT NULL,
                    auth TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sent_slots (
                    subscription_id TEXT NOT NULL,
                    slot_key TEXT NOT NULL,
                    sent_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (subscription_id, slot_key)
                );
            """)

            # ✅ baseline_slots 테이블 (이게 핵심)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS baseline_slots (
                    id SERIAL PRIMARY KEY,
                    subscription_id TEXT NOT NULL,
                    court_group TEXT NOT NULL,
                    date CHAR(8) NOT NULL,
                    time_content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (subscription_id, court_group, date, time_content)
                );
            """)
        conn.commit()

@app.before_request
def ensure_db_initialized():
    global db_initialized
    if db_initialized:
        return

    init_db()
    db_initialized = True

import hashlib

def is_critical_window_kst(now_kst: datetime) -> bool:
    """
    매일 23:50 ~ 24:00(KST) 구간
    """
    return (
        now_kst.hour == 23 and now_kst.minute >= 50
    ) or (
        now_kst.hour == 0 and now_kst.minute == 0
    )

def make_subscription_id(subscription):
    """
    Web Push subscription → 기기 고유 ID 생성
    """
    endpoint = subscription.get("endpoint", "")
    return hashlib.sha256(endpoint.encode("utf-8")).hexdigest()

# =========================
# 서비스워커 제공
# =========================

@app.route("/sw.js")
def service_worker():
    return send_from_directory("static", "sw.js")

# =========================
# 전역 캐시
# =========================
CACHE = {
    "facilities": {},
    "availability": {},
    "updated_at": None
}

# =========================
# 메인 페이지
# =========================
@app.route("/")
def index():
    return send_file("ios_template.html")

# =========================
# 데이터 API
# =========================
@app.route("/data")
def data():
    if not CACHE["updated_at"]:
        try:
            facilities, raw_availability = crawl_all()
            availability = {}
            for cid, days in raw_availability.items():
                availability[cid] = {}
                for date, slots in days.items():
                    availability[cid][date] = []
                    for s in slots:
                        availability[cid][date].append({
                            "timeContent": s.get("timeContent"),
                            "resveId": s.get("resveId")   # 🔥 이 줄이 핵심
                        })

            CACHE["facilities"] = facilities
            CACHE["availability"] = availability
            CACHE["updated_at"] = datetime.now(KST).isoformat()

        except Exception:
            pass

    return jsonify({
        "facilities": CACHE["facilities"],
        "availability": CACHE["availability"],
        "updated_at": CACHE["updated_at"]

    })

# =========================
# 크롤링 갱신 (UptimeRobot)
# =========================
@app.route("/refresh")
def refresh():
    print("[INFO] refresh start")

    with get_db() as conn:
        with conn.cursor() as cur:
            cleanup_old_alarm_data(cur)
        conn.commit()
    try:
        facilities, availability = crawl_all()
        court_group_map = build_court_group_map(facilities)
    except Exception as e:
        print("[ERROR] crawl failed", e)
        return "crawl failed", 500

    # 🔥 테스트 모드: ?test=1
    if request.args.get("test") == "1":
        inject_test_slot_1(facilities, availability)
    if request.args.get("test") == "2":
        inject_test_slot_2(facilities, availability)
    if request.args.get("test") == "3":
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM push_subscriptions LIMIT 1")
                s = cur.fetchone()

        if s:
            send_push_notification(
                {
                    "endpoint": s["endpoint"],
                    "keys": {
                        "p256dh": s["p256dh"],
                        "auth": s["auth"]
                    }
                },
                title="🎾 예약 가능 알림 테스트",
                body="정상 동작 확인"
            )
        else:
            print("[TEST] push_subscriptions 비어 있음")
    try:
        new_availability = {}
        for cid, days in availability.items():
            new_availability[cid] = {}
            for date, slots in days.items():
                new_availability[cid][date] = []
                for s in slots:
                    new_availability[cid][date].append({
                    "timeContent": s.get("timeContent"),
                    "resveId": s.get("resveId"),
                    })
        CACHE["facilities"] = facilities
        CACHE["availability"] = new_availability
        CACHE["updated_at"] = datetime.now(KST).isoformat()
        print("[INFO] CACHE updated in /refresh")
    except Exception as e:
        print("[ERROR] cache update failed", e)
    
    # ✅ 여기!
    court_group_map = build_court_group_map(facilities)
    current_slots = flatten_slots(facilities, availability)

    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM alarms")
                alarms = cur.fetchall()

                cur.execute("SELECT * FROM push_subscriptions")
                subs = cur.fetchall()

                # ✅ subs_map 정의 (id -> subscription dict)
                subs_map = {}
                for s in subs:
                    subs_map[s["id"]] = {
                        "endpoint": s["endpoint"],
                        "keys": {"p256dh": s["p256dh"], "auth": s["auth"]},
                    }

                fired = 0

                for alarm in alarms:
                    subscription_id = alarm["subscription_id"]
                    alarm_group = alarm["court_group"]
                    alarm_date = alarm["date"]

                    group_cids = court_group_map.get(alarm_group, [])
                    if not group_cids:
                        continue

                    # 🔑 이 알람(사람+코트+날짜)의 baseline 로드
                    cur.execute("""
                        SELECT time_content
                        FROM baseline_slots
                        WHERE subscription_id = %s
                        AND court_group = %s
                        AND date = %s
                    """, (subscription_id, alarm_group, alarm_date))

                    baseline = {r["time_content"] for r in cur.fetchall()}

                    # 🔥 최초 refresh → baseline 초기화만 하고 알람 ❌
                    if not baseline:
                        times = {
                            slot["time"]
                            for slot in current_slots
                            if slot["cid"] in group_cids and slot["date"] == alarm_date
                        }
                        for t in times:
                            add_to_baseline(cur, subscription_id, alarm_group, alarm_date, t)
                        continue
                            # ❗ 최초 refresh에서는 절대 알람 안 울림
                    print("DEBUG alarm:", subscription_id, alarm_group, alarm_date)
                    print("DEBUG group_cids:", group_cids)

                    for slot in current_slots:
                        if slot["date"] == alarm_date:
                            print("DEBUG slot:", slot["cid"], slot["date"], slot["time"])

                    # 🔔 이후 refresh → 신규 슬롯만 알람
                    for slot in current_slots:
                        if slot["cid"] not in group_cids:
                            continue
                        if slot["date"] != alarm_date:
                            continue
                        if slot["time"] in baseline:
                            continue

                        sub = subs_map.get(subscription_id)
                        if not sub:
                            continue

                        # 중복 발송 방지 (group 기준)
                        slot_key = f"{alarm_group}|{alarm_date}|{slot['time']}"

                        cur.execute("""
                            SELECT 1 FROM sent_slots
                            WHERE subscription_id=%s AND slot_key=%s
                        """, (subscription_id, slot_key))
                        if cur.fetchone():
                            continue

                        # 🔔 알람 발송
                        send_push_notification(
                            sub,
                            title="🎾 예약 가능 알림",
                            body=f"{alarm_group} {alarm_date} {slot['time']}"
                        )
                        fired += 1
                        print(f"[INFO] push sent to {subscription_id} | {alarm_group} | {alarm_date} | {slot['time']}")

                        # 기록
                        add_to_baseline(
                            cur,
                            subscription_id,
                            alarm_group,
                            alarm_date,
                            slot["time"]
                        )
                        baseline.add(slot["time"])

                        cur.execute("""
                            INSERT INTO sent_slots (subscription_id, slot_key)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (subscription_id, slot_key))


            conn.commit()

        print(f"[INFO] refresh done (fired={fired})")
        return "ok"

    except Exception as e:
        print("[ERROR] push notification failed", e)
        traceback.print_exc()
        return "push failed", 500

# =========================
# Push 구독 저장 API
# =========================
@app.route("/push/subscribe", methods=["POST"])
def push_subscribe():
    sub = request.json
    if not sub:
        return jsonify({"error": "no subscription"}), 400

    sid = make_subscription_id(sub)

    endpoint = sub.get("endpoint")
    keys = sub.get("keys", {})
    p256dh = keys.get("p256dh")
    auth = keys.get("auth")

    if not endpoint or not p256dh or not auth:
        return jsonify({"error": "invalid subscription"}), 400

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO push_subscriptions (id, endpoint, p256dh, auth)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET
                  endpoint = EXCLUDED.endpoint,
                  p256dh = EXCLUDED.p256dh,
                  auth = EXCLUDED.auth
            """, (sid, endpoint, p256dh, auth))

    return jsonify({"subscription_id": sid})

# =========================
# 알람 등록 API (중복 방지 포함)
# =========================
@app.route("/alarm/add", methods=["POST"])
def alarm_add():
    data = request.json or {}

    subscription_id = data.get("subscription_id")
    court_group = data.get("court_group")
    date_raw = data.get("date")   # "2025-12-22"

    if not subscription_id or not court_group or not date_raw:
        return jsonify({"error": "invalid request"}), 400

    # 날짜 포맷 통일 (YYYYMMDD)
    date = date_raw.replace("-", "")

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alarms (subscription_id, court_group, date)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (subscription_id, court_group, date))
            conn.commit()

        return jsonify({"status": "added"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =========================
# 알람 목록 조회 API
# =========================
@app.route("/alarm/list")
def alarm_list():
    subscription_id = request.args.get("subscription_id")
    if not subscription_id:
        return jsonify([])

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT court_group, date, created_at
                FROM alarms
                WHERE subscription_id = %s
                ORDER BY created_at DESC
            """, (subscription_id,))
            rows = cur.fetchall()

    return jsonify(rows)

# =========================
# 알람 삭제 API
# =========================
@app.route("/alarm/delete", methods=["POST"])
def alarm_delete():
    body = request.json or {}

    subscription_id = body.get("subscription_id")
    court_group = body.get("court_group")
    date = body.get("date")

    if not subscription_id or not court_group or not date:
        return jsonify({"error": "invalid request"}), 400

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM alarms
                WHERE subscription_id=%s AND court_group=%s AND date=%s
            """, (subscription_id, court_group, date))

    return jsonify({"status": "deleted"})
# =========================
# 헬스체크
# =========================
@app.route("/health")
def health():
    return "ok"

# =========================
# 안전한 JSON 로드/저장
# =========================

def safe_load(path, default):
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, type(default)) else default
    except Exception as e:
        print(f"[WARN] JSON load failed: {path} | {e}")
        return default



def safe_save(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] JSON save failed: {path} | {e}")

# =========================
# 전체 크롤링 실행
# =========================
def crawl_all():
    # 앱 라우트(/data, /refresh)도 워커/액션과 동일한 통합 크롤러를 사용한다.
    # refresh_and_notify.crawl_all()은 RUN_TARGET 기준으로 용인/고양/전체를 처리한다.
    return refresh_and_notify.crawl_all()

# =========================
def make_reserve_link(resve_id):
    base = "https://publicsports.yongin.go.kr/publicsports/sports/selectFcltyRceptResveViewU.do"
    return (
        f"{base}"
        f"?key=4236"
        f"&resveId={resve_id}"
        f"&pageUnit=8"
        f"&pageIndex=1"
        f"&checkSearchMonthNow=false"
    )
# =========================
#  알림 전송
# =========================
def send_push_notification(subscription, title, body):
    payload = json.dumps({
        "title": title,
        "body": body
    })

    webpush(
        subscription_info=subscription,
        data=payload,
        vapid_private_key=VAPID_PRIVATE_KEY,
        vapid_claims={
            "sub": "mailto:ccoo2000@naver.com"
        }
    )

# =========================
# 기준선 슬롯 존재 여부 확인
# =========================

def is_in_baseline(cur, subscription_id, cid, date, time_content):
    cur.execute("""
        SELECT 1
        FROM baseline_slots
        WHERE subscription_id = %s
          AND cid = %s
          AND date = %s
          AND time_content = %s
        LIMIT 1
    """, (subscription_id, cid, date, time_content))

    return cur.fetchone() is not None

# =========================
# 기준선 슬롯 추가
# =========================
def add_to_baseline(cur, subscription_id, court_group, date, time_content):
    cur.execute("""
        INSERT INTO baseline_slots
            (subscription_id, court_group, date, time_content)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (subscription_id, court_group, date, time_content))


# =========================
# 기준선 슬롯 정리
# =========================
def cleanup_old_alarm_data(cur):
    today = datetime.now(KST).strftime("%Y%m%d")

    cur.execute("""
        DELETE FROM alarms
        WHERE date < %s
    """, (today,))

    cur.execute("""
        DELETE FROM baseline_slots
        WHERE date < %s
    """, (today,))

    cur.execute("""
        DELETE FROM sent_slots
        WHERE sent_at < NOW() - INTERVAL '1 day';
    """, (f"%|{today}%",))


# =========================
# 코트 그룹 추출
# =========================
def get_court_group(title: str) -> str:
    if not title:
        return ""

    # [유료], [무료] 같은 대괄호 제거
    title = re.sub(r"\[.*?\]", "", title)

    # '테니스장' 앞까지만 사용
    if "테니스장" in title:
        title = title.split("테니스장")[0]

    return title.strip()

# =========================
# 코트 그룹 맵 빌드
# =========================
def build_court_group_map(facilities: dict) -> dict:
    """
    {
      "남사": ["10153", "10154"],
      "죽전": ["10201"]
    }
    """
    group_map = {}

    for cid, info in facilities.items():
        title = info.get("title", "")
        group = get_court_group(title)
        if not group:
            continue

        group_map.setdefault(group, []).append(cid)

    return group_map

# =========================
# 슬롯 평탄화
# =========================
def flatten_slots(facilities, availability):
    slots = []
    for cid, days in availability.items():
        title = facilities.get(cid, {}).get("title", "")
        for date, items in days.items():
            for s in items:
                slots.append({
                    "cid": cid,
                    "court_title": title,
                    "date": date,
                    "time": s["timeContent"],
                    "key": f"{cid}|{date}|{s['timeContent']}",
                    "is_test": s.get("is_test", False)
                })
    return slots


def inject_test_slot_1(facilities, availability):
    # 🔥 반드시 문자열
    target_cid = "10343"

    if target_cid not in facilities:
        print("[TEST] cid 10343 not found")
        return

    # 🔥 availability 실제 포맷
    test_date = "20251222"
    test_time = "04:00 ~ 06:00"

    availability.setdefault(target_cid, {})
    availability[target_cid].setdefault(test_date, [])

    if any(s["timeContent"] == test_time
           for s in availability[target_cid][test_date]):
        print("[TEST] 이미 테스트 슬롯 존재")
        return

    availability[target_cid][test_date].append({
        "timeContent": test_time,
        "resveId": None
    })

    print("[TEST] 슬롯 주입:", target_cid, test_date, test_time)


def inject_test_slot_2(facilities, availability):
    # 🔥 반드시 문자열
    target_cid = "10343"

    if target_cid not in facilities:
        print("[TEST] cid 10343 not found")
        return

    # 🔥 availability 실제 포맷
    test_date = "20251222"
    test_time = "22:00 ~ 24:00"

    availability.setdefault(target_cid, {})
    availability[target_cid].setdefault(test_date, [])

    if any(s["timeContent"] == test_time
           for s in availability[target_cid][test_date]):
        print("[TEST] 이미 테스트 슬롯 존재")
        return

    availability[target_cid][test_date].append({
        "timeContent": test_time,
        "resveId": None
    })

    print("[TEST] 슬롯 주입:", target_cid, test_date, test_time)



if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8080)


