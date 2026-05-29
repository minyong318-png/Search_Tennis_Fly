import os
import json
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Set, Tuple, Optional, Iterable, Any

import psycopg
from psycopg.rows import dict_row
from pywebpush import webpush, WebPushException

import tennis_core
import crawl_goyang


# =========================================================
# Utils
# =========================================================
def utcnow() -> datetime:
    return datetime.utcnow()


KST = timezone(timedelta(hours=9))


def kst_today_yyyymmdd() -> str:
    return datetime.now(KST).strftime("%Y%m%d")


def is_goyang_crawl_window() -> bool:
    now = datetime.now(KST)
    return 5 <= now.hour < 22


def to_yyyymmdd(s: str) -> str:
    """
    alarms.date가 "YYYY-MM-DD" 또는 "YYYYMMDD" 둘 다 올 수 있음.
    availability는 "YYYYMMDD" 키를 쓰는 걸 전제로 맞춘다.
    """
    if not s:
        return ""
    s = s.strip()
    if "-" in s:
        return s.replace("-", "")
    return s


def yyyymmdd_to_date(yyyymmdd: str) -> date:
    return date(int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))


def date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def get_court_group(title: str, facility_id: str = "") -> str:
    import re
    base = re.sub(r"\[.*?\]", "", title or "").split("테니스장")[0].strip()

    if str(facility_id).startswith("yongin:"):
        return f"용인|{base}"
    if str(facility_id).startswith("goyang:"):
        return f"고양|{base}"
    return base

def slot_key_from_time(t: Any) -> str:
    """
    tennis_core 결과 슬롯이 str이 아니라 dict로 오는 케이스 대응.
    가능한 키 후보:
    - time / startTime / start_time / hhmm / label / timeContent
    - court / courtNo / court_name 같이 코트 구분
    최종 slot_key는 "TIME" 또는 "TIME|COURT" 형태
    """
    if t is None:
        return ""
    if isinstance(t, str):
        return t.strip()

    if isinstance(t, dict):
        time_val = (
            t.get("time")
            or t.get("startTime")
            or t.get("start_time")
            or t.get("stime")
            or t.get("label")
            or t.get("timeContent")
        )
        if isinstance(time_val, dict):
            time_val = time_val.get("time") or time_val.get("label")
        time_str = str(time_val).strip() if time_val is not None else ""

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

    return str(t).strip()


def slot_obj_for_frontend(t: Any, fallback_resve_id: str) -> Dict[str, Any]:
    """
    availability_cache.slots_json에 넣을 프론트용 슬롯 객체.
    프론트는 {timeContent, resveId}를 기대.
    """
    if isinstance(t, dict):
        # 가능한 키를 timeContent/resveId로 정규화
        time_content = (
            t.get("timeContent")
            or t.get("label")
            or t.get("time")
            or t.get("startTime")
            or t.get("start_time")
            or t.get("stime")
        )
        resve_id = t.get("resveId") or t.get("resve_id") or fallback_resve_id
        obj = dict(t)
        obj["timeContent"] = str(time_content) if time_content is not None else slot_key_from_time(t)
        obj["resveId"] = str(resve_id) if resve_id is not None else fallback_resve_id
        return obj

    # str/기타
    return {"timeContent": slot_key_from_time(t), "resveId": fallback_resve_id}


# =========================================================
# DB schema (추가/조회용만: 기존 테이블은 건드리지 않음)
# =========================================================
SCHEMA_SQL = """
-- 프론트용 시설 메타 (없으면 생성)
create table if not exists public.facilities (
  facility_id text primary key,
  title text not null,
  location text,
  updated_at timestamptz not null default now()
);

-- 프론트용 availability 캐시 (없으면 생성)
create table if not exists public.availability_cache (
  facility_id text not null references public.facilities(facility_id) on delete cascade,
  date_ymd date not null,
  slots_json jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (facility_id, date_ymd)
);

create index if not exists idx_avcache_date on public.availability_cache(date_ymd);
"""

def ensure_extra_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()

def _ns_yongin_id(rid: str) -> str:
    return f"yongin:{rid}"

def _ns_goyang_id(kind: str, key: str) -> str:
    # kind: "gytennis" / "daehwa"
    return f"goyang:{kind}:{key}" if key else f"goyang:{kind}"

def _strip_yongin_resve_id(fid: str) -> str:
    # yongin:123 -> 123 (프론트 예약 링크용)
    if isinstance(fid, str) and fid.startswith("yongin:"):
        return fid.split(":", 1)[1]
    return str(fid)

def _ymd_to_yyyymmdd(s: str) -> str:
    # "YYYY-MM-DD" or "YYYYMMDD" -> "YYYYMMDD"
    return to_yyyymmdd(s)
# =========================================================
# Crawl adapter
# =========================================================
def _clean_goyang_title(title: str) -> str:
    if not title:
        return title
    # "고양특례시테니스협회 " 제거 (공백 유무 모두 대응)
    return title.replace("고양특례시테니스협회", "").strip()

def _goyang_split_title(meta: dict, suffix: str) -> dict:
    """시설 meta를 복사해서 title에 suffix를 붙인다."""
    m = dict(meta or {})
    base = _clean_goyang_title(m.get("title", "")).strip()
    if suffix:
        m["title"] = f"{base} {suffix}".strip()
    else:
        m["title"] = base
    return m


def _split_daymap_by_court(
    daymap: Dict[str, List[Any]],
    reserve_url_builder,
) -> Dict[str, Dict[str, List[dict]]]:
    """
    daymap: {yyyymmdd: [slot(dict)...]}
    return: {courtNo: {yyyymmdd: [slot(dict)...]}}
    reserve_url_builder(slot, yyyymmdd) -> str | None
    """
    out: Dict[str, Dict[str, List[dict]]] = {}
    for yyyymmdd, slots in (daymap or {}).items():
        for s in (slots or []):
            if not isinstance(s, dict):
                continue
            cno = str(s.get("courtNo") or "").strip()
            if not cno:
                continue
            ss = dict(s)
            url = reserve_url_builder(ss, yyyymmdd)
            if url:
                ss["reserveUrl"] = url
            out.setdefault(cno, {}).setdefault(yyyymmdd, []).append(ss)
    return out

def crawl_all() -> Tuple[Dict[str, Any], Dict[str, Dict[str, List[Any]]]]:
    """
    최종 반환:
      facilities: { facility_id(text): {title, location, ...} }
      availability: { facility_id: { "YYYYMMDD": [slot, ...] } }

    ✅ 고양은 "코트별 facility"로 분리해서 반환
      - gytennis: goyang:gytennis:{courtvalue}:{courtNo}
      - daehwa  : goyang:daehwa:{courtNo}

    ✅ RUN_TARGET
      - yongin / goyang / all
    """
    target = (os.getenv("RUN_TARGET") or "all").strip().lower()

    facilities: Dict[str, Any] = {}
    availability: Dict[str, Dict[str, List[Any]]] = {}

    # -----------------------------
    # (A) 용인 (tennis_core)
    # -----------------------------
    if target in ("all", "yongin"):
        y_fac, y_av = tennis_core.run_all()

        for rid, meta in (y_fac or {}).items():
            rid = str(rid)
            fid = _ns_yongin_id(rid)
            facilities[fid] = meta

        for rid, daymap in (y_av or {}).items():
            rid = str(rid)
            fid = _ns_yongin_id(rid)
            availability[fid] = daymap or {}

    # -----------------------------
    # (B) 고양 (crawl_goyang)
    # -----------------------------
    if target in ("all", "goyang") and is_goyang_crawl_window():
        out_g1 = crawl_goyang.crawl_gytennis()

        # ✅ daehwa는 로그인정보 없으면 스킵(실패로 전체 종료 방지)
        try:
            out_g2 = crawl_goyang.crawl_daehwa()
        except Exception as e:
            print("[DAEHWA] skipped:", e)
            out_g2 = {"facilities": {}, "availability": {}}

        g_fac = {**out_g1.get("facilities", {}), **out_g2.get("facilities", {})}
        g_av = {**out_g1.get("availability", {}), **out_g2.get("availability", {})}

        # -------------------------------------------------
        # 고양 base meta 확보 (코트별 분리 시 title 생성용)
        # -------------------------------------------------
        base_meta_gytennis: Dict[str, dict] = {}   # cv -> meta2
        base_meta_daehwa: Optional[dict] = None

        for raw_fid, meta in (g_fac or {}).items():
            raw_fid = str(raw_fid)
            meta2 = dict(meta or {})
            meta2["title"] = _clean_goyang_title(meta2.get("title", ""))

            if raw_fid.startswith("gy-gytennis-"):
                cv = raw_fid.split("gy-gytennis-", 1)[1]
                base_meta_gytennis[cv] = meta2
                continue

            if raw_fid == "gy-daehwa":
                base_meta_daehwa = meta2
                continue

            # 기타 facility가 있다면 그대로 저장
            fid = f"goyang:{raw_fid}"
            facilities[fid] = meta2

        def _goyang_split_title(meta: dict, suffix: str) -> dict:
            m = dict(meta or {})
            base = _clean_goyang_title(m.get("title", "")).strip()
            m["title"] = f"{base} {suffix}".strip() if suffix else base
            return m

        def _split_daymap_by_court(
            daymap: Dict[str, List[Any]],
            reserve_url_builder,
        ) -> Dict[str, Dict[str, List[dict]]]:
            """
            daymap: {yyyymmdd: [slot(dict)...]}
            return: {courtNo: {yyyymmdd: [slot(dict)...]}}
            reserve_url_builder(slot, yyyymmdd) -> str | None
            """
            out: Dict[str, Dict[str, List[dict]]] = {}
            for yyyymmdd, slots in (daymap or {}).items():
                for s in (slots or []):
                    if not isinstance(s, dict):
                        continue
                    cno = str(s.get("courtNo") or "").strip()
                    if not cno:
                        continue
                    ss = dict(s)
                    url = reserve_url_builder(ss, yyyymmdd)
                    if url:
                        ss["reserveUrl"] = url
                    out.setdefault(cno, {}).setdefault(yyyymmdd, []).append(ss)
            return out

        # -------------------------------------------------
        # availability 처리: 날짜키 변환 + 코트별 facility로 분리
        # -------------------------------------------------
        for raw_fid, daymap in (g_av or {}).items():
            raw_fid = str(raw_fid)

            # ---------- gytennis (courtvalue별) ----------
            if raw_fid.startswith("gy-gytennis-"):
                cv = raw_fid.split("gy-gytennis-", 1)[1]

                # 1) 날짜키 "YYYY-MM-DD" -> "YYYYMMDD" 변환
                new_daymap: Dict[str, List[Any]] = {}
                for ymd, slots in (daymap or {}).items():
                    ymd = str(ymd)
                    yyyymmdd = _ymd_to_yyyymmdd(ymd)
                    if len(yyyymmdd) != 8:
                        continue
                    new_daymap[yyyymmdd] = slots or []

                # 2) reserveUrl 빌더
                def gytennis_url_builder(slot: dict, yyyymmdd: str) -> str:
                    ymd_dash = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
                    return f"https://www.gytennis.or.kr/daily/{cv}/{ymd_dash}"

                # 3) 코트별 분리
                split = _split_daymap_by_court(new_daymap, gytennis_url_builder)

                base_meta = base_meta_gytennis.get(cv, {"title": f"{cv}코트", "location": "고양시", "courtvalue": cv})

                for cno, c_daymap in split.items():
                    fid = f"goyang:gytennis:{cv}:{cno}"
                    facilities[fid] = _goyang_split_title(base_meta, f"{cno}코트")
                    availability[fid] = c_daymap

                continue

            # ---------- daehwa ----------
            if raw_fid == "gy-daehwa":
                # 1) 날짜키 변환
                new_daymap: Dict[str, List[Any]] = {}
                for ymd, slots in (daymap or {}).items():
                    ymd = str(ymd)
                    yyyymmdd = _ymd_to_yyyymmdd(ymd)
                    if len(yyyymmdd) != 8:
                        continue
                    new_daymap[yyyymmdd] = slots or []

                # 2) reserveUrl 빌더
                def daehwa_url_builder(slot: dict, yyyymmdd: str) -> str:
                    return "https://daehwa.gys.or.kr:451/rent/tennis_rent.php"

                # 3) 코트별 분리
                split = _split_daymap_by_court(new_daymap, daehwa_url_builder)

                base_meta = base_meta_daehwa or {"title": "대화", "location": "고양시"}

                for cno, c_daymap in split.items():
                    fid = f"goyang:daehwa:{cno}"
                    facilities[fid] = _goyang_split_title(base_meta, f"{cno}코트")
                    availability[fid] = c_daymap

                continue

            # ---------- 기타 ----------
            fid = f"goyang:{raw_fid}"
            new_daymap: Dict[str, List[Any]] = {}
            for ymd, slots in (daymap or {}).items():
                ymd = str(ymd)
                yyyymmdd = _ymd_to_yyyymmdd(ymd)
                if len(yyyymmdd) != 8:
                    continue
                new_daymap[yyyymmdd] = slots or []
            if new_daymap:
                availability[fid] = new_daymap

    if target in ("all", "goyang") and not is_goyang_crawl_window():
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[GOYANG] skipped outside KST window (05:00~22:00): now={now_kst}")

    return facilities, availability
# =========================================================
# DB write: facilities / availability_cache (프론트용)
# =========================================================
def upsert_facilities_for_frontend(conn: psycopg.Connection, facilities: Dict[str, Any]) -> None:
    ts = utcnow()
    rows = []
    for fid, v in facilities.items():
        fid = str(fid)
        if isinstance(v, dict):
            title = v.get("title") or v.get("name") or v.get("facility_name") or f"RID {fid}"
            location = v.get("location") or ""
        else:
            title = str(v) if v is not None else f"RID {fid}"
            location = ""
        rows.append((fid, title, location, ts))

    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.facilities (facility_id, title, location, updated_at)
            values (%s, %s, %s, %s)
            on conflict (facility_id)
            do update set title=excluded.title, location=excluded.location, updated_at=excluded.updated_at
            """,
            rows,
        )
    conn.commit()

def clear_availability_cache_for_target(
    conn: psycopg.Connection,
    target: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    keep_yongin_today: bool = False,
) -> None:
    """
    타겟별로 해당 기간의 availability_cache를 빈 배열로 초기화한다.
    - DELETE가 아니라 slots_json='[]'로 초기화 (충돌/외래키 안전)
    - target: yongin | goyang | all
    """
    target = (target or "all").strip().lower()

    prefixes = []
    if target in ("all", "yongin"):
        prefixes.append("yongin:%")
    if target in ("all", "goyang"):
        prefixes.append("goyang:%")

    if not prefixes:
        return

    d1 = yyyymmdd_to_date(start_yyyymmdd)
    d2 = yyyymmdd_to_date(end_yyyymmdd)

    today_d = yyyymmdd_to_date(kst_today_yyyymmdd())

    with conn.cursor() as cur:
        # 여러 prefix를 OR로 처리
        where = " OR ".join([f"facility_id LIKE %s" for _ in prefixes])
        extra_guard = ""
        if keep_yongin_today:
            # 용인 당일 데이터는 전날 말미(23:59 근접) 스냅샷을 유지한다.
            extra_guard = " and not (facility_id like %s and date_ymd = %s)"
        params = [d1, d2, *prefixes]
        if keep_yongin_today:
            params.append("yongin:%")
            params.append(today_d)
        cur.execute(
            f"""
            update public.availability_cache
               set slots_json = '[]'::jsonb,
                   updated_at = now()
             where date_ymd between %s and %s
               and ({where})
               {extra_guard}
            """,
            tuple(params),
        )
    conn.commit()

def upsert_availability_cache_for_frontend(
    conn: psycopg.Connection,
    facilities: Dict[str, Any],
    availability: Dict[str, Dict[str, List[Any]]],
    keep_yongin_today: bool = False,
) -> None:
    """
    availability_cache는 프론트가 /api/data 대신 읽는 용도.
    slots_json은 [{"timeContent":"..","resveId":".."}, ...]
    """
    ts = utcnow()
    rows = []

    today_ymd = kst_today_yyyymmdd()

    for fid, day_map in availability.items():
        fid = str(fid)
        for ymd, slots in (day_map or {}).items():
            ymd = (ymd or "").strip()
            if len(ymd) != 8:
                continue
            if keep_yongin_today and fid.startswith("yongin:") and ymd == today_ymd:
                # 당일 용인 데이터는 기존 스냅샷 유지(전날 23:59 근접값)
                continue
            d = yyyymmdd_to_date(ymd)

            # reserve link의 resveId는 슬롯별로 더 정확할 수 있지만,
            # 최소한 "시설 id로 링크 생성"도 가능하게 fallback 처리
            fallback_resve_id = _strip_yongin_resve_id(fid)

            arr = [slot_obj_for_frontend(s, fallback_resve_id) for s in (slots or []) if slot_key_from_time(s)]
            rows.append((fid, d, json.dumps(arr, ensure_ascii=False), ts))

    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.availability_cache (facility_id, date_ymd, slots_json, updated_at)
            values (%s, %s, %s::jsonb, %s)
            on conflict (facility_id, date_ymd)
            do update set slots_json=excluded.slots_json, updated_at=excluded.updated_at
            """,
            rows,
        )
    conn.commit()




# =========================================================
# DB read (batch)
# =========================================================
def load_push_subscriptions(conn: psycopg.Connection, sub_ids: List[str]) -> Dict[str, dict]:
    """
    push_subscriptions: id(subscription_id) -> pywebpush subscription_info
    """
    if not sub_ids:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select id, endpoint, p256dh, auth
            from public.push_subscriptions
            where id = any(%s)
            """,
            (sub_ids,),
        )
        rows = cur.fetchall()

    m = {}
    for r in rows:
        sid = r["id"]
        m[sid] = {"endpoint": r["endpoint"], "keys": {"p256dh": r["p256dh"], "auth": r["auth"]}}
    return m


def load_alarms(conn: psycopg.Connection) -> List[dict]:
    """
    alarms: subscription_id, court_group, date(text)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select subscription_id, court_group, date
            from public.alarms
            """
        )
        return cur.fetchall()


def cleanup_expired_alarms(conn: psycopg.Connection) -> int:
    """
    KST 기준 오늘보다 과거 날짜 알람을 자동 삭제한다.
    alarms.date는 YYYY-MM-DD 또는 YYYYMMDD 모두 허용한다.
    """
    today_ymd = kst_today_yyyymmdd()
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from public.alarms
             where replace(coalesce(date, ''), '-', '') ~ '^[0-9]{8}$'
               and replace(date, '-', '') < %s
            """,
            (today_ymd,),
        )
        deleted = cur.rowcount or 0
    conn.commit()
    return deleted


def preload_baseline(conn: psycopg.Connection, keys: List[Tuple[str, str, str]]) -> Dict[Tuple[str, str, str], Set[str]]:
    """
    baseline_slots: subscription_id, court_group, date(char/text), time_content
    keys: [(sub_id, court_group, yyyymmdd), ...]
    return: key -> set(time_content)
    """
    if not keys:
        return {}

    # UNNEST로 3열 join
    sub_ids = [k[0] for k in keys]
    groups = [k[1] for k in keys]
    dates = [k[2] for k in keys]

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with q as (
              select * from unnest(%s::text[], %s::text[], %s::text[])
                as t(subscription_id, court_group, date)
            )
            select b.subscription_id, b.court_group, b.date, b.time_content
            from public.baseline_slots b
            join q
              on q.subscription_id = b.subscription_id
             and q.court_group = b.court_group
             and q.date = b.date
            """,
            (sub_ids, groups, dates),
        )
        rows = cur.fetchall()

    m: Dict[Tuple[str, str, str], Set[str]] = {}
    for r in rows:
        key = (r["subscription_id"], r["court_group"], r["date"])
        m.setdefault(key, set()).add(r["time_content"])
    return m


def insert_baseline(conn: psycopg.Connection, sub_id: str, court_group: str, ymd: str, slot_keys: Iterable[str]) -> None:
    """
    baseline_slots에 현재 슬롯을 baseline으로 박아 넣음.
    (첫 실행/첫 알람 등록 시 기존 슬롯은 알림 안 보내기용)
    """
    rows = []
    for sk in slot_keys:
        if not sk:
            continue
        rows.append((sub_id, court_group, ymd, sk))

    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.baseline_slots (subscription_id, court_group, date, time_content)
            values (%s, %s, %s, %s)
            on conflict do nothing
            """,
            rows,
        )
    conn.commit()


def preload_sent_slots(conn: psycopg.Connection, sub_ids: List[str], slot_keys: List[str]) -> Dict[str, Set[str]]:
    """
    sent_slots: subscription_id, slot_key
    한번에 preload해서 per-user DB hit 제거
    """
    if not sub_ids or not slot_keys:
        return {sid: set() for sid in sub_ids}

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select subscription_id, slot_key
            from public.sent_slots
            where subscription_id = any(%s)
              and slot_key = any(%s)
            """,
            (sub_ids, slot_keys),
        )
        rows = cur.fetchall()

    m: Dict[str, Set[str]] = {sid: set() for sid in sub_ids}
    for r in rows:
        m.setdefault(r["subscription_id"], set()).add(r["slot_key"])
    return m


def bulk_mark_sent(conn: psycopg.Connection, sent_rows: List[Tuple[str, str]]) -> None:
    """
    sent_slots bulk insert
    sent_rows: [(subscription_id, slot_key), ...]
    """
    if not sent_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.sent_slots (subscription_id, slot_key, sent_at)
            values (%s, %s, now())
            on conflict do nothing
            """,
            sent_rows,
        )
    conn.commit()


def bulk_upsert_slots_snapshot(conn: psycopg.Connection, snapshot_rows: List[Tuple[str, date, str]]) -> None:
    """
    slots_snapshot: facility_id, date_ymd, slot_key
    (first_seen_at/last_seen_at 갱신)
    """
    if not snapshot_rows:
        return
    ts = utcnow()
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.slots_snapshot (facility_id, date_ymd, slot_key, first_seen_at, last_seen_at)
            values (%s, %s, %s, %s, %s)
            on conflict (facility_id, date_ymd, slot_key)
            do update set last_seen_at = excluded.last_seen_at
            """,
            [(fid, d, sk, ts, ts) for (fid, d, sk) in snapshot_rows],
        )
    conn.commit()

def compute_minmax_yyyymmdd(availability: Dict[str, Dict[str, List[Any]]]) -> Tuple[str, str] | None:
    ys = []
    for _, day_map in (availability or {}).items():
        for ymd in (day_map or {}).keys():
            ymd = str(ymd)
            if len(ymd) == 8 and ymd.isdigit():
                ys.append(ymd)
    if not ys:
        return None
    ys.sort()
    return ys[0], ys[-1]


def count_slots_for_prefix(availability: Dict[str, Dict[str, List[Any]]], prefix: str) -> int:
    total = 0
    for fid, day_map in (availability or {}).items():
        if not str(fid).startswith(prefix):
            continue
        for slots in (day_map or {}).values():
            total += len(slots or [])
    return total
# =========================================================
# Push
# =========================================================
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


# =========================================================
# Main
# =========================================================
def main() -> None:
    target = (os.getenv("RUN_TARGET") or "all").strip().lower()
    print(f"[RUN] target={target}")
    database_url = os.environ["DATABASE_URL"]
    _ = os.environ.get("VAPID_PUBLIC_KEY", "")
    _ = os.environ["VAPID_PRIVATE_KEY"]
    _ = os.environ["VAPID_SUBJECT"]

    ts_now = utcnow()

    # 결과 누적(마지막에 bulk flush)
    snapshot_rows: List[Tuple[str, date, str]] = []   # (facility_id, date_ymd, slot_key)
    sent_rows: List[Tuple[str, str]] = []            # (subscription_id, slot_key)

    with psycopg.connect(database_url) as conn:
        # 프론트용 테이블만 (없으면) 추가 생성
        ensure_extra_schema(conn)
        deleted_alarms = cleanup_expired_alarms(conn)
        if deleted_alarms:
            print(f"[ALARMS] expired deleted={deleted_alarms} (KST<{kst_today_yyyymmdd()})")
        
        # 1) 크롤링
        facilities, availability = crawl_all()
        print(f"[INFO] crawled facilities={len(availability)}")

        # ✅ 이번 실행 타겟/기간에 해당하는 캐시를 먼저 비워두고(빈 배열),
        #    아래 upsert에서 다시 채운다.
        goyang_slots = count_slots_for_prefix(availability, "goyang:gytennis:")
        protect_goyang_cache = target in ("all", "goyang") and goyang_slots == 0
        if protect_goyang_cache:
            print("[GOYANG][SAFEGUARD] gytennis slots=0; keep existing goyang cache")

        facilities_for_write = facilities
        availability_for_write = availability
        clear_target = target
        if protect_goyang_cache:
            if target == "all":
                clear_target = "yongin"
                facilities_for_write = {k: v for k, v in facilities.items() if not str(k).startswith("goyang:")}
                availability_for_write = {k: v for k, v in availability.items() if not str(k).startswith("goyang:")}
            else:
                clear_target = ""
                facilities_for_write = {}
                availability_for_write = {}

        mm = compute_minmax_yyyymmdd(availability)
        if mm:
            start_ymd, end_ymd = mm
            print(f"[CACHE] clear target={clear_target or '(skip)'} range={start_ymd}~{end_ymd}")
            clear_availability_cache_for_target(
                conn,
                clear_target,
                start_ymd,
                end_ymd,
                keep_yongin_today=(clear_target in ("all", "yongin")),
            )
        else:
            print("[CACHE] skip clear: no dates in availability")

        # 2) 프론트용 저장 (시설/availability_cache)
        try:
            upsert_facilities_for_frontend(conn, facilities_for_write)
            upsert_availability_cache_for_frontend(
                conn,
                facilities_for_write,
                availability_for_write,
                keep_yongin_today=(clear_target in ("all", "yongin")),
            )
        except Exception as e:
            # 프론트용 저장이 실패해도 알림은 계속 수행할 수 있게 한다
            print(f"[WARN] frontend cache save failed: {e}")

        # 3) alarms preload
        alarms = load_alarms(conn)
        if not alarms:
            print("[SUMMARY] alarms=0 (no work)")
            return

        # 4) subscription_id 목록 -> push_subscriptions preload
        sub_ids = sorted({str(a["subscription_id"]) for a in alarms if a.get("subscription_id")})
        push_map = load_push_subscriptions(conn, sub_ids)

        # 5) alarms를 (subscription_id, court_group, yyyymmdd) 키로 정규화
        alarm_keys: List[Tuple[str, str, str]] = []
        for a in alarms:
            sid = str(a["subscription_id"])
            group = (a.get("court_group") or "").strip()
            ymd = to_yyyymmdd(a.get("date") or "")
            if not sid or not group or len(ymd) != 8:
                continue
            alarm_keys.append((sid, group, ymd))

        if not alarm_keys:
            print("[SUMMARY] alarms valid=0 (bad date/group)")
            return

        # 6) baseline preload (한번에)
        baseline_map = preload_baseline(conn, alarm_keys)

        # 7) court_group별로, 해당 날짜 슬롯을 빠르게 모으기 위한 인덱스
        # group -> facility_ids
        group_to_fids: Dict[str, List[str]] = {}
        for fid, meta in facilities.items():
            fid = str(fid)
            title = meta.get("title") if isinstance(meta, dict) else str(meta)
            g = get_court_group(title, fid)
            group_to_fids.setdefault(g, []).append(fid)

        # 8) 모든 알람에서 "현재 슬롯 후보(slot_key)"를 먼저 모아서
        #    sent_slots를 한번에 preload 하기 위한 candidate set 생성
        #    (sent_slots preload는 subscription_id+slot_key 조합을 batch로 한 번에)
        key_to_current_slots: Dict[Tuple[str, str, str], Set[str]] = {}
        all_candidate_slot_keys: Set[str] = set()

        for (sid, group, ymd) in alarm_keys:
            fids = group_to_fids.get(group, [])
            if not fids:
                key_to_current_slots[(sid, group, ymd)] = set()
                continue

            cur_slots: Set[str] = set()
            for fid in fids:
                day_map = availability.get(str(fid)) or {}
                slots = day_map.get(ymd) or []
                for t in slots:
                    sk = slot_key_from_time(t)
                    if sk:
                        cur_slots.add(sk)

                        # sent_slots는 slot_key만 저장하니까
                        # (facility 구분까지 필요하면 slot_key 설계를 더 고유하게 해야 함)
                        all_candidate_slot_keys.add(sk)

            key_to_current_slots[(sid, group, ymd)] = cur_slots

        # 9) sent_slots preload (한 번에)
        sent_map = preload_sent_slots(conn, sub_ids, sorted(all_candidate_slot_keys))

        # 10) 알림 처리
        push_requests = 0
        baseline_inserts = 0
        added_total = 0

        for (sid, group, ymd) in alarm_keys:
            cur_slots = key_to_current_slots.get((sid, group, ymd), set())

            # push 구독이 없으면 스킵
            sub_info = push_map.get(sid)
            if not sub_info:
                continue

            # baseline이 없다면: "첫 실행"으로 보고 baseline 저장 후 알림 스킵
            base_key = (sid, group, ymd)
            baseline = baseline_map.get(base_key)
            if not baseline:
                if cur_slots:
                    insert_baseline(conn, sid, group, ymd, cur_slots)
                    baseline_inserts += 1
                    baseline_map[base_key] = set(cur_slots)
                continue

            # baseline 이후 신규 슬롯 = cur - baseline
            added = cur_slots - baseline
            if not added:
                continue

            # 이미 보낸 슬롯 제외
            already_sent = sent_map.get(sid, set())
            to_send = [sk for sk in sorted(added) if sk not in already_sent]
            if not to_send:
                continue

            # 푸시 메시지 구성
            # 너무 길면 잘라서 요약
            preview = ", ".join(to_send[:6])
            more = "" if len(to_send) <= 6 else f" 외 {len(to_send)-6}개"

            title = "🎾 예약 오픈"
            body = f"{group} {ymd[4:6]}/{ymd[6:8]} 신규 슬롯: {preview}{more}"

            try:
                send_push(sub_info, title, body)
                push_requests += 1
                added_total += len(to_send)

                # sent_slots bulk insert를 위해 누적
                for sk in to_send:
                    sent_rows.append((sid, sk))
                    sent_map.setdefault(sid, set()).add(sk)

            except WebPushException as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                print(f"[PUSH_FAIL] sid={sid} group={group} date={ymd} status={code} err={e}")

        # 11) slots_snapshot 갱신(시설/날짜 단위로는 availability 기준으로 계속 갱신)
        #     (이 테이블은 “원본 슬롯 변화 기록” 용도라 alarms와 별개로 유지 가능)
        for fid, day_map in (availability or {}).items():
            fid = str(fid)
            for ymd, slots in (day_map or {}).items():
                if not ymd or len(ymd) != 8:
                    continue
                d = yyyymmdd_to_date(ymd)
                for t in (slots or []):
                    sk = slot_key_from_time(t)
                    if sk:
                        snapshot_rows.append((fid, d, sk))

        # 12) bulk flush
        try:
            bulk_upsert_slots_snapshot(conn, snapshot_rows)
        except Exception as e:
            print(f"[WARN] slots_snapshot bulk upsert failed: {e}")

        try:
            bulk_mark_sent(conn, sent_rows)
        except Exception as e:
            print(f"[WARN] sent_slots bulk insert failed: {e}")

        print(f"[SUMMARY] alarms={len(alarm_keys)} baseline_inserts={baseline_inserts} push_requests={push_requests} sent_slots_added={added_total}")


if __name__ == "__main__":
    main()
