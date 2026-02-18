# benchmark_55s.py
from __future__ import annotations

import argparse
import asyncio
import calendar
import json
import random
import statistics
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import aiohttp

from tennis_core import get_connector, HEADERS, init_session, fetch_times


def build_datevals(days_ahead: int) -> List[str]:
    """
    오늘 제외, 내일부터 days_ahead일치 dateVal 리스트 생성 (YYYYMMDD)
    """
    today = datetime.today()
    start = today + timedelta(days=1)
    out = []
    for i in range(days_ahead):
        d = start + timedelta(days=i)
        out.append(d.strftime("%Y%m%d"))
    return out


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    vs = sorted(values)
    k = (len(vs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(vs) - 1)
    if f == c:
        return vs[f]
    return vs[f] + (vs[c] - vs[f]) * (k - f)


async def run_timeboxed(
    rids: List[str],
    datevals: List[str],
    timebox_s: float,
    concurrency: int,
    per_request_timeout_s: float,
    shuffle: bool,
) -> Dict:
    """
    55초(기본) 동안 가능한 한 많은 (rid, dateVal) 조합을 처리.
    여기서 "크롤링 1개" = fetch_times(POST) 1회로 카운팅.
    """
    # 작업 큐 구성
    jobs: List[Tuple[str, str]] = [(rid, dv) for rid in rids for dv in datevals]
    if shuffle:
        random.shuffle(jobs)

    q: asyncio.Queue[Tuple[str, str]] = asyncio.Queue()
    for j in jobs:
        q.put_nowait(j)

    start = time.perf_counter()
    deadline = start + timebox_s

    ok = 0
    fail = 0
    durations: List[float] = []
    fail_reasons: Dict[str, int] = {}

    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(
        connector=get_connector(),
        headers=HEADERS,
        timeout=aiohttp.ClientTimeout(total=per_request_timeout_s),
    ) as session:
        await init_session(session)

        async def one() -> None:
            nonlocal ok, fail
            while True:
                # 타임박스 종료
                if time.perf_counter() >= deadline:
                    return

                try:
                    rid, dv = q.get_nowait()
                except asyncio.QueueEmpty:
                    return

                # 남은 시간이 너무 없으면 시작하지 않음(마지막 타임아웃 방지)
                if time.perf_counter() + 0.3 >= deadline:
                    return

                async with sem:
                    t0 = time.perf_counter()
                    try:
                        lst = await fetch_times(session, dv, rid)  # tennis_core 함수 그대로 사용
                        dt = time.perf_counter() - t0
                        durations.append(dt)
                        if lst:
                            ok += 1
                        else:
                            # 빈 리스트도 "응답은 성공, 예약 가능 시간 없음"일 수 있음
                            ok += 1
                    except asyncio.TimeoutError:
                        dt = time.perf_counter() - t0
                        durations.append(dt)
                        fail += 1
                        fail_reasons["timeout"] = fail_reasons.get("timeout", 0) + 1
                    except Exception as e:
                        dt = time.perf_counter() - t0
                        durations.append(dt)
                        fail += 1
                        key = type(e).__name__
                        fail_reasons[key] = fail_reasons.get(key, 0) + 1

        # 워커 여러 개 실행 (큐는 공유, 실제 동시성은 semaphore가 제어)
        workers = [asyncio.create_task(one()) for _ in range(max(1, concurrency))]
        await asyncio.gather(*workers)

    elapsed = time.perf_counter() - start
    completed = ok + fail

    return {
        "timebox_s": timebox_s,
        "elapsed_s": round(elapsed, 3),
        "concurrency": concurrency,
        "jobs_total": len(jobs),
        "completed_requests": completed,        # (= POST 횟수)
        "ok_requests": ok,
        "fail_requests": fail,
        "fail_reasons": fail_reasons,
        "avg_s": round(statistics.mean(durations), 3) if durations else 0.0,
        "p50_s": round(percentile(durations, 50), 3) if durations else 0.0,
        "p95_s": round(percentile(durations, 95), 3) if durations else 0.0,
        "rids": len(rids),
        "datevals": len(datevals),
    }


def load_rids_from_cache(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        facilities = json.load(f)  # {rid: {title, location}}
    return list(facilities.keys())


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--facilities-cache", default="facilities_cache.json")
    p.add_argument("--rid-sample", type=int, default=20, help="테스트에 사용할 시설(rid) 샘플 개수")
    p.add_argument("--days", type=int, default=7, help="내일부터 며칠치 dateVal을 만들지")
    p.add_argument("--timebox", type=float, default=55.0)
    p.add_argument("--timeout", type=float, default=8.0, help="요청 1개당 타임아웃(초)")
    p.add_argument("--concurrency", type=int, default=5)
    p.add_argument("--shuffle", action="store_true")
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--seed", type=int, default=None, help="랜덤 시드(시설 샘플 고정)")

    return p.parse_args()


async def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    rids = load_rids_from_cache(args.facilities_cache)
    random.shuffle(rids)
    rids = rids[: args.rid_sample]

    datevals = build_datevals(args.days)

    runs = []
    for i in range(args.repeat):
        res = await run_timeboxed(
            rids=rids,
            datevals=datevals,
            timebox_s=args.timebox,
            concurrency=args.concurrency,
            per_request_timeout_s=args.timeout,
            shuffle=args.shuffle,
        )
        res["run"] = i + 1
        runs.append(res)
        print(json.dumps(res, ensure_ascii=False))

    completed = [r["completed_requests"] for r in runs]
    p95 = [r["p95_s"] for r in runs]
    summary = {
        "repeat": args.repeat,
        "concurrency": args.concurrency,
        "rid_sample": args.rid_sample,
        "days": args.days,
        "timebox_s": args.timebox,
        "completed_avg_per_55s": round(statistics.mean(completed), 2),
        "completed_min": min(completed),
        "completed_max": max(completed),
        "p95_s_avg": round(statistics.mean(p95), 3),
    }
    print("SUMMARY:", json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
