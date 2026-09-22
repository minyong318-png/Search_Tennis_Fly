"""Observe normal Goyang requests, independently parse them, and verify saved slots."""
import argparse
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lxml import html as html_parser
import requests

import crawl_goyang as crawl
import refresh_and_notify as refresh


OUTPUT = Path(__file__).resolve().parents[1] / "debug/gytennis/source-audit.json"
TIME = re.compile(r"(\d{1,2}):\s*(\d{2})\s*[~-]\s*(\d{1,2}):\s*(\d{2})")
GYT_URL = re.compile(r"https://www\.gytennis\.or\.kr/daily/(\d+)/(\d{4}-\d{2}-\d{2})(?:[?#]|$)")
DAEHWA_COURTS = {"2": "1", "7": "2", "8": "3", "9": "4"}
CLOSURE_PHRASES = ("휴관", "휴장", "정기휴무", "휴무일", "예약불가", "예약 불가", "접수마감", "접수 마감", "예약기간", "예약 기간", "선택하세요", "선택해", "회원전용", "로그인후 이용", "이용할 수 없", "이용 불가")


def has_class(name):
    return "contains(concat(' ', normalize-space(@class), ' '), ' " + name + " ')"


def text(node):
    return " ".join(node.text_content().split())


def slot_time(value):
    match = TIME.search(value)
    if not match:
        raise ValueError("time_label_missing")
    h1, m1, h2, m2 = match.groups()
    return f"{int(h1):02}:{m1}~{int(h2):02}:{m2}"


def response_html(response):
    return getattr(response, "content", None) or response.text


def selected_place(doc):
    values = doc.xpath('//select[@name="place_opt"]/option[@selected]/@value')
    values += doc.xpath('//input[@name="place_opt" and (@type="hidden" or not(@type) or @checked)]/@value')
    if not any(values):
        values += doc.xpath('//select[@name="place_opt"]/option[string-length(@value)>0]/@value')[:1]
    return next((value.strip() for value in values if value.strip()), "")


def parse_gyt(doc, group, date):
    if doc.xpath('//input[@name="cdate"]/@value') != [date]:
        raise ValueError("date_mismatch")
    times = [slot_time(text(cell)) for cell in doc.xpath('//table[' + has_class('custom') + ']//td[' + has_class('wide') + ']')]
    tables = doc.xpath('//table[' + has_class('innerCustom') + ']')
    if not times or not tables:
        raise ValueError("timetable_missing")
    units = {}
    for table in tables:
        labels = table.xpath('.//td[' + has_class('courtTag') + ']')
        number = re.search(r"\d+", text(labels[0])) if labels else None
        cells = table.xpath('.//td[' + has_class('resTag') + ']')
        if not number or len(cells) != len(times):
            raise ValueError("incomplete_timetable")
        slots = Counter()
        for time, cell in zip(times, cells):
            if cell.xpath('.//span[' + has_class('public-empty-slot') + ']') or cell.xpath('.//input[@type="checkbox" and not(@disabled)]'):
                slots[time] += 1
        units[(f"goyang:gytennis:{group}:{number.group()}", date)] = slots
    return units


def parse_gys(doc, source, date, place):
    dates = doc.xpath('//input[@name="rent_date"]/@value')
    if not dates or re.sub(r"\D", "", dates[0]) != date.replace("-", ""):
        raise ValueError("date_mismatch")
    tables = doc.xpath('//table[contains(@summary,"이용신청 테이블")]')
    if not tables or not TIME.search(text(tables[0])):
        raise ValueError("timetable_missing")
    selected = selected_place(doc)
    if source == "daehwa":
        if selected and selected != place:
            raise ValueError("court_mismatch")
        court = DAEHWA_COURTS[place]
    else:
        if place and selected and selected != place:
            raise ValueError("court_mismatch")
        court = selected or place or "1"
    if not court.isdigit():
        raise ValueError("court_number_invalid")
    slots = Counter()
    for row in tables[0].xpath('.//tr'):
        enabled = row.xpath('.//input[@name="rent_chk[]" and not(@disabled)]')
        enabled = [node for node in enabled if not re.fullmatch(r"\d{8}", node.get("value", "").strip())]
        if enabled:
            slots[slot_time(text(row))] += 1
    return {(f"goyang:{source}:{court}", date): slots}


class Audit:
    def __init__(self):
        self.expected = {}
        self.pages = []
        self.failures = {}
        self.summary = {"started_at": datetime.now(timezone.utc).isoformat(), "status": "running", "sources": {}}

    def capture(self, source, content, status, date, place="", group=""):
        record = {"source": source, "date": date, "http_status": status}
        key = (source, date, group, place)
        if place.isdigit():
            record["requested_place"] = place
        if group:
            record["court_group"] = group
        try:
            doc = html_parser.fromstring(content)
            record["date_place_fields"] = sorted(set(node.tag + ":" + node.get("name") for node in doc.xpath('//*[@name="rent_date" or @name="place_opt"]')))
            record["observed_dates"] = [value for value in doc.xpath('//input[@name="rent_date" or @name="cdate"]/@value') if re.fullmatch(r"\d{4}[-/.]?\d{2}[-/.]?\d{2}", value)]
            record["table_summaries"] = sorted(set(doc.xpath('//table/@summary')))
            record["place_options"] = [
                {"id": node.get("value"), "label": text(node)[:80], "selected": "selected" in node.attrib}
                for node in doc.xpath('//select[@name="place_opt"]/option')
                if re.fullmatch(r"\d*", node.get("value", ""))
            ]
            record["empty_timetable_text"] = [
                text(table)[:400] for table in doc.xpath('//table[contains(@summary,"이용신청")]')
                if not TIME.search(text(table)) and not table.xpath('.//input[@name="rent_chk[]"]')
            ]
            body = text(doc)
            record["closure_phrases"] = [phrase for phrase in CLOSURE_PHRASES if phrase in body]
            if status != 200:
                raise ValueError("http_status")
            units = parse_gyt(doc, group, date) if source == "gytennis" else parse_gys(doc, source, date, place)
            self.expected.update(units)
            record["parsed_units"] = len(units)
            record["parsed_slots"] = sum(slots.total() for slots in units.values())
            self.failures.pop(key, None)
        except Exception as exc:
            # No exception text or arbitrary form values: either can contain account data.
            record["parse_failure"] = type(exc).__name__
            self.failures[key] = record
        self.pages.append(record)

    def install(self, stack, include_gyt=True):
        if include_gyt:
            def observe_get(original):
                def wrapped(url, *args, **kwargs):
                    response = original(url, *args, **kwargs)
                    match = GYT_URL.match(str(url))
                    if match:
                        self.capture("gytennis", response_html(response), response.status_code, match[2], group=match[1])
                    return response
                return wrapped
            if crawl.curl_requests is not None:
                stack.enter_context(patch.object(crawl.curl_requests, "get", observe_get(crawl.curl_requests.get)))
            original_get = requests.Session.get
            def session_get(session, url, *args, **kwargs):
                return observe_get(lambda address, *a, **kw: original_get(session, address, *a, **kw))(url, *args, **kwargs)
            stack.enter_context(patch.object(requests.Session, "get", session_get))

        original_daehwa = crawl.post_rent
        def daehwa(session, payload, state):
            result = original_daehwa(session, payload, state)
            date = str(payload["rent_date"])
            self.capture("daehwa", result[0], result[2], f"{date[:4]}-{date[4:6]}-{date[6:]}", str(payload["place_opt"]))
            return result
        stack.enter_context(patch.object(crawl, "post_rent", daehwa))
        original_baekseok = crawl._baekseok_post
        def baekseok(session, payload):
            response = original_baekseok(session, payload)
            date = str(payload["rent_date"])
            self.capture("baekseok", response_html(response), response.status_code, f"{date[:4]}-{date[4:6]}-{date[6:]}", str(payload["place_opt"]))
            return response
        stack.enter_context(patch.object(crawl, "_baekseok_post", baekseok))

    def verify_database(self):
        self.summary["unresolved_responses"] = list(self.failures.values())
        if self.failures:
            raise AssertionError("Source snapshots could not all be independently parsed")
        if not self.expected:
            raise AssertionError("No source snapshots captured")
        dates = [date for _, date in self.expected]
        with refresh.psycopg.connect(os.environ["DATABASE_URL"], options="-c default_transaction_read_only=on") as conn:
            with conn.cursor() as cur:
                cur.execute("select facility_id, date_ymd, slots_json, query_status, availability_status, checked_at from public.availability_cache where facility_id like 'goyang:%%' and date_ymd between %s and %s", (min(dates), max(dates)))
                rows = cur.fetchall()
        actual, invalid_rows = {}, []
        for fid, day, slots, query_status, availability_status, checked_at in rows:
            key = (fid, day.isoformat())
            if isinstance(slots, str):
                slots = json.loads(slots)
            actual[key] = Counter(slot_time(slot["timeContent"]) for slot in slots)
            if key in self.expected and (query_status != "success" or checked_at is None or availability_status != ("available" if slots else "confirmed_empty")):
                invalid_rows.append(key)
            if any(str(slot.get("courtNo")) != fid.rsplit(":", 1)[-1] for slot in slots):
                invalid_rows.append(key)
        missing_rows = sorted(set(self.expected) - set(actual))
        differences = []
        for key in sorted(set(self.expected) | set(actual)):
            wanted, saved = self.expected.get(key, Counter()), actual.get(key, Counter())
            if wanted != saved:
                differences.append({"facility_id": key[0], "date": key[1], "missing": dict(wanted-saved), "extra": dict(saved-wanted)})
        self.summary.update({"missing_rows": missing_rows, "invalid_rows": invalid_rows, "differences": differences, "source_units": len(self.expected), "database_rows": len(actual)})
        for source in ("gytennis", "daehwa", "baekseok"):
            prefix = f"goyang:{source}:"
            source_units = {key: slots for key, slots in self.expected.items() if key[0].startswith(prefix)}
            self.summary["sources"][source] = {"facilities": len({fid for fid, _ in source_units}), "dates": len({day for _, day in source_units}), "units": len(source_units), "source_slots": sum(slots.total() for slots in source_units.values()), "saved_slots": sum(slots.total() for (fid, _), slots in actual.items() if fid.startswith(prefix))}
        if missing_rows or invalid_rows or differences or any(not value["units"] for value in self.summary["sources"].values()):
            raise AssertionError("Source/DB audit mismatch; see source-audit.json")

    def save(self):
        self.summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.summary["response_diagnostics"] = self.pages
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps(self.summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[SOURCE_AUDIT] " + json.dumps({key: value for key, value in self.summary.items() if key != "response_diagnostics"}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspect-gys", action="store_true", help="Inspect GYS source responses without any DB or GYT requests")
    args = parser.parse_args()
    audit = Audit()
    audit.summary["mode"] = "inspect-gys" if args.inspect_gys else "verify"
    try:
        with ExitStack() as stack:
            audit.install(stack, include_gyt=not args.inspect_gys)
            if args.inspect_gys:
                for source, collect in (("daehwa", crawl.crawl_daehwa), ("baekseok", crawl.crawl_baekseok)):
                    try:
                        result = collect()
                        audit.summary["sources"][source] = {"partial_failure": bool(result.get("partial_failure"))}
                    except Exception as exc:
                        audit.summary["sources"][source] = {"partial_failure": True, "error_type": type(exc).__name__}
                audit.summary["status"] = "inspected"
            else:
                if os.environ.get("RUN_TARGET", "").lower() != "goyang":
                    raise ValueError("Audit requires RUN_TARGET=goyang")
                refresh.main()
                audit.verify_database()
                audit.summary["status"] = "pass"
    except BaseException as exc:
        audit.summary.update(status="failed", error_type=type(exc).__name__)
        raise
    finally:
        original_error = sys.exc_info()[0]
        try:
            audit.save()
        except Exception as exc:
            print(f"[SOURCE_AUDIT] artifact save failed: {type(exc).__name__}")
            if original_error is None:
                raise


if __name__ == "__main__":
    main()
