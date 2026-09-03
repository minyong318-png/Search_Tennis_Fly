import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import path from "node:path";

const COLLECTOR = "android_chrome_cdp";
const BASE_URL = "https://res.isdc.co.kr";
const LIST_URL = `${BASE_URL}/facilityList.do?facType=29`;
const CDP_HOST = "http://127.0.0.1:9222";
const DEFAULT_PAGE_FETCH_TIMEOUT_MS = 10000;
const DEFAULT_ANDROID_TIMEOUT_MS = 300000;
const DEFAULT_ANDROID_CONCURRENCY = 8;
const DEFAULT_ANDROID_BATCH_SIZE = 36;
const DEFAULT_ANDROID_REQUEST_DELAY_MS = 150;
const DEFAULT_ANDROID_FETCH_TIMEOUT_MS = 12000;
const DEFAULT_ANDROID_FETCH_RETRIES = 1;
const SENSITIVE_HEADERS = new Set(["cookie", "set-cookie", "authorization", "proxy-authorization"]);
const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SCRIPT_DIR, "..");
const OUTER_ROOT = path.resolve(REPO_ROOT, "..");

export function numberEnv(name, fallback, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const value = process.env[name];
  if (value === undefined || String(value).trim() === "") return fallback;
  const raw = Number(value);
  if (!Number.isFinite(raw)) return fallback;
  return Math.max(min, Math.min(raw, max));
}

export function androidRequestConfig() {
  return {
    concurrency: numberEnv("SEONGNAM_ANDROID_CONCURRENCY", DEFAULT_ANDROID_CONCURRENCY, 1, 20),
    batchSize: numberEnv("SEONGNAM_ANDROID_BATCH_SIZE", DEFAULT_ANDROID_BATCH_SIZE, 1, 1000),
    requestDelayMs: numberEnv("SEONGNAM_ANDROID_REQUEST_DELAY_MS", DEFAULT_ANDROID_REQUEST_DELAY_MS, 0, 5000),
    fetchTimeoutMs: numberEnv("SEONGNAM_ANDROID_FETCH_TIMEOUT_MS", DEFAULT_ANDROID_FETCH_TIMEOUT_MS, 1000, 60000),
    fetchRetries: numberEnv("SEONGNAM_ANDROID_FETCH_RETRIES", DEFAULT_ANDROID_FETCH_RETRIES, 0, 3),
  };
}

export function pageFetchEvaluateTimeoutMs(config = androidRequestConfig()) {
  const timeoutMs = Number(config.fetchTimeoutMs || DEFAULT_ANDROID_FETCH_TIMEOUT_MS);
  const retries = Number(config.fetchRetries || 0);
  return Math.max(5000, timeoutMs * (retries + 1) + 10000);
}

function nowIso() {
  return new Date().toISOString();
}

function ymd(days = 0) {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
}

function ymdDash(value) {
  const s = normalizeYmd(value);
  return `${s.slice(0, 4)}-${Number(s.slice(4, 6))}-${Number(s.slice(6, 8))}`;
}

function normalizeYmd(value) {
  return String(value || "").replace(/\D/g, "").slice(0, 8);
}

function weekdayOfYmd(value) {
  const ymdValue = normalizeYmd(value);
  if (ymdValue.length !== 8) return null;
  const date = new Date(
    Number(ymdValue.slice(0, 4)),
    Number(ymdValue.slice(4, 6)) - 1,
    Number(ymdValue.slice(6, 8))
  );
  return date.getDay();
}

export function shouldQueryCourtDate(title, dateYmd, todayYmd = ymd(0)) {
  const text = String(title || "");
  const date = normalizeYmd(dateYmd);
  if (!date) return true;
  if (text.includes("당일예약")) return date === normalizeYmd(todayYmd);
  const weekday = weekdayOfYmd(date);
  if (weekday === null) return true;
  if (text.includes("일요일")) return weekday === 0;
  if (text.includes("토요일") || text.includes("공휴일")) return weekday === 6;
  if (text.includes("평일")) return weekday >= 1 && weekday <= 5;
  return true;
}

function cleanText(value) {
  return String(value || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function ok(payload) {
  return {
    status: "ok",
    collector: COLLECTOR,
    device_serial: maskSerial(payload.deviceSerial || ""),
    page_url: payload.pageUrl || "",
    collected_at: nowIso(),
    facilities: payload.facilities || [],
    slots: payload.slots || [],
    unavailable_dates: payload.unavailableDates || [],
    diagnostics: payload.diagnostics || {},
  };
}

function fail(status, message, extra = {}) {
  return {
    status,
    collector: COLLECTOR,
    message,
    ...extra,
  };
}

function maskSerial(serial) {
  const raw = String(serial || "");
  if (!raw) return "";
  if (raw.length <= 4) return "masked";
  return `${raw.slice(0, 2)}***${raw.slice(-2)}`;
}

function execFileText(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    execFile(command, args, { timeout: 15000, windowsHide: true, ...options }, (error, stdout, stderr) => {
      if (error) {
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
        return;
      }
      resolve(String(stdout || ""));
    });
  });
}

export function resolveAdbPath() {
  const candidates = [
    process.env.SEONGNAM_ADB_PATH,
    process.env.DAEHOE_TENNISTOWN_ADB_PATH,
    path.join(OUTER_ROOT, ".tools", "android-platform-tools", "platform-tools", process.platform === "win32" ? "adb.exe" : "adb"),
    path.join(OUTER_ROOT, ".tools", "android-sdk", "platform-tools", process.platform === "win32" ? "adb.exe" : "adb"),
    path.join(REPO_ROOT, ".tools", "android-platform-tools", "platform-tools", process.platform === "win32" ? "adb.exe" : "adb"),
    path.join(REPO_ROOT, ".tools", "android-sdk", "platform-tools", process.platform === "win32" ? "adb.exe" : "adb"),
    process.platform === "win32" ? "D:\\Python_Save\\search_tennis_cloudflared\\.tools\\android-platform-tools\\platform-tools\\adb.exe" : null,
    process.platform === "win32" ? "D:\\Python_Save\\search_tennis_cloudflared\\.tools\\android-sdk\\platform-tools\\adb.exe" : null,
    process.env.ANDROID_HOME ? path.join(process.env.ANDROID_HOME, "platform-tools", process.platform === "win32" ? "adb.exe" : "adb") : null,
    process.env.ANDROID_SDK_ROOT ? path.join(process.env.ANDROID_SDK_ROOT, "platform-tools", process.platform === "win32" ? "adb.exe" : "adb") : null,
    process.platform === "win32" ? "C:\\Android\\platform-tools\\adb.exe" : null,
    process.platform === "win32" ? "C:\\platform-tools\\adb.exe" : null,
    "adb",
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (candidate === "adb" || fs.existsSync(candidate)) return candidate;
  }
  return "adb";
}

export function parseAdbDevices(output) {
  const lines = String(output || "").split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    if (/^list of devices/i.test(line)) continue;
    const [serial, state] = line.split(/\s+/);
    if (!serial) continue;
    if (state === "device") return { status: "device", serial };
    if (state === "unauthorized") return { status: "android_unauthorized", serial };
    if (state === "offline") return { status: "android_device_offline", serial };
    return { status: "android_device_offline", serial };
  }
  return { status: "android_device_offline", serial: "" };
}

export function selectSeongnamTab(tabs) {
  return selectSeongnamTabs(tabs)[0] || null;
}

export function selectSeongnamTabs(tabs) {
  const candidates = (Array.isArray(tabs) ? tabs : []).filter((tab) => {
    const haystack = `${tab?.url || ""} ${tab?.title || ""}`.toLowerCase();
    return haystack.includes("res.isdc.co.kr") && !String(tab?.url || "").includes("auto_detect.do") && tab?.webSocketDebuggerUrl;
  });
  candidates.sort((a, b) => scoreTab(b) - scoreTab(a));
  return candidates.slice(0, numberEnv("SEONGNAM_ANDROID_MAX_TAB_CANDIDATES", 12, 1, 50));
}

export function hasOnlyBlockedSeongnamTabs(tabs) {
  const seongnamTabs = (Array.isArray(tabs) ? tabs : []).filter((tab) => {
    const haystack = `${tab?.url || ""} ${tab?.title || ""}`.toLowerCase();
    return haystack.includes("res.isdc.co.kr");
  });
  return seongnamTabs.length > 0 && seongnamTabs.every((tab) => String(tab?.url || "").includes("auto_detect.do"));
}

function scoreTab(tab) {
  const url = String(tab?.url || "");
  let score = 0;
  if (url.includes("facilityList.do")) score += 5;
  if (url.includes("tennis")) score += 4;
  if (url.includes("reservationInfo.do")) score -= 5;
  if (!url.includes("login.do")) score += 2;
  if (tab?.webSocketDebuggerUrl) score += 1;
  const numericId = Number(tab?.id);
  if (Number.isFinite(numericId)) score += numericId / 10000;
  return score;
}

export function classifyPage(url, bodyTextOrHtml) {
  const urlText = String(url || "").toLowerCase();
  const body = String(bodyTextOrHtml || "").toLowerCase();
  if (urlText.includes("auto_detect.do") || body.includes("비정상 접근") || body.includes("abnormal access")) {
    return "automation_blocked";
  }
  if (
    urlText.includes("login.do") ||
    body.includes("rest_logincheck.do") ||
    /name=["']web_id["']/.test(body) ||
    /name=["']web_pw["']/.test(body) ||
    /id=["']web_id["']/.test(body) ||
    /id=["']web_pw["']/.test(body)
  ) {
    return "login_required";
  }
  if (urlText.includes("res.isdc.co.kr")) return "ok";
  return "chrome_tab_not_found";
}

export function classifyFetchResponse(response, expectedType = "html") {
  const status = Number(response?.status || 0);
  const urlText = String(response?.url || "").toLowerCase();
  const contentType = String(response?.contentType || "").toLowerCase();
  const body = String(response?.text || "").toLowerCase();
  const fetchError = String(response?.fetchError || "").toLowerCase();
  if (status === 429) return { status: "rate_limited", retryable: true };
  if (status === 403) return { status: "automation_blocked", retryable: true };
  if (fetchError) {
    const timedOut = fetchError.includes("timeout") || fetchError.includes("abort");
    return { status: timedOut ? "request_timeout" : "partial_failure", retryable: true };
  }
  if (urlText.includes("auto_detect.do") || body.includes("鍮꾩젙???묎렐") || body.includes("abnormal access")) {
    return { status: "automation_blocked", retryable: true };
  }
  if (
    urlText.includes("login.do") ||
    body.includes("rest_logincheck.do") ||
    /name=["']web_id["']/.test(body) ||
    /name=["']web_pw["']/.test(body)
  ) {
    return { status: "login_required", retryable: false };
  }
  if (status < 200 || status >= 300) return { status: "partial_failure", retryable: true };
  if (expectedType === "json" && !contentType.includes("json")) return { status: "invalid_response", retryable: true };
  if (expectedType === "html" && contentType.includes("json") && /^\s*[{[]/.test(body)) {
    return { status: "invalid_response", retryable: true };
  }
  return { status: "ok", retryable: false };
}

export class AdaptiveThrottle {
  constructor(options = {}) {
    this.min = Math.max(1, Number(options.min ?? 2) || 2);
    this.max = Math.max(this.min, Number(options.max ?? DEFAULT_ANDROID_CONCURRENCY) || DEFAULT_ANDROID_CONCURRENCY);
    this.current = Math.max(this.min, Math.min(Number(options.initial ?? this.max) || this.max, this.max));
    this.recoveryStep = Math.max(1, Number(options.recoveryStep ?? 2) || 2);
    this.goodBatchesForRecovery = Math.max(1, Number(options.goodBatchesForRecovery ?? 2) || 2);
    this.goodBatchStreak = 0;
    this.changes = [];
  }

  observeBatch(classifications) {
    const values = Array.isArray(classifications) ? classifications : [];
    const total = values.length || 1;
    const statuses = values.map((item) => String(item?.status || "ok"));
    const severe = statuses.some((status) => ["automation_blocked", "rate_limited", "login_required"].includes(status));
    const failures = statuses.filter((status) => status !== "ok").length;
    let next = this.current;
    let reason = "";
    if (severe) {
      next = this.min;
      reason = "blocked_or_rate_limited";
    } else if (failures >= 2 || failures / total >= 0.2) {
      next = Math.max(this.min, this.current - 3);
      reason = "transient_failures";
    }

    if (reason) {
      this.goodBatchStreak = 0;
      this.setCurrent(next, reason);
      return;
    }

    this.goodBatchStreak += 1;
    if (this.goodBatchStreak >= this.goodBatchesForRecovery && this.current < this.max) {
      this.goodBatchStreak = 0;
      this.setCurrent(Math.min(this.max, this.current + this.recoveryStep), "healthy_recovery");
    }
  }

  setCurrent(next, reason) {
    const bounded = Math.max(this.min, Math.min(this.max, Number(next) || this.current));
    if (bounded === this.current) return;
    this.changes.push({ at: nowIso(), from: this.current, to: bounded, reason });
    this.current = bounded;
  }
}

export function sanitizeHeaders(headers) {
  const out = {};
  for (const [key, value] of Object.entries(headers || {})) {
    if (SENSITIVE_HEADERS.has(String(key).toLowerCase())) continue;
    out[key] = value;
  }
  return out;
}

export function parseFacilityListHtml(html) {
  const groups = [];
  const pattern = /name=["']groupId["']\s+value=["'](\d+)["'][\s\S]*?<div\s+class=["']head-area["']>\s*([\s\S]*?)\s*<\/div>/gi;
  let match;
  while ((match = pattern.exec(String(html || "")))) {
    groups.push({ groupId: match[1], title: cleanText(match[2]).replace(/테니스장/g, "").trim() });
  }
  if (!groups.length) {
    for (const simple of String(html || "").matchAll(/name=["']groupId["']\s+value=["'](\d+)["']/gi)) {
      groups.push({ groupId: simple[1], title: "" });
    }
  }
  return groups;
}

export function parseGroupHtml(html, groupTitle = "") {
  const courts = [];
  const pattern = /<li\s+id=["'](FAC\d+)["'][^>]*class=["'][^"']*facilityInfo[^"']*["'][\s\S]*?<div\s+class=["']head-area["']>\s*([\s\S]*?)\s*<\/div>/gi;
  let match;
  while ((match = pattern.exec(String(html || "")))) {
    const title = cleanText(match[2]);
    courts.push({
      facId: match[1],
      title: groupTitle && !title.startsWith(groupTitle) ? `${groupTitle} ${title}`.trim() : title,
    });
  }
  return courts;
}

export function parseTimetableHtml(html) {
  const rows = [];
  for (const rowMatch of String(html || "").matchAll(/<tr[\s\S]*?<\/tr>/gi)) {
    const row = rowMatch[0];
    const input = row.match(/<input[^>]+name=["']rbTime["'][^>]*>/i)?.[0] || "";
    if (!input || /\sdisabled(?:=|\s|>)/i.test(input)) continue;
    if (!/value=["'][^"']+["']/i.test(input)) continue;
    const cells = [...row.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((m) => cleanText(m[1]));
    const timeContent = cells[2] || "";
    const statusText = cells[3] || "";
    if (!timeContent || statusText) continue;
    rows.push({ timeContent, reserveUrl: LIST_URL, available: true });
  }
  return rows;
}

export function parseDomFallbackRows(rows) {
  return (Array.isArray(rows) ? rows : []).map((row, index) => {
    const date = normalizeYmd(row.date || row.ymd || "");
    const timeContent = row.timeContent || row.time || [row.startTime, row.endTime].filter(Boolean).join(" ~ ");
    return {
      facility: cleanText(row.facility || row.facilityName || ""),
      court: cleanText(row.court || row.courtName || ""),
      date,
      timeContent: cleanText(timeContent),
      available: /가능|available|empty/i.test(String(row.status || "")),
      remaining: row.remaining || row.remain || "",
      statusText: cleanText(row.status || ""),
      slotKey: `${date}:${timeContent || index}`,
    };
  }).filter((row) => row.date && row.timeContent);
}

function normalizeCollectorData(facilitiesById, availabilityById, diagnostics, pageUrl, deviceSerial) {
  const facilities = [];
  const slots = [];
  const unavailableDates = [];
  for (const [facId, meta] of facilitiesById.entries()) {
    facilities.push({
      id: facId,
      title: meta.title || facId,
      location: "성남",
      reserveUrl: LIST_URL,
    });
  }
  for (const [facId, daymap] of availabilityById.entries()) {
    for (const [date, daySlots] of Object.entries(daymap || {})) {
      if (!daySlots?.length) {
        unavailableDates.push({
          facilityId: facId,
          facilityName: facilitiesById.get(facId)?.title || facId,
          date,
          reserveUrl: LIST_URL,
          statusText: "unavailable_or_disabled",
        });
      }
      for (const slot of daySlots || []) {
        slots.push({
          facilityId: facId,
          facilityName: facilitiesById.get(facId)?.title || facId,
          courtName: slot.courtName || facilitiesById.get(facId)?.title || "",
          date,
          timeContent: slot.timeContent,
          available: slot.available !== false,
          remaining: slot.remaining || "",
          reserveUrl: slot.reserveUrl || LIST_URL,
          statusText: slot.statusText || "",
        });
      }
    }
  }
  return ok({ deviceSerial, pageUrl, facilities, slots, unavailableDates, diagnostics });
}

async function getJson(url, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const client = String(url).startsWith("https:") ? https : http;
    const request = client.get(url, { timeout: timeoutMs }, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        body += chunk;
      });
      response.on("end", () => {
        if (response.statusCode < 200 || response.statusCode >= 300) {
          reject(new Error(`http_${response.statusCode}`));
          return;
        }
        try {
          resolve(JSON.parse(body || "null"));
        } catch (error) {
          reject(new Error(`invalid_json:${error.message}`));
        }
      });
    });
    request.on("timeout", () => {
      request.destroy(new Error("timeout"));
    });
    request.on("error", reject);
  });
}

async function connectCdp(wsUrl) {
  if (typeof WebSocket === "undefined") {
    throw new Error("global WebSocket is unavailable in this Node.js runtime");
  }
  const ws = new WebSocket(wsUrl);
  let nextId = 1;
  const pending = new Map();
  const listeners = new Map();
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("cdp websocket timeout")), 10000);
    ws.addEventListener("open", () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
    ws.addEventListener("error", () => {
      clearTimeout(timer);
      reject(new Error("cdp websocket error"));
    }, { once: true });
  });
  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject, timer } = pending.get(msg.id);
      pending.delete(msg.id);
      clearTimeout(timer);
      if (msg.error) reject(new Error(msg.error.message || "cdp command failed"));
      else resolve(msg.result || {});
      return;
    }
    if (msg.method && listeners.has(msg.method)) {
      for (const fn of listeners.get(msg.method)) fn(msg.params || {});
    }
  });
  return {
    send(method, params = {}, timeoutOverrideMs = null) {
      const id = nextId++;
      ws.send(JSON.stringify({ id, method, params }));
      return new Promise((resolve, reject) => {
        const timeoutMs = timeoutOverrideMs || numberEnv("SEONGNAM_ANDROID_CDP_TIMEOUT_MS", 120000, 5000, 300000);
        const timer = setTimeout(() => {
          if (!pending.has(id)) return;
          pending.delete(id);
          reject(new Error(`cdp timeout: ${method}`));
        }, timeoutMs);
        pending.set(id, { resolve, reject, timer });
      });
    },
    on(method, fn) {
      const arr = listeners.get(method) || [];
      arr.push(fn);
      listeners.set(method, arr);
    },
    close() {
      ws.close();
    },
  };
}

async function evaluate(cdp, expression, awaitPromise = true, timeoutOverrideMs = null) {
  const result = await cdp.send("Runtime.evaluate", {
    expression,
    awaitPromise,
    returnByValue: true,
  }, timeoutOverrideMs);
  if (result.exceptionDetails) throw new Error("runtime evaluation failed");
  return result.result?.value;
}

function jsString(value) {
  return JSON.stringify(String(value || ""));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function mapWithConcurrency(items, concurrency, mapper) {
  const values = Array.from(items || []);
  const limit = Math.max(1, Math.min(Number(concurrency) || 1, values.length || 1));
  const results = new Array(values.length);
  let nextIndex = 0;
  async function worker() {
    while (nextIndex < values.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await mapper(values[index], index);
    }
  }
  await Promise.all(Array.from({ length: limit }, () => worker()));
  return results;
}

export function chunkArray(items, size) {
  const values = Array.from(items || []);
  const chunkSize = Math.max(1, Number(size) || values.length || 1);
  const chunks = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

export async function retryAsync(fn, retries = 1, delayMs = 250) {
  let lastValue;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    lastValue = await fn(attempt);
    if (!lastValue?.fetchError) return lastValue;
    if (attempt < retries && delayMs) await sleep(delayMs + Math.floor(Math.random() * 500));
  }
  return lastValue;
}

async function pageFetchText(cdp, url, options = {}) {
  const config = androidRequestConfig();
  const timeoutMs = config.fetchTimeoutMs;
  const retries = config.fetchRetries;
  const evalTimeoutMs = pageFetchEvaluateTimeoutMs(config);
  const optionJson = JSON.stringify({
    method: options.method || "GET",
    credentials: "include",
    headers: options.headers || { Accept: "*/*" },
    body: options.body || undefined,
  });
  const timeoutJson = JSON.stringify(timeoutMs);
  return retryAsync(() => evaluate(
      cdp,
      `(async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), ${timeoutJson});
      try {
        const opts = ${optionJson};
        opts.signal = controller.signal;
        const res = await fetch(${jsString(url)}, opts);
        const text = await res.text();
        return { ok: res.ok, status: res.status, url: res.url, contentType: res.headers.get('content-type') || '', text };
      } catch (error) {
        return { ok: false, status: 0, url: ${jsString(url)}, contentType: '', text: '', fetchError: String(error && error.message || error) };
      } finally {
        clearTimeout(timer);
      }
    })()`,
      true,
      evalTimeoutMs
    ),
    retries,
    options.retryDelayMs ?? 1500
  );
}

async function pageFetchManyText(cdp, requests, options = {}) {
  const config = androidRequestConfig();
  const timeoutMs = config.fetchTimeoutMs;
  const retries = config.fetchRetries;
  const concurrency = Math.max(1, Math.min(Number(options.concurrency || config.concurrency) || config.concurrency, 20));
  const evalTimeoutMs = pageFetchEvaluateTimeoutMs(config);
  const payload = (requests || []).map((request, index) => ({
    index,
    key: request.key ?? index,
    url: request.url,
    method: request.method || "GET",
    headers: request.headers || { Accept: "*/*" },
    body: request.body || undefined,
    context: request.context || {},
  }));
  if (!payload.length) return [];
  return retryAsync(() => evaluate(
      cdp,
      `(async () => {
        const requests = ${JSON.stringify(payload)};
        const timeoutMs = ${JSON.stringify(timeoutMs)};
        const concurrency = ${JSON.stringify(concurrency)};
        const results = new Array(requests.length);
        let nextIndex = 0;
        async function fetchOne(request) {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), timeoutMs);
          try {
            const res = await fetch(request.url, {
              method: request.method,
              credentials: 'include',
              headers: request.headers,
              body: request.body,
              signal: controller.signal
            });
            const text = await res.text();
            return {
              key: request.key,
              context: request.context,
              ok: res.ok,
              status: res.status,
              url: res.url,
              contentType: res.headers.get('content-type') || '',
              text
            };
          } catch (error) {
            return {
              key: request.key,
              context: request.context,
              ok: false,
              status: 0,
              url: request.url,
              contentType: '',
              text: '',
              fetchError: String(error && error.message || error)
            };
          } finally {
            clearTimeout(timer);
          }
        }
        async function worker() {
          while (nextIndex < requests.length) {
            const current = nextIndex++;
            results[current] = await fetchOne(requests[current]);
          }
        }
        await Promise.all(Array.from({ length: Math.min(concurrency, requests.length) }, () => worker()));
        return results;
      })()`,
      true,
      evalTimeoutMs
    ),
    retries,
    options.retryDelayMs ?? 1500
  );
}

async function collectWithPageFetch(cdp, diagnostics) {
  const facilitiesById = new Map();
  const availabilityById = new Map();
  const requestConfig = androidRequestConfig();
  const daysAhead = Math.max(0, Math.min(Number(process.env.SEONGNAM_DAYS_AHEAD || 2), 45));
  const maxCourts = numberEnv("SEONGNAM_ANDROID_MAX_COURTS", 0, 0, 500);
  const concurrency = requestConfig.concurrency;
  const batchSize = requestConfig.batchSize;
  const requestDelayMs = requestConfig.requestDelayMs;
  const throttle = new AdaptiveThrottle({ initial: concurrency, min: 2, max: concurrency });
  const deadlineMs = Date.now() + numberEnv(
    "SEONGNAM_ANDROID_TIMEOUT_MS",
    DEFAULT_ANDROID_TIMEOUT_MS,
    10000,
    900000
  );
  const maxDiagnosticResponses = numberEnv("SEONGNAM_ANDROID_MAX_DIAGNOSTIC_RESPONSES", 200, 0, 1000);
  const deadlineExceeded = () => Date.now() > deadlineMs;
  const markDeadline = () => {
    diagnostics.deadline_exceeded = true;
  };
  const failCollection = (status, message) => {
    const error = new Error(message || status);
    error.collectorStatus = status;
    return error;
  };
  const recordResponse = (response, context = {}) => {
    const classification = classifyFetchResponse(response, context.expectedType || "html");
    diagnostics.response_count = (diagnostics.response_count || 0) + 1;
    diagnostics.total_requests = (diagnostics.total_requests || 0) + 1;
    diagnostics[classification.status] = (diagnostics[classification.status] || 0) + 1;
    if (classification.status === "ok") diagnostics.success_requests = (diagnostics.success_requests || 0) + 1;
    else diagnostics.failed_requests = (diagnostics.failed_requests || 0) + 1;
    if (response?.status === 403) diagnostics.http_403 = (diagnostics.http_403 || 0) + 1;
    if (response?.status === 429) diagnostics.http_429 = (diagnostics.http_429 || 0) + 1;
    if (classification.status === "request_timeout") diagnostics.timeout_count = (diagnostics.timeout_count || 0) + 1;
    if (!maxDiagnosticResponses || diagnostics.responses.length < maxDiagnosticResponses) {
      diagnostics.responses.push({
        url: response?.url,
        status: response?.status,
        contentType: response?.contentType,
        classification: classification.status,
        ...context,
      });
    }
    if (response?.fetchError) {
      diagnostics.response_errors ||= [];
      diagnostics.response_errors.push({
        url: response.url,
        error: response.fetchError,
        ...context,
      });
    }
    return classification;
  };
  const list = await pageFetchText(cdp, LIST_URL, { headers: { Accept: "text/html,*/*" } });
  const listClass = recordResponse(list, { kind: "facility_list", expectedType: "html" });
  if (["automation_blocked", "rate_limited", "login_required"].includes(listClass.status)) {
    throw failCollection(listClass.status, `facility_list_${listClass.status}`);
  }
  if (list.fetchError) throw new Error(`facility_list_fetch_${list.fetchError}`);
  if (!list.ok) throw new Error(`facility_list_http_${list.status}`);
  const groups = parseFacilityListHtml(list.text);
  diagnostics.groups = groups.length;
  for (const group of groups) {
    if (deadlineExceeded()) {
      markDeadline();
      break;
    }
    if (maxCourts && facilitiesById.size >= maxCourts) break;
    const body = new URLSearchParams({ groupId: group.groupId }).toString();
    const response = await pageFetchText(cdp, `${BASE_URL}/tennisList.do`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", Accept: "text/html,*/*" },
      body,
    });
    const classification = recordResponse(response, { kind: "court_group", groupId: group.groupId, expectedType: "html" });
    if (requestDelayMs) await sleep(requestDelayMs + Math.floor(Math.random() * 500));
    if (["automation_blocked", "rate_limited", "login_required"].includes(classification.status)) {
      throw failCollection(classification.status, `court_group_${classification.status}`);
    }
    if (response.fetchError || !response.ok || classification.status !== "ok") continue;
    for (const court of parseGroupHtml(response.text, group.title)) {
      if (maxCourts && facilitiesById.size >= maxCourts) break;
      facilitiesById.set(court.facId, {
        title: court.title || group.title || court.facId,
      });
      availabilityById.set(court.facId, {});
    }
  }
  diagnostics.courts = facilitiesById.size;
  diagnostics.initial_concurrency = concurrency;
  diagnostics.concurrency = concurrency;
  diagnostics.batch_size = batchSize;
  diagnostics.request_delay_ms = requestDelayMs;
  diagnostics.fetch_timeout_ms = requestConfig.fetchTimeoutMs;
  diagnostics.fetch_retries = requestConfig.fetchRetries;
  const jobs = [];
  const todayYmd = ymd(0);
  let skippedBySchedule = 0;
  for (const [facId, meta] of facilitiesById.entries()) {
    for (let offset = 0; offset <= daysAhead; offset += 1) {
      const date = ymd(offset);
      if (!shouldQueryCourtDate(meta.title || facId, date, todayYmd)) {
        skippedBySchedule += 1;
        const daymap = availabilityById.get(facId) || {};
        daymap[date] = [];
        availabilityById.set(facId, daymap);
        continue;
      }
      jobs.push({ facId, date });
    }
  }
  diagnostics.schedule_skipped = skippedBySchedule;
  diagnostics.date_jobs = jobs.length;
  const tableJobs = [];
  for (const chunk of chunkArray(jobs, batchSize)) {
    if (deadlineExceeded()) {
      markDeadline();
      break;
    }
    const batchStartedAt = Date.now();
    const statusResponses = await pageFetchManyText(
      cdp,
      chunk.map(({ facId, date }) => ({
        key: `${facId}:${date}`,
        url: `${BASE_URL}/getReservationInfoByDate.do?facId=${encodeURIComponent(facId)}&resdate=${encodeURIComponent(ymdDash(date))}`,
        headers: { Accept: "text/plain,*/*" },
        context: { facId, date },
      })),
      { concurrency: throttle.current }
    );
    const classifications = [];
    for (const status of statusResponses) {
      const { facId, date } = status.context || {};
      const daymap = availabilityById.get(facId) || {};
      daymap[date] = [];
      availabilityById.set(facId, daymap);
      const classification = recordResponse(status, { kind: "date_status", facId, date, expectedType: "html" });
      classifications.push(classification);
      if (["automation_blocked", "rate_limited", "login_required"].includes(classification.status)) {
        throw failCollection(classification.status, `date_status_${classification.status}`);
      }
      if (status.fetchError || !status.ok || classification.status !== "ok") continue;
      if (["closed", "empty", "full"].includes(String(status.text || "").trim().toLowerCase())) continue;
      tableJobs.push({ facId, date });
    }
    throttle.observeBatch(classifications);
    diagnostics.batch_timings ||= [];
    diagnostics.batch_timings.push({ kind: "date_status", requests: chunk.length, ms: Date.now() - batchStartedAt, concurrency: throttle.current });
    if (requestDelayMs) await sleep(requestDelayMs + Math.floor(Math.random() * 500));
  }
  diagnostics.table_candidates = tableJobs.length;
  for (const chunk of chunkArray(tableJobs, batchSize)) {
    if (deadlineExceeded()) {
      markDeadline();
      break;
    }
    const batchStartedAt = Date.now();
    const tableResponses = await pageFetchManyText(
      cdp,
      chunk.map(({ facId, date }) => ({
        key: `${facId}:${date}`,
        url: `${BASE_URL}/getTimeTableByDate.do`,
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", Accept: "text/html,*/*" },
        body: new URLSearchParams({ facId, resdate: ymdDash(date) }).toString(),
        context: { facId, date },
      })),
      { concurrency: throttle.current }
    );
    const classifications = [];
    for (const table of tableResponses) {
      const { facId, date } = table.context || {};
      const classification = recordResponse(table, { kind: "time_table", facId, date, expectedType: "html" });
      classifications.push(classification);
      if (["automation_blocked", "rate_limited", "login_required"].includes(classification.status)) {
        throw failCollection(classification.status, `time_table_${classification.status}`);
      }
      if (table.fetchError || !table.ok || classification.status !== "ok") continue;
      const daymap = availabilityById.get(facId) || {};
      daymap[date] = parseTimetableHtml(table.text).map((slot) => ({
        ...slot,
        courtName: facilitiesById.get(facId)?.title || facId,
      }));
      availabilityById.set(facId, daymap);
    }
    throttle.observeBatch(classifications);
    diagnostics.batch_timings ||= [];
    diagnostics.batch_timings.push({ kind: "time_table", requests: chunk.length, ms: Date.now() - batchStartedAt, concurrency: throttle.current });
    if (requestDelayMs) await sleep(requestDelayMs + Math.floor(Math.random() * 500));
  }
  diagnostics.final_concurrency = throttle.current;
  diagnostics.concurrency_changes = throttle.changes;
  return { facilitiesById, availabilityById };
}

async function collectDomFallback(cdp) {
  const rows = await evaluate(
    cdp,
    `(() => {
      const text = (node) => (node?.innerText || node?.textContent || '').replace(/\\s+/g, ' ').trim();
      const out = [];
      for (const row of document.querySelectorAll('tr, li, .time, .reserve, .facilityInfo')) {
        const label = text(row);
        if (!/\\d{1,2}:\\d{2}/.test(label)) continue;
        out.push({
          facility: text(document.querySelector('.head-area, h1, h2')) || '',
          court: label.match(/(\\S*코트\\S*)/)?.[1] || '',
          date: (document.body.innerText.match(/20\\d{2}[-.]?\\d{1,2}[-.]?\\d{1,2}/) || [''])[0],
          time: (label.match(/\\d{1,2}:\\d{2}\\s*(?:~|-|부터|-)\\s*\\d{1,2}:\\d{2}/) || label.match(/\\d{1,2}:\\d{2}/) || [''])[0],
          status: label,
          remaining: (label.match(/(\\d+)\\s*(?:면|개|코트)/) || [])[1] || ''
        });
      }
      return out.slice(0, 200);
    })()`
  );
  const parsed = parseDomFallbackRows(rows);
  const facilitiesById = new Map();
  const availabilityById = new Map();
  for (const row of parsed) {
    const facId = `android-dom-${row.facility || row.court || "current"}`;
    facilitiesById.set(facId, { title: [row.facility, row.court].filter(Boolean).join(" ") || "성남 Android Chrome" });
    const daymap = availabilityById.get(facId) || {};
    daymap[row.date] ||= [];
    daymap[row.date].push({
      timeContent: row.timeContent,
      available: row.available,
      remaining: row.remaining,
      statusText: row.statusText,
      courtName: row.court,
      reserveUrl: LIST_URL,
    });
    availabilityById.set(facId, daymap);
  }
  return { facilitiesById, availabilityById };
}

export async function collectAndroid() {
  const adbPath = resolveAdbPath();
  let device;
  try {
    device = parseAdbDevices(await execFileText(adbPath, ["devices"]));
  } catch (error) {
    return fail("android_cdp_unavailable", "adb 명령을 실행할 수 없습니다. Android platform-tools 설치와 PATH를 확인하세요.");
  }
  if (device.status === "android_unauthorized") {
    return fail("android_unauthorized", "휴대폰에서 USB 디버깅 승인 팝업을 허용한 뒤 다시 실행하세요.", { device_serial: maskSerial(device.serial) });
  }
  if (device.status !== "device") {
    return fail("android_device_offline", "USB 디버깅이 활성화된 Android 기기를 연결한 뒤 다시 실행하세요.", { device_serial: maskSerial(device.serial) });
  }
  try {
    await execFileText(adbPath, ["forward", "tcp:9222", "localabstract:chrome_devtools_remote"]);
  } catch (error) {
    return fail("android_cdp_unavailable", "ADB port forwarding에 실패했습니다. Chrome이 실행 중인지 확인하세요.", { device_serial: maskSerial(device.serial) });
  }

  let tabs = [];
  let endpointSuccesses = 0;
  const endpointDiagnostics = {};
  for (const endpoint of ["/json", "/json/list", "/json/version"]) {
    try {
      const payload = await getJson(`${CDP_HOST}${endpoint}`);
      endpointSuccesses += 1;
      endpointDiagnostics[endpoint] = Array.isArray(payload) ? { count: payload.length } : { ok: true };
      if (Array.isArray(payload) && !tabs.length) tabs = payload;
    } catch (error) {
      endpointDiagnostics[endpoint] = { error: error.message };
    }
  }
  if (endpointSuccesses === 0) {
    return fail("android_cdp_unavailable", "Android Chrome CDP 엔드포인트가 응답하지 않습니다. 휴대폰 Chrome 실행과 원격 디버깅 소켓을 확인하세요.", {
      device_serial: maskSerial(device.serial),
      diagnostics: { tab_found: false, endpoints: endpointDiagnostics },
    });
  }
  const tabCandidates = selectSeongnamTabs(tabs);
  if (!tabCandidates.some((candidate) => candidate?.webSocketDebuggerUrl)) {
    if (hasOnlyBlockedSeongnamTabs(tabs)) {
      return fail("automation_blocked", "Android Chrome에서도 비정상 접근 탐지 탭만 열려 있습니다. 정상 성남 예약 페이지를 직접 열어 주세요.", {
        device_serial: maskSerial(device.serial),
        diagnostics: { tab_found: true, endpoints: endpointDiagnostics },
      });
    }
    return fail("chrome_tab_not_found", "휴대폰 Chrome에서 성남 예약 페이지를 열어 둔 뒤 다시 실행하세요.", {
      device_serial: maskSerial(device.serial),
      diagnostics: { tab_found: false, endpoints: endpointDiagnostics },
    });
  }

  let cdp;
  let tab;
  let pageUrl = "";
  let bodyText = "";
  let lastTabError = "";
  try {
    for (const candidate of tabCandidates) {
      if (!candidate?.webSocketDebuggerUrl) continue;
      try {
        cdp = await connectCdp(candidate.webSocketDebuggerUrl);
        await Promise.all([
          cdp.send("Runtime.enable", {}, 5000),
          cdp.send("Page.enable", {}, 5000),
          cdp.send("Network.enable", {}, 5000),
          cdp.send("DOM.enable", {}, 5000),
        ]);
        pageUrl = await evaluate(cdp, "location.href", true, 5000);
        bodyText = await evaluate(cdp, "document.body ? document.body.innerText.slice(0, 2000) : ''", true, 5000);
        tab = candidate;
        break;
      } catch (error) {
        lastTabError = error.message;
        if (cdp) cdp.close();
        cdp = null;
      }
    }
    if (!cdp || !tab) {
      throw new Error(lastTabError || "no responsive seongnam cdp tab");
    }
    const pageStatus = classifyPage(pageUrl, bodyText);
    if (pageStatus === "login_required") {
      return fail("login_required", "휴대폰 Chrome에서 성남 사이트에 직접 로그인한 뒤 다시 실행하세요.", {
        device_serial: maskSerial(device.serial),
        page_url: pageUrl,
      });
    }
    if (pageStatus === "automation_blocked") {
      return fail("automation_blocked", "Android Chrome에서도 비정상 접근 탐지 화면입니다. 탭을 정상 예약 페이지로 다시 열어 주세요.", {
        device_serial: maskSerial(device.serial),
        page_url: pageUrl,
      });
    }

    const diagnostics = {
      tab_found: true,
      network_capture: false,
      page_fetch: false,
      dom_fallback: false,
      endpoints: endpointDiagnostics,
      responses: [],
    };
    let collected;
    try {
      collected = await collectWithPageFetch(cdp, diagnostics);
      diagnostics.page_fetch = true;
      diagnostics.network_capture = diagnostics.responses.length > 0;
    } catch (error) {
      if (error.collectorStatus) {
        diagnostics.page_fetch_error = error.message;
        diagnostics.protected_existing_data = true;
        return fail(error.collectorStatus, `Android Chrome page fetch stopped: ${error.message}`, {
          device_serial: maskSerial(device.serial),
          page_url: pageUrl,
          diagnostics,
        });
      }
      diagnostics.page_fetch_error = error.message;
      collected = await collectDomFallback(cdp);
      diagnostics.dom_fallback = true;
    }
    const slotCount = [...collected.availabilityById.values()].reduce(
      (total, daymap) => total + Object.values(daymap).reduce((n, slots) => n + (slots || []).length, 0),
      0
    );
    diagnostics.slots = slotCount;
    if (!collected.facilitiesById.size) {
      if (diagnostics.page_fetch_error) {
        diagnostics.protected_existing_data = true;
        return fail("partial_failure", "Android Chrome page fetch did not complete; keep existing Seongnam data", {
          device_serial: maskSerial(device.serial),
          page_url: pageUrl,
          diagnostics,
        });
      }
      return fail("parse_failed", "성남 시설 또는 코트 정보를 추출하지 못했습니다.", {
        device_serial: maskSerial(device.serial),
        page_url: pageUrl,
        diagnostics,
      });
    }
    return normalizeCollectorData(collected.facilitiesById, collected.availabilityById, diagnostics, pageUrl, device.serial);
  } catch (error) {
    return fail("android_cdp_unavailable", `Android Chrome CDP 연결 또는 실행에 실패했습니다: ${error.message}`, {
      device_serial: maskSerial(device.serial),
    });
  } finally {
    if (cdp) cdp.close();
  }
}

async function main() {
  const result = await collectAndroid();
  process.stdout.write(`${JSON.stringify(result)}\n`);
  process.exitCode = result.status === "ok" ? 0 : 2;
}

const entryPath = process.argv[1] ? fileURLToPath(import.meta.url) === process.argv[1] : false;
if (entryPath) {
  main().catch((error) => {
    process.stdout.write(`${JSON.stringify(fail("android_cdp_unavailable", error.message))}\n`);
    process.exitCode = 2;
  });
}
