import { chromium } from "playwright";

const BASE_URL = "https://share.gg.go.kr";
const DAYS_AHEAD = Number(process.env.GGSHARE_DAYS_AHEAD || 45);
const USER_ID = process.env.GGSHARE_ID || "";
const USER_PW = process.env.GGSHARE_PW || "";

const STATIC_FACILITIES = [
  { city: "anseong", label: "안성", facilityId: "F0137", instiCode: "1230001", title: "고삼테니스장 1코트" },
  { city: "anseong", label: "안성", facilityId: "F0142", instiCode: "1230001", title: "팜랜드 물류단지공원 테니스장" },
  { city: "uijeongbu", label: "의정부", facilityId: "F0003", instiCode: "1130004", title: "모두의 운동장 테니스장 A코트" },
  { city: "yangpyeong", label: "양평", facilityId: "F0004", instiCode: "4170037", title: "강하테니스장" }
];

const UIWANG_SEARCH_URL = `${BASE_URL}/facilityListS11?searchArea=4143&searchType=S1_1&searchType2=${encodeURIComponent("테니스장")}`;

function todayYmd() {
  const now = new Date();
  return `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}`;
}

function addDaysYmd(days) {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
}

function normalizeYmd(value) {
  return String(value || "").replace(/\D/g, "").slice(0, 8);
}

function viewUrl(item) {
  return `${BASE_URL}/facilityListO/view?eshare=1&facilityId=${encodeURIComponent(item.facilityId)}&instiCode=${encodeURIComponent(item.instiCode)}`;
}

async function waitForMain(page) {
  await page.waitForLoadState("domcontentloaded").catch(() => {});
  await page.waitForTimeout(4000);
}

async function login(page) {
  if (!USER_ID || !USER_PW) {
    throw new Error("GGSHARE_ID/GGSHARE_PW env is required for slot crawl");
  }
  await page.goto(BASE_URL, { waitUntil: "domcontentloaded", timeout: 90000 });
  await waitForMain(page);
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (await page.locator("#mberIdChk").count().catch(() => 0)) break;
    await page.evaluate(() => {
      const links = [...document.querySelectorAll("a[href$='/member'], a[href='/member'], a[href*='/member']")]
        .filter((a) => /로그인/.test((a.innerText || "").trim()));
      const visible = links.find((a) => {
        const rect = a.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      });
      (visible || links[0])?.click();
    }).catch(() => null);
    await page.waitForTimeout(5000);
    if (await page.locator("#mberIdChk").count().catch(() => 0)) break;
    await page.goto(`${BASE_URL}/member`, { waitUntil: "domcontentloaded", timeout: 90000 }).catch(() => null);
    await waitForMain(page);
  }
  await page.waitForSelector("#mberIdChk", { state: "attached", timeout: 90000 });
  await page.locator("#mberIdChk").fill(USER_ID, { force: true });
  await page.locator("#passwordChk").fill(USER_PW, { force: true });
  await page.press("#passwordChk", "Enter").catch(() => null);
  await page.waitForTimeout(1500);
  if (/로그아웃|마이페이지/.test(await page.locator("body").innerText().catch(() => ""))) return;
  await page.evaluate(() => {
    const input = document.querySelector("#passwordChk");
    const scope = input?.closest(".login, .loginBox, .member, section, div") || document;
    const candidates = [...scope.querySelectorAll("button, a, input[type=button], input[type=submit]")]
      .filter((el) => /로그인/.test((el.innerText || el.value || "").trim()));
    const target = candidates.at(-1);
    if (target) target.click();
    else if (typeof window.fn_login === "function") window.fn_login();
  });
  await page.waitForTimeout(5000);
  const text = await page.locator("body").innerText().catch(() => "");
  if (/아이디\(이메일\)|비밀번호/.test(text) && !/로그아웃|마이페이지/.test(text)) {
    throw new Error("login form still visible after submit");
  }
}

async function discoverUiwangFacilities(page) {
  await page.goto(UIWANG_SEARCH_URL, { waitUntil: "domcontentloaded", timeout: 90000 });
  await waitForMain(page);
  const candidates = await page.evaluate(() => {
    const out = [];
    const seen = new Set();
    const textOf = (node) => (node?.innerText || node?.textContent || "").replace(/\s+/g, " ").trim();
    for (const link of document.querySelectorAll("a[href*='facilityId'], a[onclick*='facilityId'], a[onclick*='viewPage']")) {
      const raw = `${link.getAttribute("href") || ""} ${link.getAttribute("onclick") || ""}`;
      const facilityId = raw.match(/facilityId[=:'"]+([A-Z0-9]+)/i)?.[1] || raw.match(/viewPage\(['"]?([^,'")]+)/)?.[1];
      const instiCode = raw.match(/instiCode[=:'"]+([0-9]+)/i)?.[1] || raw.match(/viewPage\([^,]+,\s*['"]?([0-9]+)/)?.[1];
      const title = textOf(link.closest("li, tr, article, .item, .list") || link);
      if (!facilityId || !instiCode || !/청계|소프트|테니스/.test(title)) continue;
      const key = `${facilityId}:${instiCode}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ city: "uiwang", label: "의왕", facilityId, instiCode, title: title || "의왕 경기공유 테니스장" });
    }
    return out;
  });
  return candidates.filter((item) => /청계|소프트/.test(item.title)).slice(0, 4);
}

function parseSlot(row, item) {
  const ymd = normalizeYmd(row.bookingDt || row.date || row.useDate || row.roundDate);
  const start = String(row.roundFtime || row.startTime || row.start_time || "").replace(/^(\d{2})(\d{2})$/, "$1:$2");
  const end = String(row.roundTtime || row.endTime || row.end_time || "").replace(/^(\d{2})(\d{2})$/, "$1:$2");
  const state = String(row.finalBookingStateCd || row.bookingStateCd || row.stateCd || row.state || "");
  const possible = String(row.possibleYn || row.reservPossibleYn || row.bookingPossibleYn || row.useYn || "").toUpperCase();
  const blocked = ["CA", "CANCEL", "END", "N"].includes(state.toUpperCase()) || possible === "N";
  if (!ymd || !start || !end || blocked) return null;
  return {
    ymd,
    slot: {
      timeContent: `${start} ~ ${end}`,
      slotKey: `${start}~${end}`,
      courtNo: row.facilityNm || row.roundNm || item.title,
      reserveUrl: viewUrl(item),
      source: "ggshare",
      price: row.roundPrice || row.price || ""
    }
  };
}

async function fetchSlots(page, item) {
  await page.goto(viewUrl(item), { waitUntil: "domcontentloaded", timeout: 90000 });
  await waitForMain(page);
  await page.evaluate(async (payload) => {
    await fetch("/fnct/integration/simpleReservationCancle", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "X-Requested-With": "XMLHttpRequest" },
      body: new URLSearchParams({ intervalCode: "200", icd: payload.instiCode, fcd: payload.facilityId })
    }).catch(() => null);
    if (typeof window.NetFunnel_Action === "function") {
      await new Promise((resolve) => {
        try {
          window.NetFunnel_Action({ action_id: "simReser" }, () => resolve());
          setTimeout(resolve, 8000);
        } catch {
          resolve();
        }
      });
    }
  }, item);
  const data = await page.evaluate(async (payload) => {
    const res = await fetch("/fnct/integration/simpleReservation", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "X-Requested-With": "XMLHttpRequest" },
      body: new URLSearchParams({
        instiCode: payload.instiCode,
        resourceCode: payload.facilityId,
        lendCode: "",
        funcionCode: "bookingStart",
        intervalCode: "200",
        intervalNum: "0",
        pickupCart: "",
        delCart: "",
        roundEquality: "",
        paramCode: "S2"
      })
    });
    const text = await res.text();
    try {
      return JSON.parse(text);
    } catch {
      return { resultCode: `http_${res.status}`, resultMsg: text.slice(0, 300), rtnList: [] };
    }
  }, item);
  return Array.isArray(data.rtnList) ? data.rtnList : [];
}

async function main() {
  const target = (process.argv[2] || "").trim().toLowerCase();
  const minYmd = todayYmd();
  const maxYmd = addDaysYmd(DAYS_AHEAD);
  let browser;
  try {
    browser = await chromium.launch({ headless: true, channel: process.env.PLAYWRIGHT_CHROME_CHANNEL || "chrome" });
  } catch {
    browser = await chromium.launch({ headless: true });
  }
  const page = await browser.newPage({ locale: "ko-KR" });
  const result = { facilities: {}, availability: {}, stats: { ok: 0, fail: 0, slots: 0 } };
  try {
    await login(page);
    let items = STATIC_FACILITIES.filter((item) => !target || item.city === target);
    if (!target || target === "uiwang") {
      items = [...items, ...(await discoverUiwangFacilities(page))];
    }
    for (const item of items) {
      const rawId = `${item.city}-${item.facilityId}-${item.instiCode}`;
      result.facilities[rawId] = {
        title: item.title,
        location: item.city === "yangpyeong" ? "양평군" : `${item.label}시`,
        reserveUrl: viewUrl(item),
        source: "경기공유서비스"
      };
      result.availability[rawId] = {};
      try {
        const rows = await fetchSlots(page, item);
        for (const row of rows) {
          const parsed = parseSlot(row, item);
          if (!parsed || parsed.ymd < minYmd || parsed.ymd > maxYmd) continue;
          result.availability[rawId][parsed.ymd] ||= [];
          result.availability[rawId][parsed.ymd].push(parsed.slot);
          result.stats.slots += 1;
        }
        result.stats.ok += 1;
      } catch (error) {
        result.stats.fail += 1;
        result.facilities[rawId].blockedReason = error.message;
      }
    }
  } finally {
    await browser.close();
  }
  console.log(JSON.stringify(result));
}

main().catch((error) => {
  console.error(`[GGSHARE_BROWSER][ERROR] ${error.message}`);
  process.exit(1);
});
