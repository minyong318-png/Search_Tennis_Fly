import { chromium } from "playwright";
import fs from "node:fs/promises";
import path from "node:path";

const BASE_URL = "https://res.isdc.co.kr";
const LIST_URL = `${BASE_URL}/facilityList.do?facType=29`;
const USER_ID = process.env.ISDC_ID || "";
const USER_PW = process.env.ISDC_PW || "";
const STORAGE_STATE =
  process.env.SEONGNAM_STORAGE_STATE ||
  path.join(process.cwd(), ".cache", "seongnam_storage_state.json");

function requiredEnv() {
  if (!USER_ID || !USER_PW) {
    throw new Error("ISDC_ID/ISDC_PW env is required");
  }
}

async function launchBrowser() {
  const options = {
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  };
  try {
    return await chromium.launch({
      ...options,
      channel: process.env.PLAYWRIGHT_CHROME_CHANNEL || "chrome",
    });
  } catch {
    return await chromium.launch(options);
  }
}

async function waitForChallenge(page) {
  await page.waitForLoadState("domcontentloaded").catch(() => {});
  await page.waitForTimeout(Number(process.env.SEONGNAM_WAF_WAIT_MS || 5000));
}

async function fillLoginForm(page) {
  await assertNotAutomationBlocked(page);
  const idSelectors = [
    "input[name='web_id']",
    "#web_id",
    "input[name='userId']",
    "input[type='text']",
  ];
  const pwSelectors = [
    "input[name='web_pw']",
    "#web_pw",
    "input[name='password']",
    "input[type='password']",
  ];
  const idSelector = await firstVisible(page, idSelectors);
  const pwSelector = await firstVisible(page, pwSelectors);
  if (!idSelector || !pwSelector) {
    throw new Error("login form fields not found");
  }
  await page.locator(idSelector).first().fill(USER_ID, { force: true });
  await page.locator(pwSelector).first().fill(USER_PW, { force: true });
  await page.locator(pwSelector).first().press("Enter").catch(() => null);
  await page.waitForTimeout(1500);
  if (await looksLoggedIn(page)) return;

  await page.evaluate(() => {
    const candidates = [...document.querySelectorAll("button, a, input[type=button], input[type=submit]")]
      .filter((el) => /login|로그인/i.test((el.innerText || el.value || "").trim()));
    const target = candidates.at(-1);
    if (target) target.click();
    else if (typeof window.fn_login === "function") window.fn_login();
    else if (typeof window.login === "function") window.login();
    else document.querySelector("form")?.submit();
  });
}

async function assertNotAutomationBlocked(page) {
  const body = await page.locator("body").innerText().catch(() => "");
  if (page.url().includes("auto_detect.do") || /비정상 접근 탐지|abnormal access|automation/i.test(body)) {
    throw new Error(`automation blocked at ${page.url()}`);
  }
}

async function firstVisible(page, selectors) {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    if ((await locator.count().catch(() => 0)) && (await locator.isVisible().catch(() => false))) {
      return selector;
    }
  }
  return "";
}

async function looksLoggedIn(page) {
  const text = await page.locator("body").innerText().catch(() => "");
  return /logout|로그아웃|마이페이지|예약확인/i.test(text);
}

async function validateWithRequestContext(context) {
  const list = await context.request.get(LIST_URL, { timeout: 30000 });
  const listText = await list.text();
  if (!list.ok() || !/name=["']groupId["']\s+value=["']\d+/.test(listText)) {
    throw new Error(`facility list validation failed status=${list.status()}`);
  }
  const groupId = listText.match(/name=["']groupId["']\s+value=["'](\d+)["']/)?.[1];
  if (groupId) {
    const group = await context.request.post(`${BASE_URL}/tennisList.do`, {
      form: { groupId },
      timeout: 30000,
    });
    if (!group.ok()) {
      throw new Error(`facility group validation failed status=${group.status()}`);
    }
  }
}

async function main() {
  requiredEnv();
  await fs.mkdir(path.dirname(STORAGE_STATE), { recursive: true });
  const browser = await launchBrowser();
  const context = await browser.newContext({
    locale: "ko-KR",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
  });
  const page = await context.newPage();
  try {
    await page.goto(`${BASE_URL}/login.do`, { waitUntil: "domcontentloaded", timeout: 90000 });
    await waitForChallenge(page);
    await fillLoginForm(page);
    await page.waitForLoadState("networkidle", { timeout: 30000 }).catch(() => {});
    await page.goto(LIST_URL, { waitUntil: "domcontentloaded", timeout: 90000 });
    await waitForChallenge(page);
    await assertNotAutomationBlocked(page);
    await validateWithRequestContext(context);
    await context.storageState({ path: STORAGE_STATE });
    console.log(`[SEONGNAM][AUTH] playwright storage_state=saved path=${STORAGE_STATE}`);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(`[SEONGNAM][AUTH] playwright error=${error.message}`);
  process.exit(1);
});
