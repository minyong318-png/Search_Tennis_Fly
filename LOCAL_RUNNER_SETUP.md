# Goyang Self-hosted Runner Setup

`crawl_goyang.yml` now runs on `runs-on: [self-hosted, goyang-home]`.

## 1) Register self-hosted runner on your home machine

In GitHub repo (`minyong318-png/Search_Tennis_Fly`) > Settings > Actions > Runners > New self-hosted runner:
- OS: pick your machine OS
- Follow the generated install commands
- Add custom label: `goyang-home`

## 2) Keep runner online

The runner must stay running for workflow jobs to start.

## 3) Run workflow

Trigger `Crawl Goyang & Notify`.

## 4) Verify in logs

Look for:
- non-zero `[GYT][STATS] ok=...`
- absence of `[GYT][EARLY_ABORT]` all-empty pattern

## 5) Trigger endpoint

Worker already maps `target=goyang` to `crawl_goyang.yml`.
Use your existing trigger URL with `target=goyang`.
