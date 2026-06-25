# Local Refresh Fallback

This Windows fallback runs the same crawler entrypoint as GitHub Actions:

```powershell
python refresh_and_notify.py
```

It exists for cases where GitHub Actions cannot reach the Tailscale node.

## GitHub Self-Hosted Runner Fallback

The workflow `.github/workflows/crawl_local_fallback.yml` runs on a self-hosted Windows runner with this label set:

```text
tennis-local
```

Register this PC as a GitHub self-hosted runner in the repository settings:

```text
Settings > Actions > Runners > New self-hosted runner > Windows
```

During runner configuration, add the custom label:

```text
tennis-local
```

When the workflow is manually triggered, GitHub sends the job to this PC. The crawler then uses this PC's normal network/IP, not the GitHub-hosted runner IP and not the Tailscale exit node.

Run it from GitHub:

```text
Actions > Crawl Local Fallback & Notify > Run workflow
```

Choose `all`, `yongin`, `goyang`, `suwon`, or `seongnam`.

## Setup

1. Copy `.env.local.example` to `.env.local`.
2. Fill the same secret values used by GitHub Actions.
3. Install the scheduled task:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\install_local_refresh_task.ps1 -TaskName TennisLocalRefreshFallback -Target all -IntervalMinutes 30
```

## Manual Run

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\run_local_refresh.ps1 -Target all
```

Dry-run without secrets:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\run_local_refresh.ps1 -DryRun -Target suwon
```

## Logs

Logs are written to `logs/local-refresh-YYYYMMDD.log`.

## Scheduled Task

Check status:

```powershell
Get-ScheduledTask -TaskName TennisLocalRefreshFallback
Get-ScheduledTaskInfo -TaskName TennisLocalRefreshFallback
```

Run immediately:

```powershell
Start-ScheduledTask -TaskName TennisLocalRefreshFallback
```

Disable:

```powershell
Disable-ScheduledTask -TaskName TennisLocalRefreshFallback
```
