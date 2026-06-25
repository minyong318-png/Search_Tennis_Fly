param(
  [string]$TaskName = "TennisLocalRefreshFallback",
  [ValidateSet("all", "yongin", "goyang", "suwon", "seongnam")]
  [string]$Target = "all",
  [int]$IntervalMinutes = 30,
  [string]$Python = "python"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runScript = Join-Path $repoRoot "scripts/run_local_refresh.ps1"

if (-not (Test-Path -LiteralPath $runScript)) {
  throw "Missing run script: $runScript"
}

$arguments = @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-File", "`"$runScript`"",
  "-Target", $Target,
  "-Python", "`"$Python`""
) -join " "

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arguments -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -MultipleInstances IgnoreNew

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger $trigger `
  -Settings $settings `
  -Description "Fallback tennis availability refresh from this Windows machine." `
  -Force | Out-Null

Write-Host "Installed scheduled task '$TaskName' target=$Target interval=${IntervalMinutes}m"
Write-Host "Run now: Start-ScheduledTask -TaskName '$TaskName'"
