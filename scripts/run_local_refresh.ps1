param(
  [ValidateSet("all", "yongin", "goyang", "suwon", "seongnam")]
  [string]$Target = "",
  [string]$EnvFile = "",
  [string]$Python = "python",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $EnvFile) {
  $EnvFile = Join-Path $repoRoot ".env.local"
}

function Import-DotEnv {
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    return $false
  }

  foreach ($line in Get-Content -LiteralPath $Path) {
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith("#")) {
      continue
    }

    $parts = $trimmed.Split("=", 2)
    if ($parts.Count -ne 2) {
      continue
    }

    $name = $parts[0].Trim()
    $value = $parts[1].Trim()
    if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
      $value = $value.Substring(1, $value.Length - 2)
    }

    [Environment]::SetEnvironmentVariable($name, $value, "Process")
  }

  return $true
}

function Require-Env {
  param([string[]]$Names)

  foreach ($name in $Names) {
    if (-not [Environment]::GetEnvironmentVariable($name, "Process")) {
      throw "Missing required environment variable: $name"
    }
  }
}

$logDir = Join-Path $repoRoot "logs"
$logFile = Join-Path $logDir ("local-refresh-{0}.log" -f (Get-Date -Format "yyyyMMdd"))

if ($DryRun) {
  Write-Host "DryRun: repo=$repoRoot target=$Target env=$EnvFile python=$Python log=$logFile"
  exit 0
}

$loadedEnvFile = Import-DotEnv $EnvFile

if ($Target) {
  [Environment]::SetEnvironmentVariable("RUN_TARGET", $Target, "Process")
} elseif (-not [Environment]::GetEnvironmentVariable("RUN_TARGET", "Process")) {
  [Environment]::SetEnvironmentVariable("RUN_TARGET", "all", "Process")
}

try {
  Require-Env @("DATABASE_URL", "VAPID_PRIVATE_KEY", "VAPID_SUBJECT")
} catch {
  if (-not $loadedEnvFile) {
    throw "Missing env file and required environment variables are not set. Expected $EnvFile or process env vars."
  }
  throw
}

if (-not (Test-Path -LiteralPath $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}

$currentTarget = [Environment]::GetEnvironmentVariable("RUN_TARGET", "Process")
$startedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
"[$startedAt] local refresh start target=$currentTarget" | Tee-Object -FilePath $logFile -Append | Out-Null

Push-Location $repoRoot
try {
  & $Python refresh_and_notify.py 2>&1 | Tee-Object -FilePath $logFile -Append
  $exitCode = $LASTEXITCODE
} finally {
  Pop-Location
}

$endedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
"[$endedAt] local refresh end target=$currentTarget exit=$exitCode" | Tee-Object -FilePath $logFile -Append | Out-Null
exit $exitCode
