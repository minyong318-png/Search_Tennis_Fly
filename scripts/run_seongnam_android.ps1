param(
  [string]$Python = "python",
  [int]$Port = 9222
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$outerRoot = Split-Path -Parent $root
$lockDir = Join-Path $root ".cache"
$lockPath = Join-Path $lockDir "seongnam_android.lock"
New-Item -ItemType Directory -Force -Path $lockDir | Out-Null

$lockStream = $null
try {
  $lockStream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
} catch {
  Write-Host "[SEONGNAM][ANDROID] another collector is already running"
  exit 3
}

try {
  Set-Location $root
  $adb = $env:SEONGNAM_ADB_PATH
  if (-not $adb) {
    $adb = $env:DAEHOE_TENNISTOWN_ADB_PATH
  }
  if (-not $adb) {
    $candidate = Join-Path $outerRoot ".tools\android-platform-tools\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = Join-Path $outerRoot ".tools\android-sdk\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = Join-Path $root ".tools\android-platform-tools\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = Join-Path $root ".tools\android-sdk\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = "D:\Python_Save\search_tennis_cloudflared\.tools\android-platform-tools\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = "D:\Python_Save\search_tennis_cloudflared\.tools\android-sdk\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb -and $env:ANDROID_HOME) {
    $candidate = Join-Path $env:ANDROID_HOME "platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb -and $env:ANDROID_SDK_ROOT) {
    $candidate = Join-Path $env:ANDROID_SDK_ROOT "platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = "C:\Android\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb) {
    $candidate = "C:\platform-tools\adb.exe"
    if (Test-Path $candidate) { $adb = $candidate }
  }
  if (-not $adb -and (Get-Command adb -ErrorAction SilentlyContinue)) {
    $adb = "adb"
  }
  if (-not $adb) {
    Write-Host "[SEONGNAM][ANDROID] adb not found; install Android platform-tools and add it to PATH"
    exit 2
  }

  $env:SEONGNAM_ADB_PATH = $adb
  $devices = & $adb devices
  Write-Host ($devices -join "`n")
  if (($devices -join "`n") -match "\tunauthorized") {
    Write-Host "[SEONGNAM][ANDROID] allow the USB debugging prompt on the phone, then rerun"
    exit 2
  }
  if (($devices -join "`n") -notmatch "\tdevice") {
    Write-Host "[SEONGNAM][ANDROID] no online Android device"
    exit 2
  }

  & $adb shell am start -a android.intent.action.VIEW -d "https://res.isdc.co.kr/facilityList.do?facType=29" com.android.chrome | Out-Host
  Start-Sleep -Seconds 3
  & $adb forward "tcp:$Port" "localabstract:chrome_devtools_remote" | Out-Host
  $env:RUN_TARGET = "seongnam"
  $env:SEONGNAM_COLLECTOR_MODE = "android"
  if (-not $env:SEONGNAM_DAYS_AHEAD) { $env:SEONGNAM_DAYS_AHEAD = "2" }
  if (-not $env:SEONGNAM_ANDROID_TIMEOUT_MS) { $env:SEONGNAM_ANDROID_TIMEOUT_MS = "600000" }
  if (-not $env:SEONGNAM_ANDROID_RUN_TIMEOUT) { $env:SEONGNAM_ANDROID_RUN_TIMEOUT = "900" }
  if (-not $env:SEONGNAM_ANDROID_FETCH_TIMEOUT_MS) { $env:SEONGNAM_ANDROID_FETCH_TIMEOUT_MS = "10000" }
  if (-not $env:SEONGNAM_ANDROID_CONCURRENCY) { $env:SEONGNAM_ANDROID_CONCURRENCY = "16" }
  if (-not $env:SEONGNAM_ANDROID_BATCH_SIZE) { $env:SEONGNAM_ANDROID_BATCH_SIZE = "120" }
  if (-not $env:SEONGNAM_ANDROID_FETCH_RETRIES) { $env:SEONGNAM_ANDROID_FETCH_RETRIES = "1" }
  & $Python refresh_and_notify.py
  exit $LASTEXITCODE
} finally {
  if ($lockStream) {
    $lockStream.Close()
    $lockStream.Dispose()
  }
}
