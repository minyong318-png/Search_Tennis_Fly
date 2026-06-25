param(
  [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

function Assert-True {
  param(
    [bool]$Condition,
    [string]$Message
  )
  if (-not $Condition) {
    throw $Message
  }
}

function Assert-FileParses {
  param([string]$Path)
  $tokens = $null
  $errors = $null
  [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors) | Out-Null
  Assert-True ($errors.Count -eq 0) "$Path has PowerShell parse errors: $($errors | Out-String)"
}

$runScript = Join-Path $RepoRoot "scripts/run_local_refresh.ps1"
$installScript = Join-Path $RepoRoot "scripts/install_local_refresh_task.ps1"
$envExample = Join-Path $RepoRoot ".env.local.example"

Assert-True (Test-Path -LiteralPath $runScript) "Missing scripts/run_local_refresh.ps1"
Assert-True (Test-Path -LiteralPath $installScript) "Missing scripts/install_local_refresh_task.ps1"
Assert-True (Test-Path -LiteralPath $envExample) "Missing .env.local.example"

Assert-FileParses $runScript
Assert-FileParses $installScript

$envText = Get-Content -Raw -LiteralPath $envExample
foreach ($key in @(
  "DATABASE_URL",
  "VAPID_PUBLIC_KEY",
  "VAPID_PRIVATE_KEY",
  "VAPID_SUBJECT",
  "RUN_TARGET"
)) {
  Assert-True ($envText -match "(?m)^$key=") ".env.local.example missing $key"
}

$dryRun = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runScript -DryRun -Target suwon 2>&1
Assert-True ($LASTEXITCODE -eq 0) "DryRun failed with exit code $LASTEXITCODE`n$($dryRun | Out-String)"
Assert-True (($dryRun | Out-String) -match "DryRun") "DryRun output did not include DryRun marker"

Write-Host "local refresh script tests passed"
