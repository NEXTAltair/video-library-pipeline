param(
  [Parameter(Mandatory = $true)]
  [string]$Root,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot,
  [switch]$DryRun,
  [int]$Limit = 0
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$fix = Join-Path $here 'fix_prefix_timestamp_names.ps1'
$norm = Join-Path $here 'normalize_unwatched_names.ps1'

$invokeArgs = @{
  Root = $Root
  OpsRoot = $OpsRoot
}
if ($DryRun) { $invokeArgs.DryRun = $true }
if ($Limit -gt 0) { $invokeArgs.Limit = $Limit }

$fixOut = $null
$normOut = $null
$warnings = @()

if (Test-Path -LiteralPath $fix) {
  $fixOut = & $fix @invokeArgs
} else {
  $warnings += "skip: missing script $fix"
}

if (Test-Path -LiteralPath $norm) {
  $normOut = & $norm @invokeArgs
} else {
  $warnings += "skip: missing script $norm"
}

Write-Output (([pscustomobject]@{
  kind = 'normalize_filenames'
  root = $Root
  dry_run = [bool]$DryRun
  limit = $Limit
  fix_output = $fixOut
  normalize_output = $normOut
  warnings = $warnings
} | ConvertTo-Json -Compress -Depth 8))
