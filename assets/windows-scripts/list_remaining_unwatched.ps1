param(
  [Parameter(Mandatory = $true)]
  [string]$Root
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'SilentlyContinue'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')

$warnBag = @()
try {
  foreach ($f in (Get-ChildFilesLong -Root $Root -Warnings ([ref]$warnBag))) {
    Write-Output ([string]$f.FullName)
  }
} finally {
  foreach ($w in @($warnBag)) {
    [Console]::Error.WriteLine("warning: [$($w.code)] $($w.path) :: $($w.message)")
  }
}
