param(
  [Parameter(Mandatory = $true)]
  [string]$Root
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'SilentlyContinue'

Get-ChildItem -LiteralPath $Root -Recurse -File -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty FullName
