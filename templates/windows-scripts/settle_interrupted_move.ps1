param(
  [Parameter(Mandatory = $true)][string]$Src,
  [Parameter(Mandatory = $true)][string]$Dst,
  [ValidateSet('inspect', 'finalize', 'discard')]
  [string]$Mode = 'inspect'
)
# Settles one move that was interrupted mid-copy (cross-volume moves copy, then delete).
#   inspect  : read-only. Reports existence, sizes and, when both exist, whether the
#              contents are byte-identical (SHA-256).
#   finalize : both exist and identical -> delete the source (completes the move).
#   discard  : both exist and NOT identical -> delete the destination (a partial copy).
# finalize/discard re-verify immediately before deleting and never touch any other file.
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_long_path_utils.ps1')

function Get-Sha256Long([string]$Path) {
  $sha = [System.Security.Cryptography.SHA256]::Create()
  $stream = [System.IO.File]::Open((Convert-ToLongPathLiteral $Path), [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
  try { return ([System.BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-', '').ToLowerInvariant() }
  finally { $stream.Dispose(); $sha.Dispose() }
}

try {
  $srcExists = Test-PathFileLong $Src
  $dstExists = Test-PathFileLong $Dst
  $srcSize = $null; $dstSize = $null; $identical = $null
  if ($srcExists) { $srcSize = [int64][System.IO.FileInfo]::new((Convert-ToLongPathLiteral $Src)).Length }
  if ($dstExists) { $dstSize = [int64][System.IO.FileInfo]::new((Convert-ToLongPathLiteral $Dst)).Length }
  if ($srcExists -and $dstExists) {
    $identical = ($srcSize -eq $dstSize) -and ((Get-Sha256Long $Src) -eq (Get-Sha256Long $Dst))
  }
  $action = 'none'
  if ($Mode -eq 'finalize') {
    if (-not ($srcExists -and $dstExists -and $identical)) { throw 'finalize_requires_identical_copy' }
    [System.IO.File]::Delete((Convert-ToLongPathLiteral $Src))
    $action = 'source_deleted'
  } elseif ($Mode -eq 'discard') {
    if (-not ($srcExists -and $dstExists) -or $identical) { throw 'discard_requires_differing_partial_copy' }
    [System.IO.File]::Delete((Convert-ToLongPathLiteral $Dst))
    $action = 'destination_deleted'
  }
  [ordered]@{
    ok = $true; mode = $Mode; action = $action
    source_exists = [bool]$srcExists; destination_exists = [bool]$dstExists
    source_size = $srcSize; destination_size = $dstSize; identical = $identical
  } | ConvertTo-Json -Compress
} catch {
  @{ ok = $false; error = $_.Exception.Message } | ConvertTo-Json -Compress
  exit 1
}
