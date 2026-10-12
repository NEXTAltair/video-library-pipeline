param(
  [Parameter(Mandatory = $true)][string]$Src,
  [Parameter(Mandatory = $true)][string]$Dst
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_long_path_utils.ps1')

function Get-LiteralAttributesOrMissing([string]$Path) {
  try { return [System.IO.File]::GetAttributes((Convert-ToLongPathLiteral $Path)) }
  catch {
    $cause = $_.Exception.GetBaseException()
    if ($cause -is [System.IO.FileNotFoundException] -or $cause -is [System.IO.DirectoryNotFoundException]) {
      return $null
    }
    throw
  }
}

try {
  $attrs = Get-LiteralAttributesOrMissing $Src
  $exists = $null -ne $attrs -and ($attrs -band [System.IO.FileAttributes]::Directory) -eq 0
  $size = $null
  $mtime = $null
  if ($exists) {
    $info = [System.IO.FileInfo]::new((Convert-ToLongPathLiteral $Src))
    $size = [int64]$info.Length
    $mtime = $info.LastWriteTimeUtc.ToString('o')
  }
  $destAttrs = Get-LiteralAttributesOrMissing $Dst
  [ordered]@{
    ok = $true
    source_exists = [bool]$exists
    size_bytes = $size
    mtime_utc = $mtime
    destination_exists = ($null -ne $destAttrs)
  } | ConvertTo-Json -Compress
} catch {
  @{ ok = $false; error = $_.Exception.Message } | ConvertTo-Json -Compress
  exit 1
}
