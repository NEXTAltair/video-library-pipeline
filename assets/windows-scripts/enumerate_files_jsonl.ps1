param(
  [Parameter(Mandatory = $true)]
  [string]$RootsJson,
  [Parameter(Mandatory = $true)]
  [string]$ExtensionsJson,
  [switch]$DetectCorruption,
  [int]$CorruptionReadBytes = 4096
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

function Parse-JsonArray([string]$Text, [string]$Name) {
  try {
    # PowerShell can unwrap single-element JSON arrays unless -NoEnumerate is used.
    $v = $Text | ConvertFrom-Json -NoEnumerate
  } catch {
    throw "invalid $Name json: $($_.Exception.Message)"
  }
  if ($v -isnot [System.Collections.IEnumerable] -or $v -is [string]) {
    throw "$Name must be a JSON array"
  }
  return @($v)
}

function Read-HeadOk {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [int]$ReadBytes = 4096
  )
  try {
    $lp = Convert-ToLongPathLiteral $Path
    $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
      $n = [Math]::Max(1, [int]$ReadBytes)
      $buf = New-Object byte[] $n
      [void]$fs.Read($buf, 0, $buf.Length)
    } finally {
      $fs.Dispose()
    }
    return @{ ok = $true; error = $null }
  } catch {
    return @{ ok = $false; error = $_.Exception.Message }
  }
}

$roots = Parse-JsonArray $RootsJson 'roots'
$extensions = Parse-JsonArray $ExtensionsJson 'extensions' | ForEach-Object {
  $s = [string]$_
  if ([string]::IsNullOrWhiteSpace($s)) { return }
  $x = $s.Trim().ToLowerInvariant()
  if (-not $x.StartsWith('.')) { $x = ".$x" }
  $x
} | Where-Object { $_ } | Sort-Object -Unique

$warnings = @()
$meta = @{
  _meta = @{
    kind = 'enumerate_files_jsonl'
    generated_at = (Get-Date).ToString('o')
    roots = @($roots)
    extensions = @($extensions)
    detect_corruption = [bool]$DetectCorruption
    corruption_read_bytes = [int]([Math]::Max(1, $CorruptionReadBytes))
  }
}
Write-Output (J $meta)

$warningCount = 0
$fileCount = 0
$readBytes = [int]([Math]::Max(1, $CorruptionReadBytes))

foreach ($root in $roots) {
  $rootText = [string]$root
  $warnBag = @()
  try {
    foreach ($f in (Get-ChildFilesLong -Root $rootText -Warnings ([ref]$warnBag))) {
      $ext = [System.IO.Path]::GetExtension([string]$f.Name).ToLowerInvariant()
      if ($extensions.Count -gt 0 -and $ext -notin $extensions) { continue }

      $full = [string]$f.FullName
      $dir = [System.IO.Path]::GetDirectoryName($full)
      $name = [string]$f.Name
      $size = [int64]$f.Length
      $mtimeUtc = ([datetime]$f.LastWriteTimeUtc).ToString('o')

      $corrupt = $false
      $corruptReason = $null
      if ($DetectCorruption) {
        if ($size -eq 0) {
          $corrupt = $true
          $corruptReason = 'size_zero'
        } else {
          $readRes = Read-HeadOk -Path $full -ReadBytes $readBytes
          if (-not $readRes.ok) {
            $corrupt = $true
            $corruptReason = "read_failed:$($readRes.error)"
          }
        }
      }

      Write-Output (J @{
        kind = 'file'
        root = $rootText
        path = $full
        dir = $dir
        name = $name
        ext = $ext
        size = $size
        mtimeUtc = $mtimeUtc
        corruptCandidate = [bool]$corrupt
        corruptReason = $corruptReason
      })
      $fileCount++
    }
  } catch {
    $warnBag += [pscustomobject]@{
      code = 'enumerate_root_failed'
      path = $rootText
      message = $_.Exception.Message
    }
  }

  foreach ($w in $warnBag) {
    $warningCount++
    Write-Output (J @{
      kind = 'warning'
      root = $rootText
      code = [string]$w.code
      path = [string]$w.path
      message = [string]$w.message
    })
  }
}

Write-Output (J @{
  _meta_end = @{
    kind = 'enumerate_files_jsonl_summary'
    file_count = $fileCount
    warning_count = $warningCount
  }
})
