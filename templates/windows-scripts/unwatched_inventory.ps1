param(
  [Parameter(Mandatory = $true)]
  [string]$Root,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot,
  [string]$OutJsonl = '',
  [switch]$IncludeHash,
  [int]$Limit = 0
)

# Emit UTF-8 JSONL inventory of files under $Root (recursive).
# Designed to be invoked from WSL via pwsh.exe -File ...

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')

$runId = "{0}_{1}" -f (Get-Date -Format 'yyyyMMdd_HHmmss_fff'), $PID
if ([string]::IsNullOrWhiteSpace($OutJsonl)) {
  $moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
  $OutJsonl = Join-Path -Path $moveDir -ChildPath "inventory_unwatched_${runId}.jsonl"
}
$dir = Split-Path -Parent $OutJsonl
Ensure-DirectoryLong $dir

function To-JsonLine([hashtable]$h) {
  return ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 5)
}

# Optional lightweight hash (NOT file content) for stable-ish identity.
function QuickHash([string]$s) {
  $sha1 = [System.Security.Cryptography.SHA1]::Create()
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($s)
  $hash = $sha1.ComputeHash($bytes)
  ($hash | ForEach-Object { $_.ToString('x2') }) -join ''
}

$tmpNonce = [guid]::NewGuid().ToString('N')
$tmpBody = "$OutJsonl.$PID.$tmpNonce.tmpbody"
$tmpOut = "$OutJsonl.$PID.$tmpNonce.tmpwrite"
$bodySw = New-Object System.IO.StreamWriter($tmpBody, $false, (New-Object System.Text.UTF8Encoding($false)))
$warnBag = @()
$warningCount = 0
try {
  $i = 0
  foreach ($f in (Get-ChildFilesLong -Root $Root -Warnings ([ref]$warnBag))) {
    $i++
    if ($Limit -gt 0 -and $i -gt $Limit) { break }

    $full = [string]$f.FullName
    $name = [string]$f.Name
    $size = [int64]$f.Length
    $mtime = ([datetime]$f.LastWriteTimeUtc).ToString('o')

    $dir = Split-Path -Parent $full
    $ext = [System.IO.Path]::GetExtension($name).ToLowerInvariant()

    # Match ingest_inventory_jsonl.py expected schema
    $rec = @{
      path = $full
      dir = $dir
      name = $name
      ext = $ext
      type = 'file'
      size = $size
      mtimeUtc = $mtime
      nameFlags = @{}
    }

    if ($IncludeHash) {
      $rec.nameFlags.quickHash = (QuickHash("$full|$size|$mtime"))
    }

    $bodySw.WriteLine((To-JsonLine $rec))
  }

  $warningCount = @($warnBag).Count
  foreach ($w in @($warnBag)) {
    [Console]::Error.WriteLine("warning: [$($w.code)] $($w.path) :: $($w.message)")
  }

  $meta = @{
    _meta = @{
      kind = 'unwatched_inventory'
      run_id = $runId
      root = $Root
      generated_at = (Get-Date).ToString('o')
      include_hash = [bool]$IncludeHash
      warning_count = $warningCount
    }
  }

  $bodySw.Flush()
  $bodySw.Close()

  $sw = New-Object System.IO.StreamWriter($tmpOut, $false, (New-Object System.Text.UTF8Encoding($false)))
  try {
    $sw.WriteLine((To-JsonLine $meta))
    $fs = New-Object System.IO.FileStream($tmpBody, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    $sr = New-Object System.IO.StreamReader($fs, (New-Object System.Text.UTF8Encoding($false)))
    try {
      while (($line = $sr.ReadLine()) -ne $null) {
        $sw.WriteLine($line)
      }
    }
    finally {
      $sr.Close()
    }
  }
  finally {
    $sw.Flush()
    $sw.Close()
  }
  Move-FileLong -Src $tmpOut -Dst $OutJsonl -Overwrite $true
}
finally {
  try { $bodySw.Flush(); $bodySw.Close() } catch {}
  if (Test-Path -LiteralPath $tmpBody) { Remove-Item -LiteralPath $tmpBody -Force -ErrorAction SilentlyContinue }
  if (Test-Path -LiteralPath $tmpOut) { Remove-Item -LiteralPath $tmpOut -Force -ErrorAction SilentlyContinue }
}

Write-Output (To-JsonLine @{
  run_id = $runId
  out_jsonl = $OutJsonl
  warning_count = $warningCount
})
