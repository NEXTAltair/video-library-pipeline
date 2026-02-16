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

$runId = Get-Date -Format 'yyyyMMdd_HHmmss'
if ([string]::IsNullOrWhiteSpace($OutJsonl)) {
  $moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
  $OutJsonl = Join-Path -Path $moveDir -ChildPath "inventory_unwatched_${runId}.jsonl"
}
$dir = Split-Path -Parent $OutJsonl
New-Item -ItemType Directory -Path $dir -Force | Out-Null

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

$meta = @{
  _meta = @{
    kind = 'unwatched_inventory'
    run_id = $runId
    root = $Root
    generated_at = (Get-Date).ToString('o')
    include_hash = [bool]$IncludeHash
  }
}

$sw = New-Object System.IO.StreamWriter($OutJsonl, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((To-JsonLine $meta))

  $i = 0
  Get-ChildItem -LiteralPath $Root -Recurse -Force -File -ErrorAction SilentlyContinue | ForEach-Object {
    $i++
    if ($Limit -gt 0 -and $i -gt $Limit) { return }

    $full = $_.FullName
    $name = $_.Name
    $size = [int64]$_.Length
    $mtime = $_.LastWriteTimeUtc.ToString('o')

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

    $sw.WriteLine((To-JsonLine $rec))
  }
}
finally {
  $sw.Flush()
  $sw.Close()
}

Write-Output (To-JsonLine @{
  run_id = $runId
  out_jsonl = $OutJsonl
})
