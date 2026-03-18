param(
  [Parameter(Mandatory = $true)]
  [string]$PlanJsonl,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Continue'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')

if (!(Test-PathFileLong $PlanJsonl)) {
  throw "Plan not found: $PlanJsonl"
}

$runId = "{0}_{1}" -f (Get-Date -Format 'yyyyMMdd_HHmmss_fff'), $PID
$moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
$out = Join-Path -Path $moveDir -ChildPath "normalize_case_apply_${runId}.jsonl"
Ensure-DirectoryLong $moveDir

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

$sw = New-Object System.IO.StreamWriter($out, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((J @{ _meta = @{ kind='normalize_case_apply'; run_id=$runId; plan=$PlanJsonl; generated_at=(Get-Date).ToString('o') } }))

  Get-Content -LiteralPath $PlanJsonl -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (!$line) { return }
    if ($line.StartsWith('{') -and $line.Contains('"_meta"')) { return }

    $o = $null
    try { $o = $line | ConvertFrom-Json } catch { return }
    if ($o.status -ne 'planned') { return }

    $src = [string]$o.src
    $dst = [string]$o.dst
    $ts = (Get-Date).ToString('o')

    if ([string]::IsNullOrWhiteSpace($src) -or [string]::IsNullOrWhiteSpace($dst)) {
      $sw.WriteLine((J @{ op='normalize_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='missing_src_or_dst' }))
      return
    }

    if ($src.ToLowerInvariant() -ne $dst.ToLowerInvariant()) {
      $sw.WriteLine((J @{ op='normalize_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='not_case_only_pair' }))
      return
    }

    if (!(Test-PathDirLong $src)) {
      $sw.WriteLine((J @{ op='normalize_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='src_dir_not_found' }))
      return
    }

    $srcParent = Split-Path -Parent $src
    $dstParent = Split-Path -Parent $dst
    $srcLeaf = Split-Path -Leaf $src
    $dstLeaf = Split-Path -Leaf $dst

    if ($srcParent.ToLowerInvariant() -ne $dstParent.ToLowerInvariant()) {
      $sw.WriteLine((J @{ op='normalize_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='parent_mismatch' }))
      return
    }

    # Two-step rename to force case change on case-insensitive filesystem.
    $tmpLeaf = "__case_tmp_${runId}_$([guid]::NewGuid().ToString('N'))"
    $tmpPath = Join-Path -Path $srcParent -ChildPath $tmpLeaf

    try {
      Rename-Item -LiteralPath $src -NewName $tmpLeaf -ErrorAction Stop
      Rename-Item -LiteralPath $tmpPath -NewName $dstLeaf -ErrorAction Stop
      $sw.WriteLine((J @{ op='normalize_case'; ts=$ts; src=$src; dst=$dst; ok=$true }))
    } catch {
      # Best effort rollback if 2nd step failed.
      try {
        if (Test-PathDirLong $tmpPath) {
          Rename-Item -LiteralPath $tmpPath -NewName $srcLeaf -ErrorAction SilentlyContinue
        }
      } catch {}
      $sw.WriteLine((J @{ op='normalize_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error=$_.Exception.Message }))
    }
  }
}
finally {
  $sw.Flush(); $sw.Close()
}

Write-Output (J @{ run_id=$runId; out_jsonl=$out })
