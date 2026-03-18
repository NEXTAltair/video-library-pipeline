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
$out = Join-Path -Path $moveDir -ChildPath "folder_case_apply_${runId}.jsonl"
Ensure-DirectoryLong $moveDir

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

$renamed = 0
$skipped = 0
$errors = 0

$sw = New-Object System.IO.StreamWriter($out, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((J @{ _meta = @{ kind='folder_case_apply'; run_id=$runId; plan=$PlanJsonl; generated_at=(Get-Date).ToString('o') } }))

  Get-Content -LiteralPath $PlanJsonl -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (!$line) { return }

    $o = $null
    try { $o = $line | ConvertFrom-Json } catch { return }
    if ($null -ne $o._meta) { return }

    $srcDir = [string]$o.src_dir
    $dstDir = [string]$o.dst_dir
    $ts = (Get-Date).ToString('o')

    if ([string]::IsNullOrWhiteSpace($srcDir) -or [string]::IsNullOrWhiteSpace($dstDir)) {
      $errors += 1
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src_dir=$srcDir; dst_dir=$dstDir; ok=$false; error='missing_src_or_dst' }))
      return
    }

    if ($srcDir -ceq $dstDir) {
      $skipped += 1
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src_dir=$srcDir; dst_dir=$dstDir; ok=$true; skipped='already_correct' }))
      return
    }

    if ($srcDir.ToLowerInvariant() -cne $dstDir.ToLowerInvariant()) {
      $errors += 1
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src_dir=$srcDir; dst_dir=$dstDir; ok=$false; error='not_case_only' }))
      return
    }

    if (!(Test-PathDirLong $srcDir)) {
      $skipped += 1
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src_dir=$srcDir; dst_dir=$dstDir; ok=$true; skipped='src_not_found' }))
      return
    }

    try {
      $parent = Split-Path -Parent $srcDir
      $dstName = Split-Path -Leaf $dstDir
      if ([string]::IsNullOrWhiteSpace($parent) -or [string]::IsNullOrWhiteSpace($dstName)) {
        throw "invalid_parent_or_dst_name"
      }

      $tmpName = "__case_tmp_${runId}_$([guid]::NewGuid().ToString('N'))"
      $tmpPath = Join-Path -Path $parent -ChildPath $tmpName

      [System.IO.Directory]::Move((Convert-ToLongPathLiteral $srcDir), (Convert-ToLongPathLiteral $tmpPath))
      [System.IO.Directory]::Move((Convert-ToLongPathLiteral $tmpPath), (Convert-ToLongPathLiteral $dstDir))

      $renamed += 1
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src_dir=$srcDir; dst_dir=$dstDir; ok=$true }))
    } catch {
      $errors += 1
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src_dir=$srcDir; dst_dir=$dstDir; ok=$false; error=$_.Exception.Message }))
    }
  }
}
finally {
  $sw.Flush(); $sw.Close()
}

$ok = ($errors -eq 0)
Write-Output (J @{ ok=$ok; run_id=$runId; out_jsonl=$out; renamed=$renamed; skipped=$skipped; errors=$errors })
if (-not $ok) {
  exit 1
}
