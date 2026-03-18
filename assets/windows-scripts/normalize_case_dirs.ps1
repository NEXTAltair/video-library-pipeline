param(
  [Parameter(Mandatory = $true)]
  [string]$PlanJsonl,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot,
  [switch]$DryRun
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Continue'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')

if (!(Test-Path -LiteralPath $PlanJsonl)) {
  throw "plan not found: $PlanJsonl"
}

$runId = "{0}_{1}" -f (Get-Date -Format 'yyyyMMdd_HHmmss_fff'), $PID
$moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
$out = Join-Path -Path $moveDir -ChildPath "case_normalize_apply_${runId}.jsonl"
Ensure-DirectoryLong $moveDir

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

$sw = New-Object System.IO.StreamWriter($out, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((J @{ _meta = @{ kind='case_normalize_apply'; run_id=$runId; plan=$PlanJsonl; dry_run=[bool]$DryRun; generated_at=(Get-Date).ToString('o') } }))

  Get-Content -LiteralPath $PlanJsonl -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (!$line) { return }
    if ($line.StartsWith('{') -and $line.Contains('"_meta"')) { return }

    try { $o = $line | ConvertFrom-Json } catch { return }
    if ([string]$o.op -ne 'rename_dir_case') { return }

    $src = [string]$o.src
    $dst = [string]$o.dst
    $ts = (Get-Date).ToString('o')

    if ([string]::IsNullOrWhiteSpace($src) -or [string]::IsNullOrWhiteSpace($dst)) {
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='missing_src_or_dst' }))
      return
    }
    if ($src -eq $dst) {
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$true; skipped='already_exact' }))
      return
    }
    if ($src.ToLowerInvariant() -ne $dst.ToLowerInvariant()) {
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='not_case_only_rename' }))
      return
    }

    if (!(Test-PathDirLong $src)) {
      if (Test-PathDirLong $dst) {
        $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$true; skipped='already_normalized' }))
      } else {
        $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error='src_not_found' }))
      }
      return
    }

    if ($DryRun) {
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$true; dry_run=$true }))
      return
    }

    try {
      $parent = Split-Path -Parent $src
      if ([string]::IsNullOrWhiteSpace($parent)) {
        throw "invalid_src_parent"
      }
      $leaf = Split-Path -Leaf $src
      $tmpLeaf = "${leaf}.__case_tmp__" + ([Guid]::NewGuid().ToString('N'))
      $tmp = Join-Path -Path $parent -ChildPath $tmpLeaf

      [System.IO.Directory]::Move((Convert-ToLongPathLiteral $src), (Convert-ToLongPathLiteral $tmp))
      [System.IO.Directory]::Move((Convert-ToLongPathLiteral $tmp), (Convert-ToLongPathLiteral $dst))
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$true }))
    }
    catch {
      $sw.WriteLine((J @{ op='rename_dir_case'; ts=$ts; src=$src; dst=$dst; ok=$false; error=$_.Exception.Message }))
    }
  }
}
finally {
  $sw.Flush(); $sw.Close()
}

Write-Output (J @{ run_id=$runId; out_jsonl=$out })
