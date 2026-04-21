param(
  [string]$PlanJsonl,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot,
  [switch]$DryRun,
  [ValidateSet('error', 'rename_suffix')]
  [string]$OnDstExists = 'error'
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Continue'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')

if ([string]::IsNullOrWhiteSpace($PlanJsonl)) {
  throw "PlanJsonl is required"
}
if (!(Test-PathFileLong $PlanJsonl)) {
  throw "Plan not found: $PlanJsonl"
}

$runId = "{0}_{1}" -f (Get-Date -Format 'yyyyMMdd_HHmmss_fff'), $PID
$moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
$out = Join-Path -Path $moveDir -ChildPath "move_apply_${runId}.jsonl"
Ensure-DirectoryLong $moveDir

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

$sw = New-Object System.IO.StreamWriter($out, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((J @{ _meta = @{ kind='move_apply'; run_id=$runId; plan=$PlanJsonl; dry_run=[bool]$DryRun; on_dst_exists=$OnDstExists; generated_at=(Get-Date).ToString('o') } }))

  Get-Content -LiteralPath $PlanJsonl -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (!$line) { return }
    if ($line.StartsWith('{') -and $line.Contains('"_meta"')) { return }

    $o = $null
    try { $o = $line | ConvertFrom-Json } catch { return }

    $pathId = $o.path_id
    $src = $o.src
    $dst = $o.dst

    $ts = (Get-Date).ToString('o')

    if ([string]::IsNullOrWhiteSpace($src) -or [string]::IsNullOrWhiteSpace($dst)) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='missing_src_or_dst' }))
      return
    }

    # Guardrail: reject malformed Windows paths that embed another drive path mid-string.
    # Example of bad dst/src:
    #   <destRoot>\\_collisions\\<hash>\\<destRoot>\\...
    $embeddedDrivePattern = '(?i)\\[A-Z]:\\'
    if ($src -match $embeddedDrivePattern -or $dst -match $embeddedDrivePattern) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='invalid_path_embedded_drive' }))
      return
    }

    # Guardrail: require absolute drive-rooted paths.
    if ($src -notmatch '^(?i)[A-Z]:\\' -or $dst -notmatch '^(?i)[A-Z]:\\') {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='invalid_path_not_drive_rooted' }))
      return
    }

    if (!(Test-PathFileLong $src)) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='src_not_found' }))
      return
    }

    $dstFinal = $dst
    if (Test-PathAnyLong $dstFinal) {
      if ($OnDstExists -eq 'rename_suffix') {
        try {
          $dstFinal = Resolve-UniquePathSuffixLong -Path $dstFinal -SuffixBase '__dup'
        } catch {
          $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error=("dst_suffix_resolve_failed: " + $_.Exception.Message) }))
          return
        }
      } else {
        $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='dst_exists' }))
        return
      }
    }

    $dstDir = Split-Path -Parent $dstFinal
    if ([string]::IsNullOrWhiteSpace($dstDir)) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$false; error='invalid_dst_parent' }))
      return
    }
    if (!$DryRun) {
      try {
        Ensure-DirectoryLong $dstDir
      } catch {
        $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$false; error=("mkdir_failed: " + $_.Exception.Message) }))
        return
      }
    } elseif (!(Test-PathDirLong $dstDir)) {
      # Dry-run validates parent path shape but does not create it.
      # No-op if missing.
    }

    if ($DryRun) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$true; dry_run=$true }))
      return
    }

    try {
      Move-FileLong -Src $src -Dst $dstFinal -Overwrite $true
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$true }))
    } catch {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$false; error=$_.Exception.Message }))
    }
  }
}
finally {
  $sw.Flush(); $sw.Close()
}

Write-Output (J @{ run_id=$runId; out_jsonl=$out })
