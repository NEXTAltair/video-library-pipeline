param(
  [string]$PlanJsonl,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot,
  [switch]$DryRun
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Continue'

if ([string]::IsNullOrWhiteSpace($PlanJsonl)) {
  throw "PlanJsonl is required"
}
if (!(Test-Path -LiteralPath $PlanJsonl)) {
  throw "Plan not found: $PlanJsonl"
}

$runId = Get-Date -Format 'yyyyMMdd_HHmmss'
$moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
$out = Join-Path -Path $moveDir -ChildPath "move_apply_${runId}.jsonl"
New-Item -ItemType Directory -Path $moveDir -Force | Out-Null

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

$sw = New-Object System.IO.StreamWriter($out, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((J @{ _meta = @{ kind='move_apply'; run_id=$runId; plan=$PlanJsonl; dry_run=[bool]$DryRun; generated_at=(Get-Date).ToString('o') } }))

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

    if (!(Test-Path -LiteralPath $src)) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='src_not_found' }))
      return
    }

    $dstDir = Split-Path -Parent $dst
    if (!(Test-Path -LiteralPath $dstDir)) {
      if (!$DryRun) { New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }
    }

    if (Test-Path -LiteralPath $dst) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error='dst_exists' }))
      return
    }

    if ($DryRun) {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$true; dry_run=$true }))
      return
    }

    try {
      Move-Item -LiteralPath $src -Destination $dst -Force
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$true }))
    } catch {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dst; ok=$false; error=$_.Exception.Message }))
    }
  }
}
finally {
  $sw.Flush(); $sw.Close()
}

Write-Output (J @{ run_id=$runId; out_jsonl=$out })
