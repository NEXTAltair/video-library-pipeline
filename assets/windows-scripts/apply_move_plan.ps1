param(
  [string]$PlanJsonl,
  [string]$OutJsonl,
  [Parameter(Mandatory = $true)]
  [string]$OpsRoot,
  [switch]$DryRun,
  [ValidateSet('error', 'rename_suffix')]
  [string]$OnDstExists = 'error'
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_long_path_utils.ps1')
. (Join-Path $here '_move_bundle.ps1')

if ([string]::IsNullOrWhiteSpace($PlanJsonl)) {
  throw "PlanJsonl is required"
}
if (!(Test-PathFileLong $PlanJsonl)) {
  throw "Plan not found: $PlanJsonl"
}

$runId = "{0}_{1}" -f (Get-Date -Format 'yyyyMMdd_HHmmss_fff'), $PID
$moveDir = Join-Path -Path $OpsRoot -ChildPath "move"
$out = if ($OutJsonl) { $OutJsonl } else { Join-Path -Path $moveDir -ChildPath "move_apply_${runId}.jsonl" }
if (Test-PathAnyLong $out) { throw "Refusing to overwrite move receipt: $out" }
Ensure-DirectoryLong (Split-Path -Parent $out)
Ensure-DirectoryLong $moveDir

function J([hashtable]$h) { ([pscustomobject]$h | ConvertTo-Json -Compress -Depth 6) }

$sw = New-Object System.IO.StreamWriter($out, $false, (New-Object System.Text.UTF8Encoding($false)))
try {
  $sw.WriteLine((J @{ _meta = @{ kind='move_apply'; run_id=$runId; plan=$PlanJsonl; dry_run=[bool]$DryRun; on_dst_exists=$OnDstExists; generated_at=(Get-Date).ToString('o') } }))
  $sw.Flush(); $sw.BaseStream.Flush($true)

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

    $bundle = @()
    $completed = [System.Collections.Generic.List[object]]::new()
    $dstFinal = $dst
    try {
      $bundle = @(Get-MoveBundle -Operation $o)
      for ($attempt = 0; $attempt -le 10000; $attempt++) {
        $dstFinal = if ($attempt -eq 0) { $dst } else {
          $ext = [System.IO.Path]::GetExtension($dst)
          $dst.Substring(0, $dst.Length - $ext.Length) + "__dup$attempt" + $ext
        }
        foreach ($member in $bundle) { $member.dst = Get-BundlePath $dstFinal $member.kind }
        $candidatePaths = @('video', 'program_txt', 'recording_json', 'psc', 'vtt') | ForEach-Object { Get-BundlePath $dstFinal $_ }
        # A shared stem must not attach Konomi metadata to another container.
        foreach ($extension in @('.mp4', '.mkv', '.m4v', '.ts', '.m2ts', '.m2t', '.mts', '.avi', '.mov', '.webm', '.mpg', '.mpeg')) {
          $sibling = [System.IO.Path]::ChangeExtension($dstFinal, $extension)
          if ($sibling -ine $dstFinal) { $candidatePaths += $sibling }
        }
        foreach ($candidatePath in $candidatePaths) { Assert-NoReparseAncestors $candidatePath }
        $collision = @($candidatePaths | Where-Object { Test-PathAnyLong $_ }).Count -gt 0
        if (!$collision) { break }
        if ($OnDstExists -ne 'rename_suffix') { throw 'dst_exists' }
        if ($attempt -eq 10000) { throw 'dst_suffix_resolve_failed' }
      }
      foreach ($member in $bundle) {
        Assert-NoReparseAncestors $member.src
        Assert-NoReparseAncestors $member.dst
        Assert-BundleSignature $member.src $member.precondition
      }
    } catch {
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$false; error=$_.Exception.Message; bundle_version=1; bundle_complete=$false; members=$bundle }))
      $sw.Flush(); $sw.BaseStream.Flush($true)
      return
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

    $keepHandles = [System.Collections.Generic.List[System.IO.FileStream]]::new()
    try {
      Assert-NoReparseAncestors $src
      Assert-NoReparseAncestors $dstFinal
      $keepers = if ($o.keeps) { @($o.keeps) } elseif ($o.keep) { @($o.keep) } else { @() }
      foreach ($keeper in $keepers) {
        if ($keeper.path -eq $src -or $keeper.path -eq $dstFinal -or !(Test-PathFileLong $keeper.path)) {
          throw 'keeper_missing_or_invalid'
        }
        Assert-NoReparseAncestors $keeper.path
        $keepInfo = [System.IO.FileInfo]::new((Convert-ToLongPathLiteral $keeper.path))
        if (($keepInfo.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'keeper_reparse_point' }
        # Retain an open read-only handle across the move: Windows rejects writes
        # and deletion of the keeper while a duplicate is being quarantined.
        $keepHandles.Add([System.IO.File]::Open((Convert-ToLongPathLiteral $keeper.path), [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read))
        $keepInfo.Refresh()
        $keepTime = if ($keeper.mtime_utc -is [DateTime]) { [DateTimeOffset]$keeper.mtime_utc } else { [DateTimeOffset]::Parse([string]$keeper.mtime_utc) }
        if ($keepInfo.Length -ne [long]$keeper.size_bytes -or [Math]::Abs(($keepInfo.LastWriteTimeUtc - $keepTime.UtcDateTime).TotalMilliseconds) -gt 5) { throw 'keeper_changed' }
      }
      if ($DryRun) {
        $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$true; dry_run=$true; bundle_version=1; bundle_complete=$true; members=$bundle }))
        return
      }
      # Durable intent precedes every filesystem effect. Members move before video.
      $sw.WriteLine((J @{ op='move_bundle_intent'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; members=$bundle }))
      $sw.Flush(); $sw.BaseStream.Flush($true)
      Assert-BundleCohort $o
      foreach ($member in $bundle) {
        $remaining = @($bundle | Where-Object { $_.kind -ne 'video' -and $_ -notin $completed } | ForEach-Object { $_.kind } | Sort-Object)
        $current = @(Get-CurrentCompanionKinds $src | Sort-Object)
        if (($remaining -join ',') -cne ($current -join ',')) { throw 'companion_cohort_changed_during_move' }
        $arrived = @($completed | Where-Object { $_.kind -ne 'video' } | ForEach-Object { $_.kind } | Sort-Object)
        $atDestination = @(Get-CurrentCompanionKinds $dstFinal | Sort-Object)
        if (($arrived -join ',') -cne ($atDestination -join ',')) { throw 'destination_cohort_changed_during_move' }
        Assert-NoReparseAncestors $member.src
        Assert-NoReparseAncestors $member.dst
        Assert-BundleSignature $member.src $member.precondition
        if (Test-PathAnyLong $member.dst) { throw 'dst_exists' }
        Move-FileLong -Src $member.src -Dst $member.dst -Overwrite $false
        $completed.Add($member)
        Assert-BundleSignature $member.dst $member.precondition
        if (Test-PathAnyLong $member.src) { throw 'source_still_exists_after_move' }
        $sw.WriteLine((J @{ op='move_member'; path_id=$pathId; kind=$member.kind; src=$member.src; dst=$member.dst; ok=$true }))
        $sw.Flush(); $sw.BaseStream.Flush($true)
      }
      $sourceAfter = @(Get-CurrentCompanionKinds $src)
      $destinationAfter = @(Get-CurrentCompanionKinds $dstFinal | Sort-Object)
      $expectedAfter = @($bundle | Where-Object { $_.kind -ne 'video' } | ForEach-Object { $_.kind } | Sort-Object)
      if ($sourceAfter.Count -ne 0 -or ($destinationAfter -join ',') -cne ($expectedAfter -join ',')) { throw 'companion_cohort_changed_after_move' }
      foreach ($member in $bundle) {
        Assert-BundleSignature $member.dst $member.precondition
        if (Test-PathAnyLong $member.src) { throw 'source_reappeared_after_move' }
      }
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; file_id=$o.file_id; ok=$true; bundle_version=1; bundle_complete=$true; members=$bundle }))
      $sw.Flush(); $sw.BaseStream.Flush($true)
    } catch {
      $moveError = $_.Exception.Message
      $rollbackErrors = @()
      for ($i = $completed.Count - 1; $i -ge 0; $i--) {
        $member = $completed[$i]
        try {
          Assert-NoReparseAncestors $member.src
          Assert-NoReparseAncestors $member.dst
          Assert-BundleSignature $member.dst $member.precondition
          if (Test-PathAnyLong $member.src) { throw 'rollback_source_exists' }
          Move-FileLong -Src $member.dst -Dst $member.src -Overwrite $false
          $sw.WriteLine((J @{ op='rollback_member'; path_id=$pathId; kind=$member.kind; src=$member.dst; dst=$member.src; ok=$true }))
        } catch {
          $rollbackErrors += $_.Exception.Message
          $sw.WriteLine((J @{ op='rollback_member'; path_id=$pathId; kind=$member.kind; src=$member.dst; dst=$member.src; ok=$false; error=$_.Exception.Message }))
        }
        $sw.Flush(); $sw.BaseStream.Flush($true)
      }
      $effects = @(@('video', 'program_txt', 'recording_json', 'psc', 'vtt') | ForEach-Object {
        $effectSrc = Get-BundlePath $src $_
        $effectDst = Get-BundlePath $dstFinal $_
        @{ kind=$_; src=$effectSrc; dst=$effectDst; source_exists=(Test-PathFileLong $effectSrc); destination_exists=(Test-PathFileLong $effectDst) }
      })
      $sw.WriteLine((J @{ op='move'; ts=$ts; path_id=$pathId; src=$src; dst=$dstFinal; ok=$false; error=$moveError; bundle_version=1; bundle_complete=$false; rollback_errors=$rollbackErrors; partial_effects=$effects }))
      $sw.Flush(); $sw.BaseStream.Flush($true)
    } finally {
      foreach ($handle in $keepHandles) { $handle.Dispose() }
    }
  }
}
finally {
  $sw.Flush(); $sw.BaseStream.Flush($true); $sw.Close()
}

Write-Output (J @{ run_id=$runId; out_jsonl=$out })
