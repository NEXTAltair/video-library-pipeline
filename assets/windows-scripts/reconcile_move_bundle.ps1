param(
  [Parameter(Mandatory=$true)][string]$PlanJsonl,
  [Parameter(Mandatory=$true)][string]$LogJsonl,
  [Parameter(Mandatory=$true)][string]$PlanSha256,
  [Parameter(Mandatory=$true)][string]$LogSha256,
  [Parameter(Mandatory=$true)][string]$Src,
  [ValidateSet('inspect','finalize')][string]$Mode='inspect',
  [ValidateSet('error','rename_suffix')][string]$OnDstExists='error',
  [string]$ResolvedDst=''
)
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot '_long_path_utils.ps1')
. (Join-Path $PSScriptRoot '_move_bundle.ps1')
function Hash-File([string]$Path) {
  $sha=[Security.Cryptography.SHA256]::Create()
  $stream=[IO.File]::Open((Convert-ToLongPathLiteral $Path), 'Open', 'Read', 'Read')
  try { return ([BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-','').ToLowerInvariant() }
  finally { $stream.Dispose(); $sha.Dispose() }
}
function Append-Proof($Record) {
  $writer=[IO.StreamWriter]::new((Convert-ToLongPathLiteral $LogJsonl),$true,[Text.UTF8Encoding]::new($false))
  try { $writer.WriteLine(($Record | ConvertTo-Json -Depth 12 -Compress)); $writer.Flush(); $writer.BaseStream.Flush($true) }
  finally { $writer.Dispose() }
}
try {
  $logExists=Test-PathFileLong $LogJsonl
  if (!$logExists -and (Test-PathAnyLong $LogJsonl)) { throw 'recovery_log_not_file' }
  if ((Hash-File $PlanJsonl) -cne $PlanSha256 -or ($logExists -and (Hash-File $LogJsonl) -cne $LogSha256) -or (!$logExists -and ($LogSha256 -cne 'absent' -or $Mode -cne 'inspect'))) { throw 'recovery_artifact_checksum_mismatch' }
  $rows=@(Get-Content -LiteralPath $PlanJsonl -Encoding UTF8 | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json } | Where-Object { $_.src -ceq $Src })
  if ($rows.Count -ne 1) { throw 'recovery_row_ownership' }
  $row=$rows[0]
  if ($row.bundle_version -ne 1 -or !$row.path_id) { throw 'recovery_bundle_contract' }
  if ($ResolvedDst -and $ResolvedDst -cne $row.dst) {
    $ext=[IO.Path]::GetExtension($row.dst)
    $stem=$row.dst.Substring(0,$row.dst.Length-$ext.Length)
    $pattern='^'+[regex]::Escape($stem)+'__dup([1-9][0-9]{0,4})'+[regex]::Escape($ext)+'$'
    $match=[regex]::Match($ResolvedDst,$pattern)
    if ($OnDstExists -cne 'rename_suffix' -or !$match.Success -or [int]$match.Groups[1].Value -gt 10000) { throw 'recovery_suffix_not_authorized' }
    $row.dst=$ResolvedDst
    foreach ($member in $row.members) { $member.dst=Get-BundlePath $ResolvedDst $member.kind }
  }
  $records=@()
  if ($logExists) { $records=@(Get-Content -LiteralPath $LogJsonl -Encoding UTF8 | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json }) }
  $intents=@($records | Where-Object { $_.op -ceq 'move_bundle_intent' -and $_.src -ceq $row.src })
  if ($intents.Count -gt 1) { throw 'recovery_duplicate_intent' }
  $kinds=@('program_txt','recording_json','psc','vtt','video')
  $expected=@(@($row.companion_cohort) + 'video')
  if (@($row.members).Count -ne $expected.Count -or @($expected | Select-Object -Unique).Count -ne $expected.Count) { throw 'recovery_member_contract' }
  $members=@()
  foreach ($kind in $kinds) {
    $found=@($row.members | Where-Object { $_.kind -ceq $kind })
    if ($kind -in $expected) {
      if ($found.Count -ne 1 -or $found[0].src -cne (Get-BundlePath $row.src $kind) -or $found[0].dst -cne (Get-BundlePath $row.dst $kind)) { throw 'recovery_member_identity' }
      $members += $found[0]
    }
  }
  if ($members.Count -ne $expected.Count) { throw 'recovery_unknown_kind' }
  foreach ($path in @($row.src,$row.dst)) {
    if ($path -notmatch '^[A-Za-z]:\\' -or $path -match '\\[A-Za-z]:\\' -or $path -match '(^|\\)\.\.(\\|$)') { throw 'recovery_invalid_path' }
    foreach ($kind in @(Get-CurrentCompanionKinds $path)) { if ($kind -notin $expected) { throw 'recovery_cohort_changed' } }
  }
  if ($row.src -ieq $row.dst) { throw 'recovery_same_path' }
  $checks=@(); $sourceOnly=0
  foreach ($m in $members) {
    if ($m.src -ieq $m.dst) { throw 'recovery_overlapping_member_path' }
    Assert-NoReparseAncestors $m.src; Assert-NoReparseAncestors $m.dst
    $sx=Test-PathFileLong $m.src; $dx=Test-PathFileLong $m.dst
    if ($sx) { Assert-BundleSignature $m.src $m.precondition }
    if ($dx) { Assert-BundleSignature $m.dst $m.precondition }
    if (!$sx -and !$dx) { throw 'recovery_member_missing' }
    if ($sx -and !$dx) { $sourceOnly++; $checks += @{member=$m; state='source_only'}; continue }
    if ($intents.Count -ne 1) { throw 'recovery_missing_intent' }
    $intent=$intents[0]
    if ($intent.path_id -cne $row.path_id -or $intent.dst -cne $row.dst) { throw 'recovery_intent_identity' }
    $im=@($intent.members | Where-Object { $_.kind -ceq $m.kind })
    if (@($intent.members).Count -ne $members.Count -or $im.Count -ne 1 -or $im[0].src -cne $m.src -or $im[0].dst -cne $m.dst -or $im[0].precondition.size_bytes -ne $m.precondition.size_bytes -or [string]$im[0].precondition.mtime_utc -cne [string]$m.precondition.mtime_utc) { throw 'recovery_intent_members' }
    $dh=Hash-File $m.dst
    if ($sx) {
      if ((Hash-File $m.src) -cne $dh) { throw 'recovery_partial_copy_fail_closed' }
    } else {
      $evidence=@($records | Where-Object { $_.src -ceq $m.src -and $_.dst -ceq $m.dst -and $_.path_id -ceq $row.path_id -and $_.kind -ceq $m.kind -and (($_.op -ceq 'move_member' -and $_.ok -eq $true) -or ($_.op -ceq 'bundle_member_verified' -and $_.sha256 -ceq $dh)) })
      if ($evidence.Count -eq 0) { throw 'recovery_missing_member_proof' }
    }
    $checks += @{member=$m; state='at_destination'; sourceExists=$sx; sha256=$dh}
  }
  if ($sourceOnly -eq $members.Count) {
    @{ok=$true;state='not_started'} | ConvertTo-Json -Compress; exit 0
  }
  # Never move or discard partial companions. Only finish already verified copies.
  if ($sourceOnly -gt 0) { throw 'recovery_mixed_partial_bundle_fail_closed' }
  if ($Mode -eq 'inspect') { @{ok=$true;state='ready'} | ConvertTo-Json -Compress; exit 0 }
  $parents=@($records | Where-Object { $_.op -ceq 'move' -and $_.src -ceq $row.src -and $_.dst -ceq $row.dst -and $_.path_id -ceq $row.path_id -and $_.bundle_complete -eq $true -and $_.ok -eq $true })
  if ($parents.Count -gt 1) { throw 'recovery_duplicate_parent' }
  if ($parents.Count -eq 1) {
    if (@($checks | Where-Object { $_.sourceExists }).Count -gt 0) { throw 'recovery_completed_source_reappeared' }
    @{ok=$true;state='complete'} | ConvertTo-Json -Compress; exit 0
  }
  foreach ($check in $checks) {
    $m=$check.member
    # Recheck the exact signatures and hashes immediately before any deletion.
    Assert-BundleSignature $m.dst $m.precondition
    if ((Hash-File $m.dst) -cne $check.sha256) { throw 'recovery_destination_changed' }
    if ($check.sourceExists) {
      Assert-BundleSignature $m.src $m.precondition
      if ((Hash-File $m.src) -cne $check.sha256) { throw 'recovery_source_changed' }
      Append-Proof @{op='bundle_member_verified';path_id=$row.path_id;kind=$m.kind;src=$m.src;dst=$m.dst;sha256=$check.sha256}
      [IO.File]::Delete((Convert-ToLongPathLiteral $m.src))
    }
    Append-Proof @{op='move_member';ok=$true;path_id=$row.path_id;kind=$m.kind;src=$m.src;dst=$m.dst;reconciled=$true}
  }
  foreach ($m in $members) {
    if (Test-PathAnyLong $m.src) { throw 'recovery_source_reappeared' }
    Assert-BundleSignature $m.dst $m.precondition
  }
  $complete=@{}; foreach ($p in $row.PSObject.Properties) { $complete[$p.Name]=$p.Value }
  $complete.op='move'; $complete.ok=$true; $complete.bundle_complete=$true; $complete.reconciled='bundle_members'; $complete.ts=(Get-Date).ToString('o')
  Append-Proof $complete
  @{ok=$true;state='complete'} | ConvertTo-Json -Compress
} catch { @{ok=$false;error=$_.Exception.Message} | ConvertTo-Json -Compress; exit 1 }
