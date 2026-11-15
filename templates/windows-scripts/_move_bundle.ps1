function Assert-NoReparseAncestors([string]$Path) {
  $current = Convert-ToLongPathLiteral $Path
  while ($current) {
    try {
      # Exists() follows links and misses dangling reparse entries. Inspect the
      # entry itself, and suppress only genuine missing-path errors.
      $attributes = [System.IO.File]::GetAttributes($current)
      if ($attributes -band [System.IO.FileAttributes]::ReparsePoint) { throw 'path_reparse_point' }
    } catch {
      $cause = $_.Exception.GetBaseException()
      if ($cause -isnot [System.IO.FileNotFoundException] -and $cause -isnot [System.IO.DirectoryNotFoundException]) { throw }
    }
    $parent = [System.IO.Path]::GetDirectoryName($current.TrimEnd('\'))
    if ($parent -eq $current) { break }
    $current = $parent
  }
}

# Recognized adjacent recording files only; never search an archive or glob suffixes.
function Get-BundlePath([string]$Video, [string]$Kind) {
  switch ($Kind) {
    'video' { return $Video }
    'program_txt' { return $Video + '.program.txt' }
    'recording_json' { return $Video + '.recording.json' }
    'psc' { return [System.IO.Path]::ChangeExtension($Video, '.psc') }
    'vtt' { return [System.IO.Path]::ChangeExtension($Video, '.vtt') }
    default { throw 'invalid_bundle_kind' }
  }
}

function Assert-BundleSignature([string]$Path, $Expected, [switch]$Legacy) {
  if (!(Test-PathFileLong $Path)) { throw 'source_missing' }
  if (!$Expected) { throw 'missing_member_precondition' }
  $info = [System.IO.FileInfo]::new((Convert-ToLongPathLiteral $Path))
  $time = if ($Expected.mtime_utc -is [DateTime]) { [DateTimeOffset]$Expected.mtime_utc } else { [DateTimeOffset]::Parse([string]$Expected.mtime_utc) }
  $changedTime = if ($Legacy) { [Math]::Abs(($info.LastWriteTimeUtc - $time.UtcDateTime).TotalMilliseconds) -gt 5 } else { $info.LastWriteTimeUtc.Ticks -ne $time.UtcDateTime.Ticks }
  if ($info.Length -ne [long]$Expected.size_bytes -or $changedTime) { throw 'source_changed' }
}

function Get-CurrentCompanionKinds([string]$Video) {
  foreach ($kind in @('program_txt', 'recording_json', 'psc', 'vtt')) {
    $path = Get-BundlePath $Video $kind
    Assert-NoReparseAncestors $path
    if (Test-PathAnyLong $path) {
      if (!(Test-PathFileLong $path)) { throw 'companion_not_file' }
      $kind
    }
  }
  # Stem companions must not be silently stolen from another video container.
  # Reject ambiguous ownership even for an explicitly empty cohort.
  foreach ($extension in @('.mp4', '.mkv', '.ts', '.m2ts', '.m2t', '.mts', '.avi', '.mov', '.webm', '.mpg', '.mpeg')) {
    $sibling = [System.IO.Path]::ChangeExtension($Video, $extension)
    if ($sibling -ine $Video -and (Test-PathAnyLong $sibling)) { throw 'ambiguous_companion_owner' }
  }
}

function Assert-BundleCohort($Operation) {
  $current = @(Get-CurrentCompanionKinds $Operation.src | Sort-Object)
  if ($Operation.PSObject.Properties.Name -contains 'companion_cohort') {
    $expected = @($Operation.companion_cohort | Sort-Object)
    if (($expected -join ',') -cne ($current -join ',')) { throw 'companion_cohort_changed' }
  } elseif ($current.Count -gt 0) {
    # Old video-only plans are safe only for recordings with no adjacent companions.
    throw 'companions_require_bundle_plan'
  }
}

function Get-MoveBundle($Operation) {
  Assert-BundleCohort $Operation
  $kinds = @('program_txt', 'recording_json', 'psc', 'vtt')
  $members = @()
  if ($Operation.PSObject.Properties.Name -contains 'companion_cohort') {
    $provided = @($Operation.members)
    $expectedKinds = @(@($Operation.companion_cohort) + 'video')
    if ($provided.Count -ne $expectedKinds.Count) { throw 'invalid_bundle_members' }
    foreach ($kind in $expectedKinds) {
      if ($kind -ne 'video' -and $kind -notin $kinds) { throw 'invalid_bundle_kind' }
      $match = @($provided | Where-Object { $_.kind -ceq $kind })
      if ($match.Count -ne 1 -or $match[0].src -ine (Get-BundlePath $Operation.src $kind) -or $match[0].dst -ine (Get-BundlePath $Operation.dst $kind)) { throw 'invalid_bundle_members' }
    }
    foreach ($kind in @($kinds + 'video')) {
      $member = @($provided | Where-Object { $_.kind -ceq $kind })
      if ($member.Count -eq 1) { $members += $member[0] }
    }
  } else {
    $signature = $Operation.precondition
    if ($signature) { Assert-BundleSignature $Operation.src $signature -Legacy }
    $info = [System.IO.FileInfo]::new((Convert-ToLongPathLiteral $Operation.src))
    $signature = [pscustomobject]@{ size_bytes=$info.Length; mtime_utc=$info.LastWriteTimeUtc.ToString('o') }
    $members = @([pscustomobject]@{ kind='video'; src=$Operation.src; dst=$Operation.dst; precondition=$signature })
  }
  return $members
}
