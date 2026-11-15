param(
  [Parameter(Mandatory = $true)][string]$Src,
  [Parameter(Mandatory = $true)][string]$Dst
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_long_path_utils.ps1')
. (Join-Path $PSScriptRoot '_move_bundle.ps1')

function Get-LiteralAttributesOrMissing([string]$Path) {
  try { return [System.IO.File]::GetAttributes((Convert-ToLongPathLiteral $Path)) }
  catch {
    $cause = $_.Exception.GetBaseException()
    if ($cause -is [System.IO.FileNotFoundException] -or $cause -is [System.IO.DirectoryNotFoundException]) {
      return $null
    }
    throw
  }
}

try {
  Assert-NoReparseAncestors $Src
  Assert-NoReparseAncestors $Dst
  $attrs = Get-LiteralAttributesOrMissing $Src
  $exists = $null -ne $attrs -and ($attrs -band [System.IO.FileAttributes]::Directory) -eq 0
  $size = $null
  $mtime = $null
  if ($exists) {
    $info = [System.IO.FileInfo]::new((Convert-ToLongPathLiteral $Src))
    $size = [int64]$info.Length
    $mtime = $info.LastWriteTimeUtc.ToString('o')
  }
  $destAttrs = Get-LiteralAttributesOrMissing $Dst
  $destinationExists = $null -ne $destAttrs
  $bundleVerified = $true
  $bundleError = $null
  $cohort = @(Get-CurrentCompanionKinds $Src)
  $members = @()
  if ($exists) {
    foreach ($kind in @('video') + $cohort) {
      $memberSrc = Get-BundlePath $Src $kind
      $memberDst = Get-BundlePath $Dst $kind
      Assert-NoReparseAncestors $memberSrc
      Assert-NoReparseAncestors $memberDst
      $memberInfo = [System.IO.FileInfo]::new((Convert-ToLongPathLiteral $memberSrc))
      $members += @{ kind=$kind; src=$memberSrc; dst=$memberDst; precondition=@{ size_bytes=[int64]$memberInfo.Length; mtime_utc=$memberInfo.LastWriteTimeUtc.ToString('o') } }
    }
    foreach ($kind in @('video', 'program_txt', 'recording_json', 'psc', 'vtt')) {
      $target = Get-BundlePath $Dst $kind
      Assert-NoReparseAncestors $target
      if ($null -ne (Get-LiteralAttributesOrMissing $target)) { $destinationExists = $true; $bundleVerified = $false; $bundleError = 'destination_exists' }
    }
  }
  [ordered]@{
    ok = $true
    bundle_verified = $bundleVerified
    bundle_error = $bundleError
    bundle_version = 1
    members = @($members)
    companion_cohort = @($cohort)
    source_exists = [bool]$exists
    size_bytes = $size
    mtime_utc = $mtime
    destination_exists = $destinationExists
  } | ConvertTo-Json -Compress -Depth 6
} catch {
  @{ ok = $false; error = $_.Exception.Message } | ConvertTo-Json -Compress -Depth 6
  exit 1
}
