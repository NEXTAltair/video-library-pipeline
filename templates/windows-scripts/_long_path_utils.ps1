[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()

function Add-LongPathWarning {
  param(
    [ref]$Warnings,
    [string]$Code,
    [string]$Path,
    [string]$Message
  )

  if ($null -eq $Warnings) { return }
  if ($null -eq $Warnings.Value) { $Warnings.Value = @() }
  $Warnings.Value += [pscustomobject]@{
    code = $Code
    path = $Path
    message = $Message
  }
}

function Convert-ToLongPathLiteral {
  param([Parameter(Mandatory = $true)][string]$Path)

  $p = [string]$Path
  if ([string]::IsNullOrWhiteSpace($p)) { throw "path is empty" }
  $p = $p.Replace('/', '\')

  if ($p.StartsWith('\\?\')) { return $p }
  if ($p.StartsWith('\\')) {
    throw "UNC path is not supported in this plugin: $p"
  }
  if ($p -notmatch '^(?i)[A-Z]:\\') {
    throw "path is not drive-rooted Windows path: $p"
  }
  return "\\?\$p"
}

function Convert-FromLongPathLiteral {
  param([Parameter(Mandatory = $true)][string]$Path)

  $p = [string]$Path
  if ($p.StartsWith('\\?\UNC\')) {
    return '\\' + $p.Substring(7)
  }
  if ($p.StartsWith('\\?\')) {
    return $p.Substring(4)
  }
  return $p
}

function Test-PathFileLong {
  param([Parameter(Mandatory = $true)][string]$Path)
  try {
    $lp = Convert-ToLongPathLiteral $Path
    return [System.IO.File]::Exists($lp)
  } catch {
    return $false
  }
}

function Test-PathDirLong {
  param([Parameter(Mandatory = $true)][string]$Path)
  try {
    $lp = Convert-ToLongPathLiteral $Path
    return [System.IO.Directory]::Exists($lp)
  } catch {
    return $false
  }
}

function Test-PathAnyLong {
  param([Parameter(Mandatory = $true)][string]$Path)
  return (Test-PathFileLong $Path) -or (Test-PathDirLong $Path)
}

function Ensure-DirectoryLong {
  param([Parameter(Mandatory = $true)][string]$DirPath)
  $lp = Convert-ToLongPathLiteral $DirPath
  [void][System.IO.Directory]::CreateDirectory($lp)
}

function Move-FileLong {
  param(
    [Parameter(Mandatory = $true)][string]$Src,
    [Parameter(Mandatory = $true)][string]$Dst,
    [bool]$Overwrite = $true
  )
  $srcLong = Convert-ToLongPathLiteral $Src
  $dstLong = Convert-ToLongPathLiteral $Dst
  if ($Overwrite) {
    [System.IO.File]::Move($srcLong, $dstLong, $true)
  } else {
    [System.IO.File]::Move($srcLong, $dstLong)
  }
}

function Resolve-UniquePathSuffixLong {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [string]$SuffixBase = '__dup',
    [int]$MaxAttempts = 10000
  )

  $base = Convert-FromLongPathLiteral (Convert-ToLongPathLiteral $Path)
  if (-not (Test-PathAnyLong $base)) { return $base }

  $dir = [System.IO.Path]::GetDirectoryName($base)
  $name = [System.IO.Path]::GetFileNameWithoutExtension($base)
  $ext = [System.IO.Path]::GetExtension($base)
  if ([string]::IsNullOrEmpty($dir)) {
    throw "cannot resolve parent dir for path: $base"
  }

  for ($i = 1; $i -le $MaxAttempts; $i++) {
    $candidate = [System.IO.Path]::Combine($dir, "$name$SuffixBase$i$ext")
    if (-not (Test-PathAnyLong $candidate)) {
      return $candidate
    }
  }
  throw "failed to resolve unique destination path after $MaxAttempts attempts: $base"
}

function Get-ChildFilesLong {
  param(
    [Parameter(Mandatory = $true)][string]$Root,
    [ref]$Warnings
  )

  $rootNormal = Convert-FromLongPathLiteral (Convert-ToLongPathLiteral $Root)
  if (-not (Test-PathDirLong $rootNormal)) {
    throw "root not found or not a directory: $rootNormal"
  }

  $stack = [System.Collections.Generic.Stack[string]]::new()
  $stack.Push((Convert-ToLongPathLiteral $rootNormal))

  while ($stack.Count -gt 0) {
    $dirLong = $stack.Pop()
    $entries = $null
    try {
      $entries = [System.IO.Directory]::EnumerateFileSystemEntries($dirLong)
    } catch {
      Add-LongPathWarning $Warnings 'enumerate_dir_failed' (Convert-FromLongPathLiteral $dirLong) $_.Exception.Message
      continue
    }

    foreach ($entryLong in $entries) {
      try {
        if ([System.IO.Directory]::Exists($entryLong)) {
          $stack.Push($entryLong)
          continue
        }
        if (-not [System.IO.File]::Exists($entryLong)) {
          continue
        }
        $fi = [System.IO.FileInfo]::new($entryLong)
        $full = Convert-FromLongPathLiteral $entryLong
        [pscustomobject]@{
          FullName = $full
          Name = $fi.Name
          Length = [int64]$fi.Length
          LastWriteTimeUtc = $fi.LastWriteTimeUtc
        }
      } catch {
        Add-LongPathWarning $Warnings 'enumerate_entry_failed' (Convert-FromLongPathLiteral $entryLong) $_.Exception.Message
      }
    }
  }
}
