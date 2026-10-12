"""Read NTFS names that Linux tools cannot open, without renaming the recording.

The native input is a hard-link name, or a SHA256-verified temporary copy when
the source volume cannot hard-link. Both stay in the scan-excluded operations
directory. Cleanup removes only our generated input, and only while the unchanged
original still has the identical native file ID.
If identity cannot be proved, preserve the alias instead of risking the data.
"""
import base64
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import uuid

from video_pipeline.platform.native_file_signatures import read_native_signatures, native_path_key
from video_pipeline.platform.pathscan_common import wsl_to_windows_path
from video_pipeline.platform.windows_pwsh_bridge import find_pwsh7_executable


SCRIPT = r"""$ErrorActionPreference='Stop';[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false)
$ProgressPreference='SilentlyContinue';$PSStyle.OutputRendering='PlainText'
$r=Get-Content -LiteralPath 'REQUEST_PATH' -Raw -Encoding utf8|ConvertFrom-Json
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
public static class NativeMediaRead {
 [StructLayout(LayoutKind.Sequential)] public struct Info {
  public uint attributes; public System.Runtime.InteropServices.ComTypes.FILETIME created;
  public System.Runtime.InteropServices.ComTypes.FILETIME accessed,modified;
  public uint volume,sizeHigh,sizeLow,links,indexHigh,indexLow;
 }
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern bool CreateHardLinkW(string link,string original,IntPtr security);
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern bool DeleteFileW(string path);
 [DllImport("kernel32.dll", SetLastError=true)]
 public static extern bool GetFileInformationByHandle(SafeFileHandle handle,out Info info);
 public static string Identity(string path) {
  using(var stream=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete)) {
   Info i;if(!GetFileInformationByHandle(stream.SafeFileHandle,out i))throw new IOException("native_identity_failed");
   return i.volume.ToString("x8")+":"+i.indexHigh.ToString("x8")+i.indexLow.ToString("x8");
  }
 }
 public static string Hash(string path) {
  using(var stream=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.Read))
  using(var hash=System.Security.Cryptography.SHA256.Create()) {
   return BitConverter.ToString(hash.ComputeHash(stream)).Replace("-","").ToLowerInvariant();
  }
 }
 public static string CopyVerified(string source,string target) {
  string digest;
  using(var input=new FileStream(source,FileMode.Open,FileAccess.Read,FileShare.Read))
  using(var output=new FileStream(target,FileMode.CreateNew,FileAccess.Write,FileShare.None))
  using(var hash=System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256)) {
   byte[] buffer=new byte[4*1024*1024];int count;
   while((count=input.Read(buffer,0,buffer.Length))>0) {
    output.Write(buffer,0,count);hash.AppendData(buffer,0,count);
   }
   output.Flush(true);
   digest=BitConverter.ToString(hash.GetHashAndReset()).Replace("-","").ToLowerInvariant();
  }
  if(Hash(target)!=digest)throw new IOException("native_copy_hash_mismatch");
  return digest;
 }
}
'@
$original='\\?\'+$r.original;$alias='\\?\'+$r.alias
$current=[System.IO.Path]::GetDirectoryName($r.alias)
while($current){
 if([System.IO.File]::GetAttributes('\\?\'+$current)-band[System.IO.FileAttributes]::ReparsePoint){throw 'alias_reparse_ancestor'}
 $parent=[System.IO.Path]::GetDirectoryName($current.TrimEnd('\'));if($parent-eq$current){break};$current=$parent
}
$i=[System.IO.FileInfo]::new($original)
if(!$i.Exists-or$i.Length-ne$r.size_bytes-or($i.LastWriteTimeUtc.Ticks-621355968000000000)*100-ne$r.mtime_ns){throw 'original_changed'}
$id=[NativeMediaRead]::Identity($original)
if($r.action-eq'create'){
 if([System.IO.File]::Exists($alias)){throw 'alias_collision'}
 $mode='hardlink';$digest=$null
 if(![NativeMediaRead]::CreateHardLinkW($alias,$original,[IntPtr]::Zero)){
  $code=[Runtime.InteropServices.Marshal]::GetLastWin32Error()
  if($code-notin@(1,50)){throw ('alias_create_failed:'+$code)}
  $drive=[System.IO.DriveInfo]::new($r.alias.Substring(0,3))
  if($drive.AvailableFreeSpace-lt($r.size_bytes+536870912)){throw 'temporary_copy_space_insufficient'}
  $mode='verified_copy';$digest=[NativeMediaRead]::CopyVerified($original,$alias)
 }
 $aliasId=[NativeMediaRead]::Identity($alias)
 if($mode-eq'hardlink'-and$aliasId-ne$id){throw 'alias_identity_mismatch'}
 @{ok=$true;source_file_id=$id;alias_file_id=$aliasId;alias_created=$true;mode=$mode;source_sha256=$digest;copy_sha256=$digest}|ConvertTo-Json -Compress
}elseif($r.action-eq'cleanup'){
 if($id-ne$r.file_id-or[NativeMediaRead]::Identity($alias)-ne$r.alias_file_id){throw 'alias_cleanup_identity_mismatch'}
 if($r.mode-eq'hardlink'){
  if($r.alias_file_id-ne$id){throw 'alias_cleanup_identity_mismatch'}
 }elseif($r.mode-eq'verified_copy'){
  if(!$r.sha256-or[NativeMediaRead]::Hash($alias)-ne$r.sha256-or[NativeMediaRead]::Hash($original)-ne$r.sha256){throw 'alias_cleanup_hash_mismatch'}
 }else{throw 'alias_cleanup_mode_unknown'}
 # This is resource cleanup of our extra link, never deletion of the original.
 if(![NativeMediaRead]::DeleteFileW($alias)){throw 'alias_cleanup_failed'}
 if([NativeMediaRead]::Identity($original)-ne$id){throw 'original_changed_after_alias_cleanup'}
 @{ok=$true;source_file_id=$id;generated_alias_removed=$true;original_preserved=$true}|ConvertTo-Json -Compress
}else{throw 'invalid_alias_operation'}
"""


def requires_native_alias(path):
    path = Path(path)
    parts = path.parts
    return (len(parts) > 3 and parts[:2] == ('/', 'mnt')
            and len(parts[2]) == 1 and parts[2].isalpha()
            and any(len(p.encode('utf-8')) > 255 for p in parts[3:]))


def _native_alias_operation(request, directory):
    path = directory / (request['action'] + '-request.json')
    path.write_text(json.dumps(request, ensure_ascii=False))
    literal = wsl_to_windows_path(str(path)).replace("'", "''")
    script = SCRIPT.replace('REQUEST_PATH', literal)
    command = base64.b64encode(script.encode('utf-16le')).decode('ascii')
    proc = subprocess.run([find_pwsh7_executable(), '-NoProfile', '-EncodedCommand', command],
                          capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=300)
    if proc.returncode:
        raise ValueError('Native media alias operation failed: ' + proc.stderr[:1500])
    result = json.loads(proc.stdout)
    if result.get('ok') is not True:
        raise ValueError('Native media alias operation incomplete')
    return result


@contextmanager
def readable_media_input(path, *, receipts=None):
    """Yield a readable path; source identity and contents remain unchanged."""
    path = Path(path)
    if not requires_native_alias(path):
        yield path
        return
    observed = read_native_signatures([path])[native_path_key(path)]
    if not observed.get('exists') or observed.get('error'):
        raise ValueError('Unsafe native media input: ' + str(path))
    signature = observed['signature']
    directory = Path('/mnt') / path.parts[2] / '_AI_WORK' / 'duplicates' / 'native-media-read' / uuid.uuid4().hex
    directory.mkdir(parents=True, exist_ok=False)
    alias = directory / ('input' + path.suffix)
    request = {'original': wsl_to_windows_path(str(path)),
               'alias': wsl_to_windows_path(str(alias)),
               'size_bytes': signature['size_bytes'], 'mtime_ns': signature['mtime_ns'],
               'action': 'create'}
    created = _native_alias_operation(request, directory)
    receipt_path = directory / 'receipt.json'
    receipt = {'source_path': str(path), 'signature': signature, 'alias_path': str(alias),
               'create': created, 'cleanup': None, 'original_media_operations': 0}
    receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
    if receipts is not None:
        receipts.append(str(receipt_path))
    try:
        yield alias
    finally:
        request.update(action='cleanup', file_id=created['source_file_id'],
                       alias_file_id=created['alias_file_id'], mode=created.get('mode','hardlink'),
                       sha256=created.get('source_sha256'))
        try:
            receipt['cleanup'] = _native_alias_operation(request, directory)
        except Exception as exc:
            receipt['cleanup_error'] = str(exc)
            raise
        finally:
            receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
