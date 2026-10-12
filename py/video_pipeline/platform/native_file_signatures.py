"""Read-only Windows signatures when DrvFS cannot address native NTFS names."""
import base64
from datetime import datetime, timezone
import json
import ntpath
from pathlib import Path
import subprocess
import uuid

from video_pipeline.platform.pathscan_common import wsl_to_windows_path
from video_pipeline.platform.windows_pwsh_bridge import find_pwsh7_executable


def native_path_key(path):
    return ntpath.normpath(wsl_to_windows_path(str(path))).casefold()


def read_native_signatures(paths):
    win = [wsl_to_windows_path(str(p)) for p in paths]
    if not win:
        return {}
    if any(len(p)<3 or p[1:3] != ':\\' for p in win):
        raise ValueError('native signatures require drive-rooted paths')
    # EncodedCommand/Windows argv has a length ceiling. Put the potentially large
    # exact-path population in an internal, scan-excluded file, not command text.
    work=Path('/mnt')/win[0][0].lower()/'_AI_WORK'/'duplicates'/'native-signatures'
    work.mkdir(parents=True,exist_ok=True)
    request=work/('request-'+uuid.uuid4().hex+'.json')
    request.write_text(json.dumps(win,ensure_ascii=False),encoding='utf-8')
    literal=wsl_to_windows_path(str(request)).replace("'","''")
    script = """$ErrorActionPreference='Stop';[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false)
$paths=Get-Content -LiteralPath 'INPUT_PATH' -Raw -Encoding utf8|ConvertFrom-Json
$out=[System.Collections.Generic.List[object]]::new()
foreach($p in $paths){
 try{
  $long='\\\\?\\'+$p
  $i=[System.IO.FileInfo]::new($long)
  $current=$p
  while($current){
   $probe='\\\\?\\'+$current
   if([System.IO.File]::Exists($probe)-or[System.IO.Directory]::Exists($probe)){
    if([System.IO.File]::GetAttributes($probe)-band[System.IO.FileAttributes]::ReparsePoint){throw 'native_reparse_ancestor'}
   }
   $parent=[System.IO.Path]::GetDirectoryName($current.TrimEnd('\\'))
   if($parent-eq$current){break};$current=$parent
  }
  if(!$i.Exists){$out.Add(@{path=$p;exists=$false;ancestors_checked=$true});continue}
  $out.Add(@{path=$p;exists=$true;ancestors_checked=$true;size_bytes=$i.Length;mtime_ns=($i.LastWriteTimeUtc.Ticks-621355968000000000)*100;mtime_utc=$i.LastWriteTimeUtc.ToString('o')})
 }catch{$out.Add(@{path=$p;exists=$false;error=$_.Exception.Message})}
}
@{entries=@($out)}|ConvertTo-Json -Compress -Depth 8
""".replace('INPUT_PATH',literal)
    command=base64.b64encode(script.encode('utf-16le')).decode('ascii')
    cp=subprocess.run([find_pwsh7_executable(),'-NoProfile','-EncodedCommand',command],capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=120)
    if cp.returncode:
        raise ValueError('native signature scan failed: '+cp.stderr[:500])
    payload=json.loads(cp.stdout)
    result={}
    for item in payload['entries']:
        if item.get('exists'):
            # Match the existing Python signature's UTC rendering (microseconds).
            stamp=datetime.fromtimestamp(int(item['mtime_ns'])/1e9,timezone.utc).isoformat()
            item['signature']={'size_bytes':int(item['size_bytes']),'mtime_ns':int(item['mtime_ns']),'mtime_utc':stamp}
        result[native_path_key(item['path'])]=item
    return result
