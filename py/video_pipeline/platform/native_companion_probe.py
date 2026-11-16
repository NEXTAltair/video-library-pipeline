"""Read-only native bundle capture, reusing shipped PowerShell without installation."""
import json
from pathlib import Path
import re
import subprocess

from video_pipeline.platform.pathscan_common import wsl_to_windows_path
from video_pipeline.platform.windows_pwsh_bridge import find_pwsh7_executable


def read_native_bundle(src, dst):
    src, dst = (wsl_to_windows_path(str(p)) for p in (src, dst))
    if any(not re.match(r'^[A-Za-z]:\\', p) for p in (src, dst)):
        raise ValueError('native bundle capture requires drive-rooted Windows paths')
    scripts = Path(__file__).resolve().parents[3] / 'templates' / 'windows-scripts'
    # Send source over stdin: Japanese/long paths and the helpers never consume
    # Windows' command-line length budget. No temp files or live script edits.
    definitions = '\n'.join((scripts / name).read_text(encoding='utf-8')
                            for name in ('_long_path_utils.ps1', '_move_bundle.ps1'))
    probe = '\n'.join(line for line in (scripts / 'inspect_move.ps1').read_text(encoding='utf-8').splitlines()
                      if not line.startswith('. (Join-Path $PSScriptRoot '))
    quote = lambda value: "'" + value.replace("'", "''") + "'"
    source = definitions + '\n& {\n' + probe + '\n} -Src ' + quote(src) + ' -Dst ' + quote(dst)
    result = subprocess.run([find_pwsh7_executable(), '-NoProfile', '-NonInteractive', '-Command',
                             '$code=[Console]::In.ReadToEnd(); & ([ScriptBlock]::Create($code))'],
                            input=source, capture_output=True, text=True, encoding='utf-8',
                            errors='replace', timeout=120)
    if result.returncode:
        raise ValueError('native bundle capture failed: ' + result.stderr[:500])
    for line in reversed(result.stdout.splitlines()):
        try:
            payload = json.loads(line)
        except ValueError:
            continue
        if isinstance(payload, dict):
            return payload
    raise ValueError('native bundle capture returned no JSON')
