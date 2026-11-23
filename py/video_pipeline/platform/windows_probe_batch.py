"""Read exact NTFS directory entries in one read-only Windows process per batch."""
import base64
import json
import re
import subprocess

from video_pipeline.platform.windows_pwsh_bridge import find_pwsh7_executable


# Static helper compiled in memory by PowerShell 7. One Win32 metadata call per
# distinct literal entry returns attributes, size and full FILETIME together.
# No enumerations, temporary manifests, dynamic code from paths, or open handles.
_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text.Json;
public static class ExactEntryProbe {
    [StructLayout(LayoutKind.Sequential)]
    private struct Data {
        public uint attributes, createLow, createHigh, accessLow, accessHigh,
            writeLow, writeHigh, sizeHigh, sizeLow;
    }
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, ExactSpelling=true, SetLastError=true)]
    private static extern bool GetFileAttributesExW(string path, int level, out Data data);

    public static string Read(string json) {
        var paths = JsonSerializer.Deserialize<string[]>(json);
        var rows = new object[paths.Length];
        for (int i=0; i<paths.Length; i++) {
            var path = paths[i];
            if (!GetFileAttributesExW(@"\\?\" + path, 0, out var d)) {
                int error = Marshal.GetLastWin32Error();
                rows[i] = new { path, kind=(error==2 || error==3 ? "missing" : "unknown") };
                continue;
            }
            string kind = (d.attributes & 0x400)!=0 ? "reparse"
                        : (d.attributes & 0x10)!=0 ? "directory"
                        : (d.attributes & 0x40)!=0 ? "other" : "file";
            rows[i] = new { path, kind, size=((long)d.sizeHigh<<32)|d.sizeLow,
                            ticks=((long)d.writeHigh<<32)|d.writeLow };
        }
        return JsonSerializer.Serialize(rows);
    }
}
'@
[ExactEntryProbe]::Read([Console]::In.ReadToEnd())
"""


def read_windows_entries(paths, exe):
    """Exact, case-preserving, ordered requests. Missing output is never absence."""
    if not paths:
        return {}
    if any(not re.fullmatch(r'[A-Za-z]:\\.*', p) or any(c in p[2:] for c in '\0:/*?"<>|')
           or any(part in ('.', '..') for part in p.split('\\'))
           for p in paths):
        raise ValueError('invalid_windows_probe_path')
    command = base64.b64encode(_SCRIPT.encode('utf-16-le')).decode('ascii')
    cp = subprocess.run([exe, '-NoProfile', '-NonInteractive', '-EncodedCommand', command],
                        input=json.dumps(paths, ensure_ascii=True), capture_output=True,
                        text=True, encoding='utf-8', errors='strict', timeout=120)
    if cp.returncode:
        raise RuntimeError('Windows metadata batch failed: ' + cp.stderr[:600])
    rows = json.loads(cp.stdout)
    if (not isinstance(rows, list) or len(rows) != len(paths)
            or any(not isinstance(r, dict) or r.get('path') != p for p, r in zip(paths, rows))):
        raise ValueError('incomplete_windows_probe_batch')
    return dict(zip(paths, rows))


def windows_entry_reader():
    # Discovery itself starts PowerShell; resolve it once per plan, not per row.
    exe = find_pwsh7_executable()
    return lambda paths: read_windows_entries(paths, exe)
