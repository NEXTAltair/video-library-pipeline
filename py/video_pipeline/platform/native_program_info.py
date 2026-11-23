"""Read exact EDCB sidecars that WSL cannot address on Windows drives."""

from __future__ import annotations

import base64
from functools import cache
import json
from pathlib import Path
import re
import subprocess

from video_pipeline.platform.pathscan_common import wsl_to_windows_path
from video_pipeline.platform.windows_pwsh_bridge import find_pwsh7_executable

# Read the bytes, not PowerShell-decoded text. The normal parser owns EDCB
# encoding, including the optional UTF-8 BOM. File.Exists would collapse
# access/sharing errors into "missing".
_READ_PROGRAM_INFO = r"""
$ErrorActionPreference = 'Stop'
[Console]::InputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$paths = ConvertFrom-Json -InputObject ([Console]::In.ReadToEnd())
$results = foreach ($path in $paths) {
    try {
        $bytes = [IO.File]::ReadAllBytes('\\?\' + $path)
        @{ status = 'read'; data = [Convert]::ToBase64String($bytes) }
    } catch [IO.FileNotFoundException] {
        @{ status = 'missing' }
    } catch [IO.DirectoryNotFoundException] {
        # The parent may really have gone away, but a disconnected drive can
        # also report PATH_NOT_FOUND. Check its root without suppressing errors.
        try {
            $null = [IO.File]::GetAttributes('\\?\' + $path.Substring(0, 3))
            @{ status = 'missing' }
        } catch {
            @{ status = 'error'; message = $_.Exception.GetBaseException().Message }
        }
    } catch {
        @{ status = 'error'; message = $_.Exception.GetBaseException().Message }
    }
}
ConvertTo-Json -InputObject @($results) -Compress
"""


@cache
def _pwsh() -> str:
    return find_pwsh7_executable()


def is_windows_mount_path(path: Path) -> bool:
    return re.match(r"/mnt/[A-Za-z]/", str(path)) is not None


def read_windows_program_info(paths: list[Path]) -> list[bytes | None]:
    """Return bytes or confirmed absence in request order; raise on native failures."""
    if not paths:
        return []
    if not all(map(is_windows_mount_path, paths)):
        raise ValueError("native program info requires Windows drive mount paths")
    request = [wsl_to_windows_path(str(path)) for path in paths]
    # Keep literal paths off the command line: a batch can exceed its length limit.
    encoded = base64.b64encode(_READ_PROGRAM_INFO.encode("utf-16le")).decode("ascii")
    result = subprocess.run(
        [_pwsh(), "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        input=json.dumps(request, ensure_ascii=False),
        capture_output=True,
        encoding="utf-8",
        timeout=60,
    )
    if result.returncode:
        raise OSError(f"Windows program info reader failed (exit {result.returncode}): {result.stderr.strip()}")
    payload = json.loads(result.stdout)
    if not isinstance(payload, list) or len(payload) != len(paths):
        raise ValueError("Windows program info reader returned an incomplete batch")
    contents = []
    for path, row in zip(paths, payload):
        if isinstance(row, dict):
            if row.get("status") == "missing":
                contents.append(None)
                continue
            if row.get("status") == "read" and isinstance(row.get("data"), str):
                contents.append(base64.b64decode(row["data"], validate=True))
                continue
            if row.get("status") == "error":
                raise OSError(f"Windows program info read failed for {path}: {row.get('message')}")
        raise ValueError(f"Invalid Windows program info response for {path}")
    return contents
