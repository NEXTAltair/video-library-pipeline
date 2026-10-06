"""Filesystem preconditions. An existing destination is never a retryable move."""
from datetime import datetime, timezone
from pathlib import Path
import re
from video_pipeline.platform.pathscan_common import windows_to_wsl_path


def inspect_move(src, dst, inventory, *, localize=windows_to_wsl_path):
    source = Path(localize(src))
    dest = Path(localize(dst))
    if not source.is_file():
        return None, "source_missing"
    stat = source.stat()
    expected_size = inventory.get("size")
    expected_mtime = inventory.get("mtimeUtc") or inventory.get("mtime_utc")
    if expected_size is not None and stat.st_size != int(expected_size):
        return None, "source_changed"
    if expected_mtime:
        # .NET emits 7 fractional digits; Python 3.10 accepts up to 6.
        iso = re.sub(r"(\.\d{6})\d+(?=[+-]|$)", r"\1", expected_mtime.replace("Z", "+00:00"))
        observed = datetime.fromisoformat(iso).timestamp()
        if abs(observed - stat.st_mtime) > 0.005:
            return None, "source_changed"
    # lexists also catches broken symlinks; never overwrite any directory entry.
    if dest.exists() or dest.is_symlink():
        return None, "destination_exists"
    return {"size_bytes":stat.st_size,"mtime_utc":datetime.fromtimestamp(stat.st_mtime,timezone.utc).isoformat()}, None
