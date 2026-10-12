"""Filesystem preconditions. An existing destination is never a retryable move."""
from datetime import datetime, timezone
import errno
from pathlib import Path
import re
from video_pipeline.platform.pathscan_common import windows_to_wsl_path


def _mtime_timestamp(value):
    # .NET emits 7 fractional digits; Python 3.10 accepts up to 6.
    iso = re.sub(r"(\.\d{6})\d+(?=[+-]|$)", r"\1", value.replace("Z", "+00:00"))
    return datetime.fromisoformat(iso).timestamp()


def inspect_move(src, dst, inventory, *, localize=windows_to_wsl_path, windows_probe=None):
    source = Path(localize(src))
    dest = Path(localize(dst))
    try:
        if not source.is_file():
            return None, "source_missing"
        stat = source.stat()
        size, mtime = stat.st_size, stat.st_mtime
        # lexists also catches broken symlinks; never overwrite any directory entry.
        destination_exists = dest.exists() or dest.is_symlink()
    except OSError as exc:
        # NTFS permits Unicode names that exceed WSL's UTF-8 component limit.
        # A Linux stat failure is not evidence that a Windows recording is missing.
        if exc.errno != errno.ENAMETOOLONG or windows_probe is None:
            raise
        observed = windows_probe(src, dst)
        if observed.get("ok") is not True:
            raise RuntimeError("Windows move preflight could not verify file state")
        if observed.get("source_exists") is not True:
            return None, "source_missing"
        size = int(observed["size_bytes"])
        mtime = _mtime_timestamp(observed["mtime_utc"])
        # Missing/ambiguous probe fields fail closed instead of accepting a move.
        destination_exists = observed["destination_exists"]
        if not isinstance(destination_exists, bool):
            raise RuntimeError("Windows move preflight returned ambiguous destination state")
    expected_size = inventory.get("size")
    expected_mtime = inventory.get("mtimeUtc") or inventory.get("mtime_utc")
    if expected_size is not None and size != int(expected_size):
        return None, "source_changed"
    if expected_mtime:
        observed = _mtime_timestamp(expected_mtime)
        if abs(observed - mtime) > 0.005:
            return None, "source_changed"
    if destination_exists:
        return None, "destination_exists"
    return {"size_bytes":size,"mtime_utc":datetime.fromtimestamp(mtime,timezone.utc).isoformat()}, None
