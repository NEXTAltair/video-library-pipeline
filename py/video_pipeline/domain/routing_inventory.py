"""Genre routing inventory: scope, exact work selection and review summaries."""
from __future__ import annotations

from collections import Counter
from pathlib import PureWindowsPath
from typing import Any

from .path_placement_rules import load_drive_routes, normalize_title_for_comparison
from video_pipeline.platform.pathscan_common import canonicalize_windows_path, windows_to_wsl_path

EXCLUDED_DIRECTORY_NAMES = {"_ai_work", "quarantine_v3", "$recycle.bin", "system volume information"}


def is_recording_path(path: str, ts_root: str) -> bool:
    """Windows component containment, including WSL spellings; not a suffix ban."""
    if not ts_root:
        return False
    candidate = PureWindowsPath(canonicalize_windows_path(windows_to_wsl_path(path)))
    root = PureWindowsPath(canonicalize_windows_path(windows_to_wsl_path(ts_root)))
    return candidate == root or root in candidate.parents


def is_excluded_media_path(path: str, ts_root: str = "") -> bool:
    return (is_recording_path(path, ts_root)
            or any(part.casefold() in EXCLUDED_DIRECTORY_NAMES for part in PureWindowsPath(path).parts))


def default_relocate_roots(source_root: str, dest_root: str, ts_root: str, drive_routes: str) -> list[str]:
    """Use configured media locations, never arbitrary mounted drives."""
    roots = [source_root, dest_root]
    if drive_routes:
        routes = load_drive_routes(drive_routes)
        roots.extend([routes.default_dest, *(r.dest_root for r in routes.routes)])
        roots.extend(str(r.root) for r in routes.work_routes)
        if routes.movie_grouping:
            roots.extend(routes.movie_grouping.destinations())
    result: list[str] = []
    seen: set[str] = set()
    for root in roots:
        if not root or is_recording_path(root, ts_root):
            continue
        canonical = str(PureWindowsPath(canonicalize_windows_path(windows_to_wsl_path(root))))
        key = canonical.casefold()
        if key not in seen:
            seen.add(key)
            result.append(canonical)
    return result


def title_in_scope(titles: list[str], title: str | None, filename: str) -> tuple[bool, bool]:
    """Exact canonical work match; a filename match with conflicting metadata is held."""
    if not titles:
        return True, False
    keys = {normalize_title_for_comparison(t) for t in titles}
    if title and normalize_title_for_comparison(title) in keys:
        return True, False
    filename_key = normalize_title_for_comparison(filename)
    if any(key in filename_key for key in keys):
        return True, bool(title)
    return False, False


def summarize_routing(rows: list[dict[str, Any]], roots: list[str], scanned: list[Any], titles: list[str]) -> dict[str, Any]:
    works: dict[str, dict[str, Any]] = {}
    for row in rows:
        title = str(row.get("program_title") or "(metadata missing)")
        work = works.setdefault(title, {"programTitle": title, "files": 0, "sizeBytes": 0,
                                       "sourceDrives": {}, "destinations": {}, "statuses": {}, "reasons": {}})
        size = int(row.get("size_bytes") or 0)
        work["files"] += 1
        work["sizeBytes"] += size
        drive = PureWindowsPath(str(row.get("src") or "")).drive.upper()
        source = work["sourceDrives"].setdefault(drive, {"files": 0, "sizeBytes": 0})
        source["files"] += 1
        source["sizeBytes"] += size
        if row.get("dst"):
            destination = str(PureWindowsPath(str(row["dst"])).parent)
            dest = work["destinations"].setdefault(destination, {"files": 0, "sizeBytes": 0, "genre": row.get("genre_route")})
            dest["files"] += 1
            dest["sizeBytes"] += size
        for field, value in [("statuses", row.get("status")), ("reasons", row.get("reason"))]:
            work[field][str(value)] = work[field].get(str(value), 0) + 1
    drives = Counter(str(sf.drive or "").upper() for sf in scanned)
    return {"roots": roots, "programTitles": titles, "scannedFiles": len(scanned),
            "scannedByDrive": dict(sorted(drives.items())), "selectedFiles": len(rows),
            "selectedSizeBytes": sum(int(r.get("size_bytes") or 0) for r in rows),
            "works": [works[key] for key in sorted(works)],
            "excludedDirectoryNames": sorted(EXCLUDED_DIRECTORY_NAMES)}
