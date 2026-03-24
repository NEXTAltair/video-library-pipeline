from __future__ import annotations

from pathlib import PureWindowsPath
import re

_YEAR_RE = re.compile(r"^\d{4}$")
_MONTH_RE = re.compile(r"^(0?[1-9]|1[0-2])$")


def _normalize_windows_path(path: str) -> str:
    return path.replace("/", "\\").strip()


def infer_library_root_from_layout(path: str) -> str | None:
    """Infer library root from canonical layout: <root>\\<program>\\<year>\\<month>\\<file>."""
    normalized = _normalize_windows_path(path)
    if not normalized:
        return None

    parts = PureWindowsPath(normalized).parts
    # Need at least: <root>, <program>, <year>, <month>, <file>
    if len(parts) < 5:
        return None

    year_seg = parts[-3]
    month_seg = parts[-2]
    if not _YEAR_RE.match(year_seg) or not _MONTH_RE.match(month_seg):
        return None

    root_parts = parts[:-4]
    if not root_parts:
        return None

    root = str(PureWindowsPath(*root_parts)).rstrip("\\")
    return root or None


def infer_library_root_from_old_title(path: str, old_titles: set[str]) -> str | None:
    """Infer library root by matching old title folder segment in current path."""
    normalized = _normalize_windows_path(path)
    if not normalized:
        return None

    titles = {t.casefold() for t in old_titles if t}
    if not titles:
        return None

    parts = [p for p in normalized.split("\\") if p]
    for i, seg in enumerate(parts):
        if seg.casefold() not in titles:
            continue
        if i == 0:
            return None
        return "\\".join(parts[:i]).rstrip("\\") or None
    return None


def infer_fallback_drive_root(path: str) -> str | None:
    normalized = _normalize_windows_path(path)
    if len(normalized) >= 3 and normalized[1:3] == ":\\":
        return normalized[:3].upper()
    return None


def resolve_affected_roots(paths: list[str], old_titles: set[str]) -> list[str]:
    """Resolve relocate roots with precedence:

    1) canonical layout inference
    2) old-title segment inference
    3) drive-root fallback
    """
    roots: set[str] = set()
    for path in paths:
        root = (
            infer_library_root_from_layout(path)
            or infer_library_root_from_old_title(path, old_titles)
            or infer_fallback_drive_root(path)
        )
        if root:
            roots.add(root)
    return sorted(roots)
