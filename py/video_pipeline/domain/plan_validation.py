"""Shared move-candidate validation for unwatched-move and relocate flows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from path_placement_rules import (
    SWALLOWED_TITLE_THRESHOLD,
    DriveRoutes,
    build_expected_dest_path,
    build_routed_dest_path,
    detect_subtitle_in_program_title,
    has_required_db_contract,
    normalize_title_for_comparison,
)


@dataclass
class MoveValidationResult:
    ok: bool
    skip_reason: str | None = None
    dst: str | None = None
    genre_route: str | None = None
    warnings: list[str] = field(default_factory=list)


def folder_title_from_path(src_path_win: str) -> str | None:
    """Extract program title folder name from a Windows path.

    Checks by_program\\ pattern first, then VideoLibrary\\ pattern.
    """
    parts = str(src_path_win or "").split("\\")
    for i, seg in enumerate(parts[:-1]):
        if str(seg).lower() == "by_program" and i + 1 < len(parts):
            return parts[i + 1]
    for i, seg in enumerate(parts[:-1]):
        if seg.lower() == "videolibrary" and i + 1 < len(parts):
            return parts[i + 1]
    return None


def detect_swallowed_program_title(src_path_win: str, md: dict[str, Any] | None) -> bool:
    """Detect if LLM copied episode title/body into program_title.

    Returns True if program_title starts with folder title and has
    SWALLOWED_TITLE_THRESHOLD+ extra characters.
    """
    if not isinstance(md, dict):
        return False
    group = folder_title_from_path(src_path_win)
    title = md.get("program_title")
    if not group or not isinstance(title, str):
        return False
    title = title.strip()
    if not title:
        return False
    g = normalize_title_for_comparison(group)
    t = normalize_title_for_comparison(title)
    if not g or not t or t == g:
        return False
    if t.startswith(g) and len(t) >= len(g) + SWALLOWED_TITLE_THRESHOLD:
        return True
    return False


def detect_shortened_program_title(src_path_win: str, md: dict[str, Any] | None) -> bool:
    """Detect if LLM extracted only a short prefix of the folder title.

    Example: folder=RNC_news_every, program_title=RNC
    """
    if not isinstance(md, dict):
        return False
    group = folder_title_from_path(src_path_win)
    title = md.get("program_title")
    if not group or not isinstance(title, str):
        return False
    title = title.strip()
    if not title:
        return False
    g = normalize_title_for_comparison(group)
    t = normalize_title_for_comparison(title)
    if not g or not t or t == g:
        return False
    if g.startswith(t) and len(g) >= len(t) + 3:
        return True
    return False


def validate_move_candidate(
    src: str,
    md: dict[str, Any] | None,
    *,
    allow_needs_review: bool = False,
    check_swallowed_title: bool = True,
    check_subtitle_separator: bool = True,
    routes: DriveRoutes | None = None,
    dest_root: str | None = None,
) -> MoveValidationResult:
    """Validate whether a file should be moved based on its metadata.

    Runs shared checks (contract, needs_review, suspicious titles, dest path)
    and returns a MoveValidationResult.  Callers handle flow-specific side
    effects (counter increments, plan rows, mark_metadata_needs_review, etc.).
    """
    if md is None:
        return MoveValidationResult(ok=False, skip_reason="missing_metadata")

    if not has_required_db_contract(md):
        return MoveValidationResult(ok=False, skip_reason="invalid_metadata_contract")

    if md.get("needs_review") and not allow_needs_review:
        return MoveValidationResult(ok=False, skip_reason="needs_review")

    prog = md.get("program_title")
    air = md.get("air_date")
    if not air or not prog:
        return MoveValidationResult(ok=False, skip_reason="missing_required_fields")

    # Subtitle separator check (▽▼◇)
    if check_subtitle_separator and detect_subtitle_in_program_title(str(prog)):
        return MoveValidationResult(ok=False, skip_reason="subtitle_separator_in_program_title")

    # Swallowed / shortened title checks
    if check_swallowed_title:
        if detect_swallowed_program_title(src, md):
            return MoveValidationResult(ok=False, skip_reason="suspicious_program_title")
        if detect_shortened_program_title(src, md):
            return MoveValidationResult(ok=False, skip_reason="suspicious_program_title_shortened")

    # Build destination path
    genre_route: str | None = None
    if routes:
        dst, genre_route, dst_err = build_routed_dest_path(routes, src, md)
    elif dest_root:
        dst, dst_err = build_expected_dest_path(dest_root, src, md)
    else:
        return MoveValidationResult(ok=False, skip_reason="no_dest_configured")

    if not dst or dst_err:
        return MoveValidationResult(ok=False, skip_reason=dst_err or "build_dest_failed")

    return MoveValidationResult(ok=True, dst=dst, genre_route=genre_route)
