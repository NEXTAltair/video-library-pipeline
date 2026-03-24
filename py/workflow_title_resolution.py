#!/usr/bin/env python3
"""Shared canonical title resolution helpers for review/apply/move workflows."""

from __future__ import annotations

import re
import sqlite3

from path_placement_rules import normalize_title_for_comparison

SUBTITLE_SEPARATOR_RE = re.compile(r"[▽▼◇「]")


def clean_title_prefix(title: str) -> str:
    """Split at first subtitle separator and return the prefix."""
    return SUBTITLE_SEPARATOR_RE.split(str(title or ""), maxsplit=1)[0].strip()


def load_human_reviewed_titles(con: sqlite3.Connection) -> set[str]:
    """Load distinct canonical candidates from human-reviewed path_metadata."""
    titles: set[str] = set()
    try:
        rows = con.execute(
            """SELECT DISTINCT program_title FROM path_metadata
               WHERE program_title IS NOT NULL AND program_title != ''
                 AND (source = 'human_reviewed' OR human_reviewed = 1)"""
        ).fetchall()
    except sqlite3.OperationalError:
        return titles

    for r in rows:
        t = str(r["program_title"] or "").strip()
        if t:
            titles.add(t)
    return titles


def load_programs_titles(con: sqlite3.Connection) -> set[str]:
    """Load canonical_title values from programs table."""
    titles: set[str] = set()
    try:
        rows = con.execute(
            "SELECT canonical_title FROM programs WHERE canonical_title IS NOT NULL AND canonical_title != ''"
        ).fetchall()
    except sqlite3.OperationalError:
        return titles

    for r in rows:
        t = str(r["canonical_title"] or "").strip()
        if t:
            titles.add(t)
    return titles


def longest_prefix_title_match(
    program_title: str,
    canonical_titles: set[str],
    *,
    min_extra_chars: int,
) -> str | None:
    """Return best longest-prefix canonical title match, if any."""
    pt_norm = normalize_title_for_comparison(program_title)
    if not pt_norm:
        return None

    best_title: str | None = None
    best_len = 0
    for ct in canonical_titles:
        ct_norm = normalize_title_for_comparison(ct)
        if not ct_norm:
            continue
        if pt_norm.startswith(ct_norm) and len(pt_norm) >= len(ct_norm) + min_extra_chars:
            if len(ct_norm) > best_len:
                best_len = len(ct_norm)
                best_title = ct
    return best_title
