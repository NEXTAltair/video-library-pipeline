#!/usr/bin/env python3
"""Shared canonical title resolution helpers across workflow stages.

This module centralizes:
- loading canonical title sources (human_reviewed + programs)
- prefix-based canonical title suggestion
- subtitle separator cleanup fallback

Keeping this logic in one place reduces stage drift between contamination
inspection, cleanup scripts, and other workflow stages.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass

from path_placement_rules import normalize_title_for_comparison

SUBTITLE_SEPARATOR_RE = re.compile(r"[▽▼◇「]")


@dataclass(frozen=True)
class CanonicalTitleSources:
    """Canonical title candidates grouped by source priority."""

    human_reviewed: tuple[str, ...]
    programs: tuple[str, ...]
    human_reviewed_norm: frozenset[str]


def clean_title_by_separator(title: str) -> str:
    """Split at first subtitle separator and return the prefix."""
    return SUBTITLE_SEPARATOR_RE.split(str(title or ""), maxsplit=1)[0].strip()


def load_canonical_title_sources(con: sqlite3.Connection) -> CanonicalTitleSources:
    """Load canonical title candidates with deterministic ordering.

    Priority is intentionally explicit and stable:
    1) human_reviewed path metadata
    2) programs canonical titles
    """

    def _load_titles(sql: str) -> tuple[str, ...]:
        titles: set[str] = set()
        try:
            rows = con.execute(sql).fetchall()
            for row in rows:
                raw = str(row[0] or "").strip()
                if raw:
                    titles.add(raw)
        except sqlite3.OperationalError:
            return tuple()
        return tuple(sorted(titles, key=lambda s: (len(normalize_title_for_comparison(s)), s), reverse=True))

    human_reviewed = _load_titles(
        """SELECT DISTINCT program_title
           FROM path_metadata
           WHERE program_title IS NOT NULL AND program_title != ''
             AND (source = 'human_reviewed' OR human_reviewed = 1)"""
    )
    programs = _load_titles(
        "SELECT canonical_title FROM programs WHERE canonical_title IS NOT NULL AND canonical_title != ''"
    )

    human_reviewed_norm = frozenset(normalize_title_for_comparison(t) for t in human_reviewed if t)
    return CanonicalTitleSources(
        human_reviewed=human_reviewed,
        programs=programs,
        human_reviewed_norm=human_reviewed_norm,
    )


def suggest_canonical_title(
    program_title: str,
    sources: CanonicalTitleSources,
    *,
    min_extra_chars: int,
) -> tuple[str | None, str]:
    """Suggest canonical title using shared source priority.

    Returns (suggested_title, match_source).
    """
    pt_norm = normalize_title_for_comparison(program_title)
    if not pt_norm:
        return None, "no_match"

    if pt_norm in sources.human_reviewed_norm:
        return None, "exact_human_reviewed"

    def _match_prefix(candidates: tuple[str, ...], source_name: str) -> tuple[str | None, str]:
        for candidate in candidates:
            cand_norm = normalize_title_for_comparison(candidate)
            if not cand_norm:
                continue
            if pt_norm.startswith(cand_norm) and len(pt_norm) >= len(cand_norm) + min_extra_chars:
                return candidate, source_name
        return None, "no_match"

    hr_match, hr_source = _match_prefix(sources.human_reviewed, "human_reviewed")
    if hr_match:
        return hr_match, hr_source

    pr_match, pr_source = _match_prefix(sources.programs, "programs_table")
    if pr_match:
        return pr_match, pr_source

    cleaned = clean_title_by_separator(program_title)
    if cleaned and normalize_title_for_comparison(cleaned) != pt_norm:
        return cleaned, "separator_split"

    return None, "no_match"
