#!/usr/bin/env python3
"""Shared canonical title resolution helpers across workflow stages.

Centralizes:
- loading canonical title sources (human_reviewed + programs + prefix families)
- prefix-based canonical title suggestion
- subtitle separator cleanup fallback

Keeping this logic in one place reduces stage drift between contamination
detection, cleanup scripts, and other workflow stages.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from path_placement_rules import SUBTITLE_SEPARATORS, clean_program_title, normalize_title_for_comparison


MIN_PREFIX_FAMILY_BASE_LEN = 4
"""Minimum normalized length for a title to qualify as a prefix-family base.

Prevents very short titles (e.g. 'NHK') from matching unrelated longer titles.
"""


@dataclass(frozen=True)
class CanonicalTitleSources:
    """Canonical title candidates grouped by source priority.

    Tuples are sorted by normalized length descending so that
    longest-prefix matching can short-circuit on first hit.
    """

    human_reviewed: tuple[str, ...]
    programs: tuple[str, ...]
    human_reviewed_norm: frozenset[str]
    prefix_families: tuple[str, ...]


def load_canonical_title_sources(con: sqlite3.Connection) -> CanonicalTitleSources:
    """Load canonical title candidates with deterministic ordering.

    Priority:
      1. human_reviewed path metadata (authoritative)
      2. programs canonical titles
      3. prefix families (self-referential: shorter titles that are
         prefixes of other titles in path_metadata)
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
            return ()
        return tuple(sorted(
            titles,
            key=lambda s: (len(normalize_title_for_comparison(s)), s),
            reverse=True,
        ))

    human_reviewed = _load_titles(
        """SELECT DISTINCT program_title
           FROM path_metadata
           WHERE program_title IS NOT NULL AND program_title != ''
             AND (source = 'human_reviewed' OR human_reviewed = 1)"""
    )
    programs = _load_titles(
        "SELECT canonical_title FROM programs "
        "WHERE canonical_title IS NOT NULL AND canonical_title != ''"
    )

    human_reviewed_norm = frozenset(
        normalize_title_for_comparison(t) for t in human_reviewed if t
    )

    prefix_families = _discover_prefix_families(con)

    return CanonicalTitleSources(
        human_reviewed=human_reviewed,
        programs=programs,
        human_reviewed_norm=human_reviewed_norm,
        prefix_families=prefix_families,
    )


def _discover_prefix_families(
    con: sqlite3.Connection,
) -> tuple[str, ...]:
    """Find titles that are prefixes of other titles in path_metadata.

    A title qualifies as a prefix-family base when:
      - its normalized length >= MIN_PREFIX_FAMILY_BASE_LEN
      - at least one other distinct title starts with it

    This intentionally includes titles already in human_reviewed or programs,
    because contaminated variants may also have been marked human_reviewed.
    The suggest_canonical_title() priority chain handles dedup.
    """
    try:
        rows = con.execute(
            "SELECT DISTINCT program_title FROM path_metadata "
            "WHERE program_title IS NOT NULL AND program_title != ''"
        ).fetchall()
    except sqlite3.OperationalError:
        return ()

    # Build norm → original mapping (keep shortest original per norm for determinism)
    norm_to_original: dict[str, str] = {}
    for row in rows:
        raw = str(row[0] or "").strip()
        if not raw:
            continue
        norm = normalize_title_for_comparison(raw)
        if not norm or len(norm) < MIN_PREFIX_FAMILY_BASE_LEN:
            continue
        if norm not in norm_to_original or len(raw) < len(norm_to_original[norm]):
            norm_to_original[norm] = raw

    norms = sorted(norm_to_original.keys())

    bases: set[str] = set()
    for candidate_norm in norms:
        # Check if any other title starts with this candidate
        for other_norm in norms:
            if other_norm == candidate_norm:
                continue
            if (
                other_norm.startswith(candidate_norm)
                and len(other_norm) > len(candidate_norm)
            ):
                bases.add(norm_to_original[candidate_norm])
                break

    return tuple(sorted(
        bases,
        key=lambda s: (len(normalize_title_for_comparison(s)), s),
        reverse=True,
    ))


def longest_prefix_title_match(
    program_title: str,
    canonical_titles: set[str] | tuple[str, ...],
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
        if (
            pt_norm.startswith(ct_norm)
            and len(pt_norm) >= len(ct_norm) + min_extra_chars
            and len(ct_norm) > best_len
        ):
            best_len = len(ct_norm)
            best_title = ct
    return best_title


def suggest_canonical_title(
    program_title: str,
    sources: CanonicalTitleSources,
    *,
    min_extra_chars: int,
) -> tuple[str | None, str]:
    """Suggest canonical title using shared source priority.

    Returns (suggested_title, match_source) where match_source is one of:
      "exact_human_reviewed" — title is already canonical (no change needed)
      "human_reviewed"      — prefix match against human-reviewed titles
      "programs_table"      — prefix match against programs table
      "prefix_family"       — shorter title exists as prefix in DB
      "separator_split"     — fallback: split at first subtitle separator
      "no_match"            — no suggestion available
    """
    pt_norm = normalize_title_for_comparison(program_title)
    if not pt_norm:
        return None, "no_match"

    # Exact match against human-reviewed → already canonical
    if pt_norm in sources.human_reviewed_norm:
        return None, "exact_human_reviewed"

    # Sorted longest-first: first match is the best
    def _match_prefix(
        candidates: tuple[str, ...], source_name: str
    ) -> tuple[str | None, str]:
        for candidate in candidates:
            cand_norm = normalize_title_for_comparison(candidate)
            if not cand_norm:
                continue
            if (
                pt_norm.startswith(cand_norm)
                and len(pt_norm) >= len(cand_norm) + min_extra_chars
            ):
                return candidate, source_name
        return None, "no_match"

    hr_match, hr_source = _match_prefix(sources.human_reviewed, "human_reviewed")
    if hr_match:
        return hr_match, hr_source

    pr_match, pr_source = _match_prefix(sources.programs, "programs_table")
    if pr_match:
        return pr_match, pr_source

    pf_match, pf_source = _match_prefix(sources.prefix_families, "prefix_family")
    if pf_match:
        return pf_match, pf_source

    # Fallback: separator split
    cleaned = clean_program_title(program_title)
    if cleaned and normalize_title_for_comparison(cleaned) != pt_norm:
        return cleaned, "separator_split"

    return None, "no_match"
