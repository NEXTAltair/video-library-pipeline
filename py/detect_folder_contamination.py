#!/usr/bin/env python3
"""Detect contaminated program_title values in path_metadata.

Scans path_metadata.program_title for subtitle/episode contamination by
cross-referencing human-reviewed titles and the programs table.

Source priority for canonical title:
  1. human_reviewed path_metadata (exact match → not contaminated; prefix match → suggested)
  2. programs table (longest prefix match)
  3. separator split fallback

Generates updateInstructions (path_id based) compatible with update_program_titles.py.

Usage:
  python detect_folder_contamination.py --db mediaops.sqlite --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from typing import Any

from mediaops_schema import connect_db
from path_placement_rules import normalize_title_for_comparison

SEPARATOR_RE = re.compile(r"[▽▼◇「]")
MIN_EXTRA_CHARS_DEFAULT = 4


def clean_title(title: str) -> str:
    """Split at first separator and return the prefix."""
    return SEPARATOR_RE.split(title, maxsplit=1)[0].strip()


def load_human_reviewed_titles(con: sqlite3.Connection) -> set[str]:
    """Load distinct program_titles from human-reviewed path_metadata."""
    titles: set[str] = set()
    try:
        rows = con.execute(
            """SELECT DISTINCT program_title FROM path_metadata
               WHERE program_title IS NOT NULL AND program_title != ''
                 AND (source = 'human_reviewed' OR human_reviewed = 1)"""
        ).fetchall()
        for r in rows:
            titles.add(str(r["program_title"]).strip())
    except sqlite3.OperationalError:
        pass
    titles.discard("")
    return titles


def load_programs_titles(con: sqlite3.Connection) -> set[str]:
    """Load canonical_title values from programs table."""
    titles: set[str] = set()
    try:
        rows = con.execute(
            "SELECT canonical_title FROM programs WHERE canonical_title IS NOT NULL AND canonical_title != ''"
        ).fetchall()
        for r in rows:
            titles.add(str(r["canonical_title"]).strip())
    except sqlite3.OperationalError:
        pass
    titles.discard("")
    return titles


def match_against_titles(
    program_title: str,
    canonical_titles: set[str],
    source_label: str,
    min_extra_chars: int = MIN_EXTRA_CHARS_DEFAULT,
) -> tuple[str | None, str]:
    """Longest-prefix match of program_title against canonical titles.

    Returns (suggested_title, match_source).
    """
    pt_norm = normalize_title_for_comparison(program_title)
    if not pt_norm:
        return None, "no_match"

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

    if best_title:
        return best_title, source_label
    return None, "no_match"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-extra-chars", type=int, default=MIN_EXTRA_CHARS_DEFAULT)
    args = ap.parse_args()

    con = connect_db(args.db)

    # Source 1 (highest priority): human-reviewed titles
    hr_titles = load_human_reviewed_titles(con)
    hr_titles_norm = {normalize_title_for_comparison(t) for t in hr_titles}
    hr_titles_norm.discard("")

    # Source 2: programs table
    programs_titles = load_programs_titles(con)

    # Query all distinct program_title values with their path_ids
    rows = con.execute(
        """SELECT pm.path_id, pm.program_title
           FROM path_metadata pm
           WHERE pm.program_title IS NOT NULL AND pm.program_title != ''"""
    ).fetchall()

    # Group path_ids by program_title
    title_to_path_ids: dict[str, list[str]] = {}
    for r in rows:
        pt = str(r["program_title"]).strip()
        if pt:
            title_to_path_ids.setdefault(pt, []).append(str(r["path_id"]))

    contaminated_titles: list[dict[str, Any]] = []
    update_instructions: list[dict[str, str]] = []

    for program_title, path_ids in sorted(title_to_path_ids.items()):
        has_separator = bool(SEPARATOR_RE.search(program_title))
        pt_norm = normalize_title_for_comparison(program_title)

        # Exact match against human-reviewed titles → NOT contaminated
        if pt_norm in hr_titles_norm:
            continue

        # Priority 1: human-reviewed titles (prefix match)
        suggested_title, match_source = match_against_titles(
            program_title, hr_titles, "human_reviewed",
            min_extra_chars=args.min_extra_chars,
        )

        # Priority 2: programs table (prefix match)
        if suggested_title is None:
            suggested_title, match_source = match_against_titles(
                program_title, programs_titles, "programs_table",
                min_extra_chars=args.min_extra_chars,
            )

        # Priority 3: separator split fallback
        if suggested_title is None and has_separator:
            cleaned = clean_title(program_title)
            if cleaned and cleaned != program_title:
                suggested_title = cleaned
                match_source = "separator_split"

        if suggested_title is None:
            continue

        # Verify title is actually different
        if pt_norm == normalize_title_for_comparison(suggested_title):
            continue

        confidence = (
            "high" if match_source == "human_reviewed"
            else "medium" if match_source == "programs_table"
            else "low"
        )

        entry: dict[str, Any] = {
            "programTitle": program_title,
            "suggestedTitle": suggested_title,
            "matchSource": match_source,
            "confidence": confidence,
            "separatorFound": SEPARATOR_RE.search(program_title).group() if has_separator else None,
            "affectedFiles": len(path_ids),
            "pathIds": path_ids,
        }
        contaminated_titles.append(entry)

        # Build path_id based update instructions
        for pid in path_ids:
            update_instructions.append({
                "path_id": pid,
                "new_title": suggested_title,
            })

    total_affected = sum(e["affectedFiles"] for e in contaminated_titles)

    result: dict[str, Any] = {
        "ok": True,
        "dryRun": args.dry_run,
        "totalContaminatedTitles": len(contaminated_titles),
        "totalAffectedFiles": total_affected,
        "contaminatedTitles": contaminated_titles,
        "updateInstructions": update_instructions,
    }

    con.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
