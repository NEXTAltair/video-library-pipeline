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
from typing import Any

from mediaops_schema import connect_db
from path_placement_rules import SUBTITLE_SEPARATORS, normalize_title_for_comparison
from title_resolution import load_canonical_title_sources, suggest_canonical_title

MIN_EXTRA_CHARS_DEFAULT = 4


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-extra-chars", type=int, default=MIN_EXTRA_CHARS_DEFAULT)
    args = ap.parse_args()

    con = connect_db(args.db)
    sources = load_canonical_title_sources(con)

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
        suggested_title, match_source = suggest_canonical_title(
            program_title,
            sources,
            min_extra_chars=args.min_extra_chars,
        )

        # Exact human-reviewed title → already canonical
        if match_source == "exact_human_reviewed":
            continue

        if suggested_title is None:
            continue

        # Verify title is actually different
        pt_norm = normalize_title_for_comparison(program_title)
        if pt_norm == normalize_title_for_comparison(suggested_title):
            continue

        confidence = (
            "high" if match_source == "human_reviewed"
            else "medium" if match_source == "programs_table"
            else "low"
        )

        has_separator = bool(SUBTITLE_SEPARATORS.search(program_title))
        entry: dict[str, Any] = {
            "programTitle": program_title,
            "suggestedTitle": suggested_title,
            "matchSource": match_source,
            "confidence": confidence,
            "separatorFound": SUBTITLE_SEPARATORS.search(program_title).group() if has_separator else None,
            "affectedFiles": len(path_ids),
            "pathIds": path_ids,
        }
        contaminated_titles.append(entry)

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
