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
    ap.add_argument(
        "--program-title",
        default="",
        help="Limit detection to a specific current program_title (exact match).",
    )
    ap.add_argument(
        "--path-like",
        default="",
        help="Resolve candidate program_title values from paths.path LIKE pattern.",
    )
    ap.add_argument(
        "--preferred-title",
        default="",
        help="If provided, override suggestedTitle with this value for matched targets.",
    )
    args = ap.parse_args()

    con = connect_db(args.db)
    sources = load_canonical_title_sources(con)

    # Query all distinct program_title values with their path_ids
    rows = con.execute(
        """SELECT pm.path_id, pm.program_title, p.path
           FROM path_metadata pm
           JOIN paths p ON p.path_id = pm.path_id
           WHERE pm.program_title IS NOT NULL AND pm.program_title != ''"""
    ).fetchall()

    # Group path_ids by program_title
    title_to_path_ids: dict[str, list[str]] = {}
    title_to_paths: dict[str, list[str]] = {}
    for r in rows:
        pt = str(r["program_title"]).strip()
        if pt:
            title_to_path_ids.setdefault(pt, []).append(str(r["path_id"]))
            title_to_paths.setdefault(pt, []).append(str(r["path"] or ""))

    target_titles: set[str] = set(title_to_path_ids.keys())
    explicit_program_title = str(args.program_title or "").strip()
    if explicit_program_title:
        target_titles = {explicit_program_title}

    path_like = str(args.path_like or "").strip()
    if path_like:
        matched_rows = con.execute(
            """SELECT DISTINCT pm.program_title
               FROM path_metadata pm
               JOIN paths p ON p.path_id = pm.path_id
               WHERE p.path LIKE ? AND pm.program_title IS NOT NULL AND pm.program_title != ''""",
            (path_like,),
        ).fetchall()
        matched_titles = {str(r["program_title"]).strip() for r in matched_rows if str(r["program_title"]).strip()}
        if explicit_program_title:
            target_titles = target_titles.intersection(matched_titles)
        else:
            target_titles = matched_titles

    contaminated_titles: list[dict[str, Any]] = []
    update_instructions: list[dict[str, str]] = []

    preferred_title = str(args.preferred_title or "").strip()
    for program_title, path_ids in sorted(title_to_path_ids.items()):
        if program_title not in target_titles:
            continue
        suggested_title, match_source = suggest_canonical_title(
            program_title,
            sources,
            min_extra_chars=args.min_extra_chars,
        )

        # Exact human-reviewed title → already canonical
        if match_source == "exact_human_reviewed":
            continue

        if suggested_title is None and preferred_title:
            suggested_title = preferred_title
            match_source = "operator_override"

        if suggested_title is None:
            continue

        # Verify title is actually different
        pt_norm = normalize_title_for_comparison(program_title)
        if pt_norm == normalize_title_for_comparison(suggested_title):
            continue

        confidence = (
            "high" if match_source == "human_reviewed"
            else "medium" if match_source == "programs_table"
            else "high" if match_source == "operator_override"
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
            "samplePaths": title_to_paths.get(program_title, [])[:3],
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
        "mode": "targeted" if (explicit_program_title or path_like) else "scan_all",
        "targeting": {
            "programTitle": explicit_program_title or None,
            "pathLike": path_like or None,
            "preferredTitle": preferred_title or None,
            "resolvedTargetTitles": sorted(target_titles),
        },
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
