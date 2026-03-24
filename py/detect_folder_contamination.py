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
from pathlib import PureWindowsPath
from typing import Any

from mediaops_schema import connect_db
from path_placement_rules import SUBTITLE_SEPARATORS, normalize_title_for_comparison
from title_resolution import load_canonical_title_sources, suggest_canonical_title

MIN_EXTRA_CHARS_DEFAULT = 4


def _normalize_path_like(value: str) -> str:
    s = (value or "").strip().replace("/", "\\")
    while "\\\\" in s:
        s = s.replace("\\\\", "\\")
    return s.lower()


def _folder_name_from_path(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return None
    try:
        parts = [x for x in PureWindowsPath(s).parts if x not in ("", "\\")]
    except Exception:
        parts = [x for x in s.replace("/", "\\").split("\\") if x]
    if not parts:
        return None
    return parts[-2] if len(parts) >= 2 else parts[-1]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-extra-chars", type=int, default=MIN_EXTRA_CHARS_DEFAULT)
    ap.add_argument("--target-path", help="Representative wrong path/folder specified by operator")
    ap.add_argument("--target-title", help="Explicit wrong current program_title specified by operator")
    args = ap.parse_args()

    con = connect_db(args.db)
    sources = load_canonical_title_sources(con)

    # Query all rows with path + program_title so we can support user-specified scoping.
    rows = con.execute(
        """SELECT pm.path_id, pm.path, pm.program_title
           FROM path_metadata pm
           WHERE pm.program_title IS NOT NULL AND pm.program_title != ''"""
    ).fetchall()

    target_path = (args.target_path or "").strip()
    target_title = (args.target_title or "").strip()

    candidate_titles: set[str] = set()
    target_path_norm = _normalize_path_like(target_path) if target_path else ""
    if target_path_norm:
        for r in rows:
            row_path_norm = _normalize_path_like(str(r["path"] or ""))
            if not row_path_norm:
                continue
            if row_path_norm == target_path_norm or row_path_norm.startswith(target_path_norm + "\\"):
                pt = str(r["program_title"] or "").strip()
                if pt:
                    candidate_titles.add(pt)

        if not candidate_titles:
            folder_guess = _folder_name_from_path(target_path)
            if folder_guess:
                folder_norm = folder_guess.strip().lower()
                for r in rows:
                    pt = str(r["program_title"] or "").strip()
                    if pt and pt.lower() == folder_norm:
                        candidate_titles.add(pt)

    if target_title:
        candidate_titles.add(target_title)

    # Group path_ids by program_title (optionally scoped to user-specified targets)
    title_to_path_ids: dict[str, list[str]] = {}
    scoped_out_rows = 0
    for r in rows:
        pt = str(r["program_title"]).strip()
        if not pt:
            continue
        if candidate_titles and pt not in candidate_titles:
            scoped_out_rows += 1
            continue
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
        "scope": {
            "targetPath": target_path or None,
            "targetTitle": target_title or None,
            "candidateTitles": sorted(candidate_titles),
            "scopedOutRows": scoped_out_rows,
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
