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
  python detect_folder_contamination.py --db mediaops.sqlite --program-title "番組名▽サブタイトル"
  python detect_folder_contamination.py --db mediaops.sqlite --path-like "%\\by_program\\番組名▽サブタイトル\\%"
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
        help="Only analyze rows whose current program_title exactly matches this value.",
    )
    ap.add_argument(
        "--path-like",
        default="",
        help="Only analyze rows whose path matches this SQL LIKE pattern (e.g. %\\\\by_program\\\\title\\\\%).",
    )
    ap.add_argument(
        "--path-id",
        action="append",
        default=[],
        help="Only analyze these path_id values (repeatable).",
    )
    args = ap.parse_args()

    con = connect_db(args.db)
    sources = load_canonical_title_sources(con)

    where_parts = ["pm.program_title IS NOT NULL", "pm.program_title != ''"]
    params: list[Any] = []
    if args.program_title.strip():
        where_parts.append("pm.program_title = ?")
        params.append(args.program_title.strip())
    if args.path_like.strip():
        where_parts.append("pm.path LIKE ?")
        params.append(args.path_like.strip())
    if args.path_id:
        placeholders = ",".join("?" for _ in args.path_id)
        where_parts.append(f"pm.path_id IN ({placeholders})")
        params.extend(str(x) for x in args.path_id)

    where_clause = " AND ".join(where_parts)
    rows = con.execute(
        f"""SELECT pm.path_id, pm.path, pm.program_title
            FROM path_metadata pm
            WHERE {where_clause}""",
        params,
    ).fetchall()

    # Group path_ids by program_title
    title_to_path_ids: dict[str, list[str]] = {}
    title_to_sample_paths: dict[str, list[str]] = {}
    for r in rows:
        pt = str(r["program_title"]).strip()
        if pt:
            title_to_path_ids.setdefault(pt, []).append(str(r["path_id"]))
            p = str(r["path"] or "").strip()
            if p:
                title_to_sample_paths.setdefault(pt, [])
                sample_paths = title_to_sample_paths[pt]
                if p not in sample_paths and len(sample_paths) < 3:
                    sample_paths.append(p)

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
            "samplePaths": title_to_sample_paths.get(program_title, []),
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
        "filters": {
            "programTitle": args.program_title.strip() or None,
            "pathLike": args.path_like.strip() or None,
            "pathIds": [str(x) for x in args.path_id] if args.path_id else [],
        },
        "scannedRows": len(rows),
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
