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
  python detect_folder_contamination.py --db mediaops.sqlite --path-like "%\\番組名▽サブタイトル\\%"
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
        help="Only analyze rows whose path matches this SQL LIKE pattern (e.g. %%\\\\番組名▽サブタイトル\\\\%%).",
    )
    ap.add_argument(
        "--path-id",
        action="append",
        default=[],
        help="Only analyze these path_id values (repeatable).",
    )
    ap.add_argument(
        "--program-title-contains",
        default="",
        help="Analyze rows whose program_title contains this substring (broad search for bulk review).",
    )
    ap.add_argument(
        "--canonical-title",
        default="",
        help=(
            "Operator-supplied canonical title for forced correction. "
            "When set in targeted mode, builds updateInstructions directly from resolvedTargets, "
            "bypassing auto-detection. Requires at least one targeting param."
        ),
    )
    args = ap.parse_args()

    con = connect_db(args.db)
    sources = load_canonical_title_sources(con)

    # Build dynamic WHERE clause for scoping
    where_parts = ["pm.program_title IS NOT NULL", "pm.program_title != ''"]
    params: list[Any] = []
    needs_path_join = False

    program_title_filter = str(args.program_title or "").strip()
    program_title_contains_filter = str(args.program_title_contains or "").strip()
    path_like_filter = str(args.path_like or "").strip()
    path_id_filters = [str(x).strip() for x in (args.path_id or []) if str(x).strip()]

    if program_title_filter:
        where_parts.append("pm.program_title = ?")
        params.append(program_title_filter)
    if program_title_contains_filter:
        where_parts.append("pm.program_title LIKE '%' || ? || '%'")
        params.append(program_title_contains_filter)
    if path_like_filter:
        needs_path_join = True
        where_parts.append("p.path LIKE ?")
        params.append(path_like_filter)
    if path_id_filters:
        placeholders = ",".join("?" for _ in path_id_filters)
        where_parts.append(f"pm.path_id IN ({placeholders})")
        params.extend(path_id_filters)

    is_targeted = bool(
        program_title_filter or program_title_contains_filter
        or path_like_filter or path_id_filters
    )

    # Always JOIN paths for samplePaths; only filter on p.path when --path-like given
    join_clause = "JOIN paths p ON p.path_id = pm.path_id"
    where_clause = " AND ".join(where_parts)

    rows = con.execute(
        f"""SELECT pm.path_id, pm.program_title, p.path
            FROM path_metadata pm
            {join_clause}
            WHERE {where_clause}""",
        params,
    ).fetchall()

    # Group path_ids by program_title, collect sample paths
    title_to_path_ids: dict[str, list[str]] = {}
    title_to_sample_paths: dict[str, list[str]] = {}
    for r in rows:
        pt = str(r["program_title"]).strip()
        if not pt:
            continue
        title_to_path_ids.setdefault(pt, []).append(str(r["path_id"]))
        path_str = str(r["path"] or "").strip()
        if path_str:
            samples = title_to_sample_paths.setdefault(pt, [])
            if path_str not in samples and len(samples) < 3:
                samples.append(path_str)

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

    # In targeted mode, always return resolvedTargets so the operator can force
    # a correction even when no contamination suggestion was generated.
    # Each entry is enriched with suggestedTitle when available (for YAML pre-fill).
    resolved_targets: list[dict[str, Any]] = []
    if is_targeted:
        for program_title, path_ids in sorted(title_to_path_ids.items()):
            rt_entry: dict[str, Any] = {
                "programTitle": program_title,
                "pathIds": path_ids,
                "affectedFiles": len(path_ids),
                "samplePaths": title_to_sample_paths.get(program_title, []),
            }
            # Enrich: attach suggestion if auto-detect found one
            suggested, match_src = suggest_canonical_title(
                program_title, sources, min_extra_chars=args.min_extra_chars,
            )
            if suggested and match_src != "exact_human_reviewed":
                st_norm = normalize_title_for_comparison(suggested)
                if st_norm != normalize_title_for_comparison(program_title):
                    rt_entry["suggestedTitle"] = suggested
                    rt_entry["matchSource"] = match_src
            resolved_targets.append(rt_entry)

    # Operator-supplied canonical title: build updateInstructions directly from resolvedTargets
    # when auto-detection is insufficient but operator already knows the correct title.
    canonical_title_override = str(args.canonical_title or "").strip()
    operator_forced = False
    if canonical_title_override and is_targeted and resolved_targets:
        already_suggested = {e["programTitle"] for e in contaminated_titles}
        ct_norm = normalize_title_for_comparison(canonical_title_override)
        for rt in resolved_targets:
            pt = rt["programTitle"]
            if pt in already_suggested:
                continue  # auto-suggest already handles this title
            if normalize_title_for_comparison(pt) == ct_norm:
                continue  # already canonical
            contaminated_titles.append({
                "programTitle": pt,
                "suggestedTitle": canonical_title_override,
                "matchSource": "operator_supplied",
                "confidence": "operator",
                "separatorFound": None,
                "affectedFiles": rt["affectedFiles"],
                "pathIds": rt["pathIds"],
                "samplePaths": rt["samplePaths"],
            })
            for pid in rt["pathIds"]:
                update_instructions.append({
                    "path_id": pid,
                    "new_title": canonical_title_override,
                })
        operator_forced = any(
            e["matchSource"] == "operator_supplied" for e in contaminated_titles
        )

    total_affected = sum(e["affectedFiles"] for e in contaminated_titles)

    result: dict[str, Any] = {
        "ok": True,
        "dryRun": args.dry_run,
        "mode": "targeted" if is_targeted else "scan_all",
        "filters": {
            "programTitle": program_title_filter or None,
            "programTitleContains": program_title_contains_filter or None,
            "pathLike": path_like_filter or None,
            "pathIds": path_id_filters if path_id_filters else [],
        },
        "scannedRows": len(rows),
        "totalContaminatedTitles": len(contaminated_titles),
        "totalAffectedFiles": total_affected,
        "contaminatedTitles": contaminated_titles,
        "updateInstructions": update_instructions,
    }
    if is_targeted:
        result["resolvedTargets"] = resolved_targets
    if operator_forced:
        result["operatorForced"] = True
        result["canonicalTitle"] = canonical_title_override

    con.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
