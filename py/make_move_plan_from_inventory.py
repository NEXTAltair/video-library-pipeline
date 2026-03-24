#!/usr/bin/env python3
r"""Create a Windows move plan JSONL from a Windows inventory JSONL."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path, PureWindowsPath

from mediaops_schema import connect_db
from path_placement_rules import load_drive_routes
from plan_validation import validate_move_candidate
from workflow_shared import resolve_effective_path_metadata
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--source-root", default="")
    ap.add_argument("--dest-root", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--allow-needs-review", action="store_true")
    ap.add_argument("--drive-routes", default="", help="Path to drive_routes.yaml for multi-dest routing")
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db or configure plugin db")
    if not args.source_root:
        raise SystemExit("sourceRoot is required: pass --source-root")
    if not args.dest_root and not args.drive_routes:
        raise SystemExit("destRoot or --drive-routes is required")

    # Load drive routes if provided
    routes = None
    if args.drive_routes and os.path.exists(args.drive_routes):
        routes = load_drive_routes(args.drive_routes)

    run_id = f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{os.getpid()}"
    out = Path(args.out) if args.out else Path(args.inventory).resolve().parent / f"move_plan_from_inventory_{run_id}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)

    con = connect_db(args.db)
    cur = con.cursor()
    total = 0
    planned = 0
    skipped_no_path = 0
    skipped_no_md = 0
    skipped_needs_review = 0
    skipped_missing_fields = 0
    skipped_outside = 0
    skipped_invalid_contract = 0
    skipped_subtitle_separator = 0
    genre_counts: dict[str, int] = {}

    meta_info: dict = {
        "kind": "move_plan_from_inventory",
        "run_id": run_id,
        "inventory": args.inventory,
        "dest_root": args.dest_root or None,
        "drive_routes": args.drive_routes or None,
    }

    with out.open("w", encoding="utf-8") as w:
        w.write(json.dumps({"_meta": meta_info}, ensure_ascii=False) + "\n")

        with open(args.inventory, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if i == 0 and '"_meta"' in line:
                    continue
                o = json.loads(line)
                src = o.get("path") or o.get("full_path")
                if not src:
                    continue
                src = str(src)
                total += 1
                root = args.source_root.rstrip("\\")
                if not (src == root or src.startswith(root + "\\")):
                    skipped_outside += 1
                    continue
                row = cur.execute("SELECT path_id FROM paths WHERE path=?", (src,)).fetchone()
                if not row:
                    skipped_no_path += 1
                    continue
                pid = row[0]
                effective_md = resolve_effective_path_metadata(con, str(pid))
                if not effective_md or not effective_md.metadata:
                    skipped_no_md += 1
                    continue
                md = effective_md.metadata
                md_source = effective_md.source

                result = validate_move_candidate(
                    src, md,
                    allow_needs_review=args.allow_needs_review,
                    routes=routes,
                    dest_root=args.dest_root if not routes else None,
                )
                if not result.ok:
                    reason = result.skip_reason or "unknown"
                    if reason == "needs_review":
                        skipped_needs_review += 1
                    elif reason == "invalid_metadata_contract":
                        skipped_invalid_contract += 1
                    elif reason in ("subtitle_separator_in_program_title",
                                    "suspicious_program_title",
                                    "suspicious_program_title_shortened"):
                        skipped_subtitle_separator += 1
                    else:
                        skipped_missing_fields += 1
                    continue

                dst = result.dst
                genre_route = result.genre_route
                prog = md.get("program_title")
                air = md.get("air_date")

                plan_row: dict = {
                    "path_id": pid,
                    "src": src,
                    "dst": dst,
                    "program_title": prog,
                    "air_date": air,
                    "metadata_source": md_source,
                    "metadata_selected_from": effective_md.selected_from,
                }
                if genre_route:
                    plan_row["genre_route"] = genre_route
                    # Extract dest drive letter
                    if len(dst) >= 2 and dst[1] == ":":
                        plan_row["dest_drive"] = dst[0].upper()
                    genre_counts[genre_route] = genre_counts.get(genre_route, 0) + 1

                w.write(json.dumps(plan_row, ensure_ascii=False) + "\n")
                planned += 1
                if args.limit and planned >= args.limit:
                    break

    print(
        json.dumps(
            {
                "out": str(out),
                "inventory": args.inventory,
                "total": total,
                "planned": planned,
                "skipped_no_path": skipped_no_path,
                "skipped_no_md": skipped_no_md,
                "skipped_needs_review": skipped_needs_review,
                "skipped_missing_fields": skipped_missing_fields,
                "skipped_outside": skipped_outside,
                "skipped_invalid_contract": skipped_invalid_contract,
                "skipped_subtitle_separator": skipped_subtitle_separator,
                "genre_route_counts": genre_counts if genre_counts else None,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
