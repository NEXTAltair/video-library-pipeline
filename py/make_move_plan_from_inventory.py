#!/usr/bin/env python3
r"""Create a Windows move plan JSONL from a Windows inventory JSONL."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path, PureWindowsPath

from path_placement_rules import (
    build_expected_dest_path,
    build_routed_dest_path,
    has_required_db_contract,
    load_drive_routes,
)


def latest_llm_metadata(con: sqlite3.Connection, path_id: str) -> dict | None:
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT data_json
        FROM path_metadata
        WHERE path_id=? AND source='llm'
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (path_id,),
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None
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

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    total = 0
    planned = 0
    skipped_no_path = 0
    skipped_no_md = 0
    skipped_needs_review = 0
    skipped_missing_fields = 0
    skipped_outside = 0
    skipped_invalid_contract = 0
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
                md = latest_llm_metadata(con, pid)
                if not md:
                    skipped_no_md += 1
                    continue
                if not has_required_db_contract(md):
                    skipped_invalid_contract += 1
                    continue
                if md.get("needs_review") and not args.allow_needs_review:
                    skipped_needs_review += 1
                    continue
                air = md.get("air_date")
                prog = md.get("program_title")
                if not air or not prog:
                    skipped_missing_fields += 1
                    continue

                # Build destination path: use drive routes if available, else single dest
                genre_route = None
                if routes:
                    dst, genre_route, dst_err = build_routed_dest_path(routes, src, md)
                else:
                    dst, dst_err = build_expected_dest_path(args.dest_root, src, md)

                if not dst or dst_err:
                    skipped_missing_fields += 1
                    continue

                plan_row: dict = {
                    "path_id": pid,
                    "src": src,
                    "dst": dst,
                    "program_title": prog,
                    "air_date": air,
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
                "genre_route_counts": genre_counts if genre_counts else None,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
