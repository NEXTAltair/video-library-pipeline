#!/usr/bin/env python3
r"""Create a Windows move plan JSONL from a Windows inventory JSONL."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path, PureWindowsPath

from path_placement_rules import build_expected_dest_path, has_required_db_contract


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
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db or configure plugin db")
    if not args.source_root:
        raise SystemExit("sourceRoot is required: pass --source-root")
    if not args.dest_root:
        raise SystemExit("destRoot is required: pass --dest-root")

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

    with out.open("w", encoding="utf-8") as w:
        w.write(json.dumps({"_meta": {"kind": "move_plan_from_inventory", "run_id": run_id, "inventory": args.inventory, "dest_root": args.dest_root}}, ensure_ascii=False) + "\n")

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
                filename = PureWindowsPath(src).name
                dst, dst_err = build_expected_dest_path(args.dest_root, src, md)
                if not dst or dst_err:
                    skipped_missing_fields += 1
                    continue
                w.write(json.dumps({"path_id": pid, "src": src, "dst": dst, "program_title": prog, "air_date": air}, ensure_ascii=False) + "\n")
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
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
