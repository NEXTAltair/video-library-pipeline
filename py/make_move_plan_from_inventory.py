#!/usr/bin/env python3
r"""Create a Windows move plan JSONL from a Windows inventory JSONL."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
from pathlib import Path, PureWindowsPath

FORB = re.compile(r'[<>:"/\\|?*]')
CTRL = re.compile(r"[\x00-\x1f]")
TRAIL = re.compile(r"[\. ]+$")
WS = re.compile(r"[\s\u3000]+")


def safe_dir_name(name: str, maxlen: int = 60) -> str:
    s = (name or "").strip()
    s = CTRL.sub("", s)
    s = FORB.sub("＿", s)
    s = WS.sub(" ", s)
    s = TRAIL.sub("", s)
    if not s:
        s = "UNKNOWN"
    if len(s) > maxlen:
        h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]
        s = s[: maxlen - 9].rstrip() + "_" + h
    return s


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
    ap.add_argument("--db", default="/mnt/b/_AI_WORK/db/mediaops.sqlite")
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--source-root", default=r"B:\\未視聴")
    ap.add_argument("--dest-root", default=r"B:\\VideoLibrary\\by_program")
    ap.add_argument("--out", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--allow-needs-review", action="store_true")
    args = ap.parse_args()

    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(args.out) if args.out else Path("/mnt/b/_AI_WORK/move") / f"move_plan_from_inventory_{run_id}.jsonl"
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
                if md.get("needs_review") and not args.allow_needs_review:
                    skipped_needs_review += 1
                    continue
                air = md.get("air_date")
                prog = md.get("program_title")
                if not air or not prog:
                    skipped_missing_fields += 1
                    continue
                try:
                    y, m, _ = air.split("-", 2)
                except Exception:
                    skipped_missing_fields += 1
                    continue

                prog_dir = safe_dir_name(prog)
                filename = PureWindowsPath(src).name
                dst = args.dest_root.rstrip("\\") + f"\\{prog_dir}\\{y}\\{m}\\{filename}"
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
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
