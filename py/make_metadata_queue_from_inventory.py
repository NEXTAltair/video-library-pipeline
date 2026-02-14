#!/usr/bin/env python3
"""Create a metadata extraction queue from an inventory JSONL (real filesystem snapshot)."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import PureWindowsPath

DB_DEFAULT = "/mnt/b/_AI_WORK/db/mediaops.sqlite"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def latest_llm_row(cur: sqlite3.Cursor, path_id: str):
    return cur.execute(
        """
        SELECT data_json
        FROM path_metadata
        WHERE path_id=? AND source='llm'
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (path_id,),
    ).fetchone()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--source-root", default=r"B:\\未視聴")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    root = args.source_root.rstrip("\\")
    picked = 0

    with open(args.out, "w", encoding="utf-8") as w:
        w.write(
            json.dumps(
                {
                    "_meta": {
                        "kind": "metadata_queue",
                        "generated_at": now_iso(),
                        "mode": "from_inventory_missing_or_needs_review",
                        "inventory": args.inventory,
                        "limit": args.limit,
                    }
                },
                ensure_ascii=False,
            )
            + "\n"
        )

        with open(args.inventory, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if i == 0 and '"_meta"' in line:
                    continue
                o = json.loads(line)
                path = o.get("path") or o.get("full_path")
                name = o.get("name")
                mtime = o.get("mtimeUtc") or o.get("mtime_utc")
                if not path:
                    continue
                path = str(path)
                if not (path == root or path.startswith(root + "\\")):
                    continue
                if PureWindowsPath(path).suffix.lower() != ".mp4":
                    continue

                row = cur.execute("SELECT path_id FROM paths WHERE path=?", (path,)).fetchone()
                if not row:
                    continue
                pid = row[0]

                md_row = latest_llm_row(cur, pid)
                if md_row is None:
                    pick = True
                else:
                    try:
                        md = json.loads(md_row[0])
                    except Exception:
                        md = {}
                    pick = bool(md.get("needs_review")) or (md.get("air_date") is None) or (not md.get("program_title"))

                if not pick:
                    continue
                if not name:
                    name = PureWindowsPath(path).name

                w.write(json.dumps({"path_id": pid, "path": path, "name": name, "mtime_utc": mtime}, ensure_ascii=False) + "\n")
                picked += 1
                if args.limit and picked >= args.limit:
                    break

    print(f"OK queue_rows={picked}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
