#!/usr/bin/env python3
"""Create a metadata extraction queue from an inventory JSONL (real filesystem snapshot)."""

from __future__ import annotations

import argparse
import json
from pathlib import PureWindowsPath

from db_helpers import latest_path_metadata
from mediaops_schema import connect_db
from pathscan_common import now_iso


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--source-root", default="")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db or configure plugin db")
    if not args.source_root:
        raise SystemExit("sourceRoot is required: pass --source-root")
    con = connect_db(args.db)
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

                row = con.execute("SELECT path_id FROM paths WHERE path=?", (path,)).fetchone()
                if not row:
                    continue
                pid = row["path_id"]

                md, _ = latest_path_metadata(con, pid)
                if md is None:
                    pick = True
                else:
                    pick = (
                        not md.get("program_title")
                        or md.get("air_date") is None
                        or bool(md.get("needs_review"))
                    )

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
