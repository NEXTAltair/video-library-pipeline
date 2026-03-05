#!/usr/bin/env python3
"""Repair paths.drive/dir/name columns from paths.path (Windows path parsing)."""

from __future__ import annotations

import argparse
import sqlite3

from pathscan_common import split_win


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db")
    con = sqlite3.connect(args.db)
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT path_id, path, name
        FROM paths
        WHERE name LIKE '%\\%'
           OR name LIKE '%:%'
        """
    ).fetchall()

    fixed = 0
    for path_id, path, name in rows:
        drive, dir_, fname, _ext = split_win(path)
        if not fname:
            continue
        if fname == name and (dir_ == (cur.execute("SELECT dir FROM paths WHERE path_id=?", (path_id,)).fetchone() or [None])[0]):
            continue
        if args.apply:
            cur.execute(
                "UPDATE paths SET drive=?, dir=?, name=?, updated_at=CURRENT_TIMESTAMP WHERE path_id=?",
                (drive, dir_, fname, path_id),
            )
        fixed += 1
        if args.limit and fixed >= args.limit:
            break

    if args.apply:
        con.commit()

    print({"candidates": len(rows), "fixed": fixed, "mode": "apply" if args.apply else "dry_run"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
