#!/usr/bin/env python3
"""Repair paths.drive/dir/name columns from paths.path (Windows path parsing)."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import PureWindowsPath


def split_win(p: str) -> tuple[str | None, str | None, str | None]:
    wp = PureWindowsPath(p)
    drive = wp.drive[:-1] if wp.drive.endswith(":") else (wp.drive or None)
    dir_ = str(wp.parent) if str(wp.parent) not in (".", "") else None
    name = wp.name or None
    return drive, dir_, name


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/mnt/b/_AI_WORK/db/mediaops.sqlite")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

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
        drive, dir_, fname = split_win(path)
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
