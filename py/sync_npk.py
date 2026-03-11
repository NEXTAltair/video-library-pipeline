#!/usr/bin/env python3
"""Sync normalized_program_key with program_title for all mismatched rows.

Run after manual edits (e.g. DBeaver) to path_metadata.program_title.

Usage:
  python sync_npk.py --db mediaops.sqlite --dry-run
  python sync_npk.py --db mediaops.sqlite
"""

from __future__ import annotations

import argparse

from epg_common import normalize_program_key
from mediaops_schema import begin_immediate, connect_db


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = connect_db(args.db)
    rows = con.execute(
        """SELECT path_id, program_title, normalized_program_key
           FROM path_metadata
           WHERE program_title IS NOT NULL AND program_title != ''"""
    ).fetchall()

    updates: list[tuple[str, str]] = []
    for r in rows:
        expected = normalize_program_key(r["program_title"])
        if r["normalized_program_key"] != expected:
            updates.append((expected, r["path_id"]))

    print(f"checked={len(rows)}  mismatched={len(updates)}")

    if not updates or args.dry_run:
        for npk, pid in updates[:20]:
            title = con.execute(
                "SELECT program_title FROM path_metadata WHERE path_id=?", (pid,)
            ).fetchone()["program_title"]
            print(f"  {title}  ->  {npk}")
        if len(updates) > 20:
            print(f"  ... and {len(updates) - 20} more")
        return 0

    begin_immediate(con)
    con.executemany(
        "UPDATE path_metadata SET normalized_program_key=? WHERE path_id=?",
        updates,
    )
    con.commit()
    con.close()
    print(f"updated {len(updates)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
