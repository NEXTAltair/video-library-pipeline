#!/usr/bin/env python3
"""Bulk-fix program_title in path_metadata where full broadcast title was stored.

Strategy:
1. extract_series_name() (alias map + episode stripping + fallback split)
2. If still long, try matching against known correct short titles from DB
3. Update program_title and normalized_program_key

Usage:
  python fix_program_titles_bulk.py --db mediaops.sqlite --aliases rules/program_aliases.yaml --dry-run
  python fix_program_titles_bulk.py --db mediaops.sqlite --aliases rules/program_aliases.yaml
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter

from epg_common import normalize_program_key
from mediaops_schema import begin_immediate, connect_db
from series_name_extractor import _load_aliases, extract_series_name

MAX_GOOD_TITLE_LEN = 25


def _build_known_titles(con: sqlite3.Connection) -> list[str]:
    """Collect known correct short program titles from DB, sorted by length DESC."""
    rows = con.execute(
        """
        SELECT program_title, COUNT(*) as cnt
        FROM path_metadata
        WHERE program_title IS NOT NULL AND program_title != ''
          AND length(program_title) <= ?
        GROUP BY program_title
        HAVING cnt >= 2
        """,
        (MAX_GOOD_TITLE_LEN,),
    ).fetchall()
    titles = set()
    for r in rows:
        t = r["program_title"]
        # Skip titles that look like partial content (end with bracket, etc.)
        if t and not t[-1] in ("【", "「", "『", "▽", "▼", "◇", "#", "＃", "、", "。"):
            titles.add(t)
    return sorted(titles, key=len, reverse=True)


def _fix_title(
    program_title: str,
    alias_map: dict[str, str],
    known_titles: list[str],
) -> str | None:
    """Return corrected title, or None if no fix needed."""
    if not program_title or len(program_title) <= MAX_GOOD_TITLE_LEN:
        return None

    # Step 1: extract_series_name
    extracted = extract_series_name(program_title, _alias_map=alias_map)
    if extracted != program_title and len(extracted) < len(program_title):
        return extracted

    # Step 2: startswith known short title
    for st in known_titles:
        if program_title.startswith(st) and len(program_title) > len(st) + 2:
            return st

    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--aliases", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-reviewed", action="store_true",
                    help="Only fix human_reviewed=1 records")
    args = ap.parse_args()

    con = connect_db(args.db)
    alias_map = _load_aliases(args.aliases or None)
    known_titles = _build_known_titles(con)

    where = "WHERE length(pm.program_title) > ?"
    params: list = [MAX_GOOD_TITLE_LEN]
    if args.only_reviewed:
        where += " AND pm.human_reviewed = 1"

    rows = con.execute(
        f"""
        SELECT pm.path_id, pm.program_title, pm.normalized_program_key
        FROM path_metadata pm
        {where}
        """,
        params,
    ).fetchall()

    updates: list[tuple[str, str, str]] = []  # (new_title, new_key, path_id)
    fix_counts: Counter[str] = Counter()

    for r in rows:
        pt = r["program_title"]
        new_pt = _fix_title(pt, alias_map, known_titles)
        if new_pt and new_pt != pt:
            new_key = normalize_program_key(new_pt)
            updates.append((new_pt, new_key, r["path_id"]))
            fix_counts[new_pt] += 1

    result = {
        "total_checked": len(rows),
        "will_fix": len(updates),
        "distinct_new_titles": len(fix_counts),
        "dry_run": args.dry_run,
    }

    if args.dry_run:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("\n=== Fixes by new title (top 30) ===")
        for title, cnt in fix_counts.most_common(30):
            print(f"  [{cnt:4d}] {title}")
        return 0

    begin_immediate(con)
    con.executemany(
        """
        UPDATE path_metadata
        SET program_title = ?, normalized_program_key = ?
        WHERE path_id = ?
        """,
        updates,
    )
    con.commit()
    con.close()

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
