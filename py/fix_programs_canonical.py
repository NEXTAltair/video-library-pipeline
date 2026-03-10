#!/usr/bin/env python3
"""Fix programs.canonical_title using improved series_name_extractor.

Re-runs the programs rebuild logic from migrate_schema_v3 with:
- Episode suffix stripping (第N話, #N) for alias lookup
- extract_series_name() for canonical_title instead of shortest old title
- Post-split alias lookup for fallback results

Usage:
  python fix_programs_canonical.py --db mediaops.sqlite --aliases rules/program_aliases.yaml --dry-run
  python fix_programs_canonical.py --db mediaops.sqlite --aliases rules/program_aliases.yaml
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict

from epg_common import normalize_program_key, program_id_for
from mediaops_schema import begin_immediate
from pathscan_common import now_iso
from series_name_extractor import _load_aliases, extract_series_name, series_program_key


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--aliases", default="")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    alias_map = _load_aliases(args.aliases or None)

    old_programs = con.execute(
        "SELECT program_id, program_key, canonical_title FROM programs"
    ).fetchall()

    # Group by new series key
    series_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in old_programs:
        old_pid = row["program_id"]
        old_title = row["canonical_title"]
        new_key = series_program_key(old_title, _alias_map=alias_map)
        series_groups[new_key].append({
            "old_program_id": old_pid,
            "old_title": old_title,
        })

    id_mapping: dict[str, str] = {}
    new_programs: dict[str, dict[str, str]] = {}
    ts = now_iso()

    title_changes = []
    merges = []

    for new_key, members in series_groups.items():
        new_pid = program_id_for(new_key)
        canonical = extract_series_name(members[0]["old_title"], _alias_map=alias_map)
        new_programs[new_key] = {"program_id": new_pid, "canonical_title": canonical}

        for m in members:
            id_mapping[m["old_program_id"]] = new_pid
            if m["old_title"] != canonical:
                title_changes.append((m["old_title"], canonical))

        if len(members) > 1:
            merges.append({
                "new_key": new_key,
                "canonical": canonical,
                "merged_from": [m["old_title"] for m in members],
            })

    # Count programs that will change canonical_title
    canonical_updates = []
    for row in old_programs:
        old_pid = row["program_id"]
        new_pid = id_mapping[old_pid]
        if old_pid == new_pid:
            # Same program_id, just update canonical_title
            new_info = [v for k, v in new_programs.items() if v["program_id"] == new_pid]
            if new_info and new_info[0]["canonical_title"] != row["canonical_title"]:
                canonical_updates.append(
                    (row["canonical_title"], new_info[0]["canonical_title"])
                )

    result = {
        "old_programs": len(old_programs),
        "new_programs": len(new_programs),
        "id_remappings": sum(1 for o, n in id_mapping.items() if o != n),
        "canonical_title_changes": len(title_changes),
        "program_merges": len(merges),
        "dry_run": args.dry_run,
    }

    if args.dry_run:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if title_changes:
            print("\n=== Canonical title changes ===")
            for old, new in sorted(title_changes, key=lambda x: x[1]):
                print(f"  {old}")
                print(f"    -> {new}")
        if merges:
            print("\n=== Program merges ===")
            for m in merges:
                print(f"  {m['canonical']} (key={m['new_key']})")
                for f in m["merged_from"]:
                    print(f"    <- {f}")
        return 0

    # Disable FK constraints for the duration of the rebuild
    con.execute("PRAGMA foreign_keys = OFF")
    begin_immediate(con)

    # Step 1: Delete safely removable programs
    old_pids_to_remove = {old for old, new in id_mapping.items() if old != new}
    target_pids = set(id_mapping.values())
    safe_to_remove = old_pids_to_remove - target_pids
    for old_pid in safe_to_remove:
        con.execute("DELETE FROM programs WHERE program_id=?", (old_pid,))

    # Step 2: Insert/update program rows
    for new_key, info in new_programs.items():
        con.execute(
            """
            INSERT INTO programs (program_id, program_key, canonical_title, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(program_id) DO UPDATE SET
              program_key=excluded.program_key,
              canonical_title=excluded.canonical_title
            """,
            (info["program_id"], new_key, info["canonical_title"], ts),
        )

    # Step 3: Update broadcasts.program_id
    for old_pid, new_pid in id_mapping.items():
        if old_pid != new_pid:
            con.execute(
                "UPDATE broadcasts SET program_id=? WHERE program_id=?",
                (new_pid, old_pid),
            )

    # Step 4: Update path_programs.program_id with PK conflict handling
    for old_pid, new_pid in id_mapping.items():
        if old_pid != new_pid:
            conflict_rows = con.execute(
                """
                SELECT pp1.path_id, pp1.broadcast_id AS old_bid, pp2.broadcast_id AS existing_bid
                FROM path_programs pp1
                JOIN path_programs pp2 ON pp2.path_id = pp1.path_id AND pp2.program_id = ?
                WHERE pp1.program_id = ?
                """,
                (new_pid, old_pid),
            ).fetchall()

            for cr in conflict_rows:
                if cr["old_bid"] and not cr["existing_bid"]:
                    con.execute(
                        "UPDATE path_programs SET broadcast_id=?, updated_at=? WHERE path_id=? AND program_id=?",
                        (cr["old_bid"], ts, cr["path_id"], new_pid),
                    )
                con.execute(
                    "DELETE FROM path_programs WHERE path_id=? AND program_id=?",
                    (cr["path_id"], old_pid),
                )

            con.execute(
                "UPDATE path_programs SET program_id=? WHERE program_id=?",
                (new_pid, old_pid),
            )

    # Step 5: Clean up orphan programs (no FK references)
    orphans = con.execute(
        """
        SELECT p.program_id FROM programs p
        WHERE NOT EXISTS (SELECT 1 FROM broadcasts b WHERE b.program_id = p.program_id)
          AND NOT EXISTS (SELECT 1 FROM path_programs pp WHERE pp.program_id = p.program_id)
        """
    ).fetchall()
    for o in orphans:
        con.execute("DELETE FROM programs WHERE program_id=?", (o["program_id"],))
    result["orphans_cleaned"] = len(orphans)

    con.commit()
    con.execute("PRAGMA foreign_keys = ON")
    con.close()

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
