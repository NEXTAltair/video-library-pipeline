#!/usr/bin/env python3
"""Migrate mediaops.sqlite from v2 to v3 schema.

Steps:
1. Backup DB
2. ALTER TABLE ADD COLUMN for new columns
3. path_metadata: promote fields from data_json to columns, remove from data_json
4. broadcasts: promote fields from data_json to columns, remove from data_json
5. Create franchises table and populate from franchise_rules.yaml
6. Reconstruct programs as series-level (grouping broadcast-level programs)
7. Set broadcast_group_members.broadcast_id from path_programs
8. Create indexes, insert schema_version
9. PRAGMA integrity_check + foreign_key_check

Usage:
  python migrate_schema_v3.py --db mediaops.sqlite --dry-run
  python migrate_schema_v3.py --db mediaops.sqlite
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from db_helpers import PROMOTED_BROADCAST_KEYS, PROMOTED_PATH_METADATA_KEYS
from epg_common import normalize_program_key, program_id_for
from mediaops_schema import _add_columns_if_missing, _table_exists, register_custom_functions
from pathscan_common import now_iso

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

_PATH_METADATA_NEW_COLUMNS = [
    ("program_title", "TEXT", ""),
    ("air_date", "TEXT", ""),
    ("needs_review", "INTEGER NOT NULL DEFAULT 0", ""),
    ("normalized_program_key", "TEXT", ""),
    ("episode_no", "TEXT", ""),
    ("subtitle", "TEXT", ""),
    ("broadcaster", "TEXT", ""),
    ("human_reviewed", "INTEGER NOT NULL DEFAULT 0", ""),
]

_BROADCASTS_NEW_COLUMNS = [
    ("is_rebroadcast_flag", "INTEGER", ""),
    ("epg_genres", "TEXT", ""),
    ("description", "TEXT", ""),
    ("official_title", "TEXT", ""),
    ("annotations", "TEXT", ""),
]

_PROGRAMS_NEW_COLUMNS = [
    ("franchise_id", "TEXT", ""),
]

_BGM_NEW_COLUMNS = [
    ("broadcast_id", "TEXT", ""),
]


def _check_sqlite_version() -> None:
    ver = sqlite3.sqlite_version_info
    if ver < (3, 38, 0):
        raise SystemExit(
            f"SQLite >= 3.38 required for json_remove. Current: {sqlite3.sqlite_version}"
        )


def _backup_db(db_path: str) -> str:
    backup_path = db_path + f".backup_v3_{now_iso().replace(':', '').replace('-', '')}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _promote_path_metadata(con: sqlite3.Connection, dry_run: bool) -> int:
    """Copy promoted fields from data_json to columns, then remove from data_json."""
    rows = con.execute("SELECT path_id, data_json FROM path_metadata").fetchall()
    count = 0
    for r in rows:
        path_id = r["path_id"]
        raw = r["data_json"]
        if not raw:
            continue
        try:
            data = json.loads(str(raw))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        # Extract promoted values
        program_title = data.get("program_title")
        air_date = data.get("air_date")
        needs_review = 1 if data.get("needs_review") else 0
        _unused_npk = data.get("normalized_program_key")  # legacy, no longer written
        episode_no = data.get("episode_no")
        if episode_no is not None:
            episode_no = str(episode_no)
        subtitle = data.get("subtitle")
        broadcaster = data.get("broadcaster")
        human_reviewed = 1 if data.get("human_reviewed") else 0

        # Remove promoted keys from data_json
        residual = {k: v for k, v in data.items() if k not in PROMOTED_PATH_METADATA_KEYS}
        new_data_json = json.dumps(residual, ensure_ascii=False)

        if not dry_run:
            con.execute(
                """
                UPDATE path_metadata SET
                  program_title=?, air_date=?, needs_review=?,
                  episode_no=?, subtitle=?,
                  broadcaster=?, human_reviewed=?,
                  data_json=?
                WHERE path_id=?
                """,
                (
                    program_title, air_date, needs_review,
                    episode_no, subtitle,
                    broadcaster, human_reviewed,
                    new_data_json, path_id,
                ),
            )
        count += 1
    return count


def _promote_broadcasts(con: sqlite3.Connection, dry_run: bool) -> int:
    """Copy promoted fields from data_json to columns, then remove from data_json."""
    rows = con.execute("SELECT broadcast_id, data_json FROM broadcasts").fetchall()
    count = 0
    for r in rows:
        bid = r["broadcast_id"]
        raw = r["data_json"]
        if not raw:
            continue
        try:
            data = json.loads(str(raw))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        is_rebroadcast_flag = data.get("is_rebroadcast_flag")
        if isinstance(is_rebroadcast_flag, bool):
            is_rebroadcast_flag = 1 if is_rebroadcast_flag else 0
        elif is_rebroadcast_flag is not None:
            is_rebroadcast_flag = 1 if is_rebroadcast_flag else 0

        epg_genres = data.get("epg_genres")
        if epg_genres is not None and not isinstance(epg_genres, str):
            epg_genres = json.dumps(epg_genres, ensure_ascii=False)

        description = data.get("description")
        if isinstance(description, str) and len(description) > 500:
            description = description[:500]

        official_title = data.get("official_title")

        annotations = data.get("annotations")
        if annotations is not None and not isinstance(annotations, str):
            annotations = json.dumps(annotations, ensure_ascii=False)

        # Remove promoted keys from data_json
        residual = {k: v for k, v in data.items() if k not in PROMOTED_BROADCAST_KEYS}
        new_data_json = json.dumps(residual, ensure_ascii=False)

        if not dry_run:
            con.execute(
                """
                UPDATE broadcasts SET
                  is_rebroadcast_flag=?, epg_genres=?, description=?,
                  official_title=?, annotations=?,
                  data_json=?
                WHERE broadcast_id=?
                """,
                (
                    is_rebroadcast_flag, epg_genres, description,
                    official_title, annotations,
                    new_data_json, bid,
                ),
            )
        count += 1
    return count


def _populate_franchises(con: sqlite3.Connection, rules_path: str | None, dry_run: bool) -> int:
    """Create franchises table entries from franchise_rules.yaml."""
    if not rules_path or yaml is None:
        return 0
    p = Path(rules_path)
    if not p.exists():
        return 0
    try:
        with p.open("r", encoding="utf-8-sig") as f:
            obj = yaml.safe_load(f)
    except Exception:
        return 0
    if not isinstance(obj, dict) or not isinstance(obj.get("rules"), list):
        return 0

    count = 0
    ts = now_iso()
    for rule in obj["rules"]:
        if not isinstance(rule, dict):
            continue
        franchise_name = rule.get("franchise")
        if not isinstance(franchise_name, str) or not franchise_name.strip():
            continue
        franchise_name = franchise_name.strip()
        franchise_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"franchise:{franchise_name}"))
        if not dry_run:
            con.execute(
                """
                INSERT INTO franchises (franchise_id, franchise_name, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(franchise_id) DO NOTHING
                """,
                (franchise_id, franchise_name, ts),
            )
        count += 1
    return count


def _rebuild_programs_series(con: sqlite3.Connection, aliases_path: str | None, dry_run: bool) -> dict[str, Any]:
    """Rebuild programs table to be series-level instead of per-broadcast-title.

    Returns stats dict.
    """
    from series_name_extractor import _load_aliases, extract_series_name, series_program_key

    alias_map = _load_aliases(aliases_path)

    # Read all existing programs
    old_programs = con.execute("SELECT program_id, program_key, canonical_title FROM programs").fetchall()
    if not old_programs:
        return {"old_programs": 0, "new_programs": 0, "id_mappings": 0}

    # Group old programs by new series key
    series_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in old_programs:
        old_pid = row["program_id"]
        old_title = row["canonical_title"]
        new_key = series_program_key(old_title, _alias_map=alias_map)
        series_groups[new_key].append({
            "old_program_id": old_pid,
            "old_title": old_title,
        })

    # Build old→new program_id mapping
    id_mapping: dict[str, str] = {}  # old_program_id → new_program_id
    new_programs: dict[str, dict[str, str]] = {}  # new_key → {program_id, canonical_title}
    ts = now_iso()

    for new_key, members in series_groups.items():
        new_pid = program_id_for(new_key)
        # Use extract_series_name to derive canonical_title from alias map,
        # falling back to the shortest old title if no alias match.
        canonical = extract_series_name(members[0]["old_title"], _alias_map=alias_map)

        new_programs[new_key] = {"program_id": new_pid, "canonical_title": canonical}
        for m in members:
            id_mapping[m["old_program_id"]] = new_pid

    if dry_run:
        return {
            "old_programs": len(old_programs),
            "new_programs": len(new_programs),
            "id_mappings": len(id_mapping),
        }

    # Step 1: Insert new program rows (may overlap with existing ones)
    # First, delete program rows that will be replaced by new series-level rows.
    # We only delete rows whose program_id is being remapped to a different ID.
    old_pids_to_remove = {old for old, new in id_mapping.items() if old != new}
    # But don't delete if the old_pid is also a target (used as new_pid by some mapping)
    target_pids = set(id_mapping.values())
    safe_to_remove = old_pids_to_remove - target_pids
    for old_pid in safe_to_remove:
        con.execute("DELETE FROM programs WHERE program_id=?", (old_pid,))

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

    # Step 2: Update broadcasts.program_id to point to new programs
    for old_pid, new_pid in id_mapping.items():
        if old_pid != new_pid:
            con.execute(
                "UPDATE broadcasts SET program_id=? WHERE program_id=?",
                (new_pid, old_pid),
            )

    # Step 3: Update path_programs.program_id with PK conflict handling
    for old_pid, new_pid in id_mapping.items():
        if old_pid != new_pid:
            # Check for PK conflicts: (path_id, new_program_id) may already exist
            conflict_rows = con.execute(
                """
                SELECT pp1.path_id, pp1.broadcast_id AS old_bid, pp2.broadcast_id AS existing_bid
                FROM path_programs pp1
                JOIN path_programs pp2 ON pp2.path_id = pp1.path_id AND pp2.program_id = ?
                WHERE pp1.program_id = ?
                """,
                (new_pid, old_pid),
            ).fetchall()

            # For conflicts: keep the one with broadcast_id, delete the other
            for cr in conflict_rows:
                if cr["old_bid"] and not cr["existing_bid"]:
                    # old row has broadcast_id, existing doesn't -> update existing
                    con.execute(
                        "UPDATE path_programs SET broadcast_id=?, updated_at=? WHERE path_id=? AND program_id=?",
                        (cr["old_bid"], ts, cr["path_id"], new_pid),
                    )
                # Delete the conflicting old row
                con.execute(
                    "DELETE FROM path_programs WHERE path_id=? AND program_id=?",
                    (cr["path_id"], old_pid),
                )

            # Update remaining non-conflicting rows
            con.execute(
                "UPDATE path_programs SET program_id=? WHERE program_id=?",
                (new_pid, old_pid),
            )

    return {
        "old_programs": len(old_programs),
        "new_programs": len(new_programs),
        "id_mappings": len(id_mapping),
    }


def _set_bgm_broadcast_ids(con: sqlite3.Connection, dry_run: bool) -> int:
    """Set broadcast_group_members.broadcast_id from path_programs reverse lookup."""
    rows = con.execute(
        """
        SELECT bgm.rowid, bgm.path_id, pp.broadcast_id
        FROM broadcast_group_members bgm
        LEFT JOIN path_programs pp ON pp.path_id = bgm.path_id
        WHERE bgm.broadcast_id IS NULL AND pp.broadcast_id IS NOT NULL
        """
    ).fetchall()
    if dry_run:
        return len(rows)
    for r in rows:
        con.execute(
            "UPDATE broadcast_group_members SET broadcast_id=? WHERE rowid=?",
            (r["broadcast_id"], r["rowid"]),
        )
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--franchise-rules", default="")
    ap.add_argument("--aliases", default="")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    _check_sqlite_version()

    # Check if already at v3
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    if _table_exists(con, "schema_version"):
        row = con.execute("SELECT MAX(version) FROM schema_version").fetchone()
        if row and row[0] is not None and int(row[0]) >= 3:
            print(json.dumps({"ok": True, "message": "Already at schema v3", "dryRun": args.dry_run}))
            con.close()
            return 0
    con.close()

    # Auto-detect rules paths
    rules_dir = Path(__file__).resolve().parent.parent / "rules"
    franchise_rules = args.franchise_rules or str(rules_dir / "franchise_rules.yaml")
    aliases_path = args.aliases or str(rules_dir / "program_aliases.yaml")

    # Backup
    backup_path = None
    if not args.dry_run:
        backup_path = _backup_db(args.db)
        print(f"Backup: {backup_path}")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    register_custom_functions(con)
    # FK constraints OFF during migration so program_id changes don't fail
    con.execute("PRAGMA foreign_keys = OFF")

    stats: dict[str, Any] = {"dryRun": args.dry_run}

    try:
        if not args.dry_run:
            con.execute("BEGIN IMMEDIATE")

        # Step 2: ALTER TABLE ADD COLUMN
        if _table_exists(con, "path_metadata"):
            _add_columns_if_missing(con, "path_metadata", _PATH_METADATA_NEW_COLUMNS)
        if _table_exists(con, "broadcasts"):
            _add_columns_if_missing(con, "broadcasts", _BROADCASTS_NEW_COLUMNS)
        if _table_exists(con, "programs"):
            _add_columns_if_missing(con, "programs", _PROGRAMS_NEW_COLUMNS)
        if _table_exists(con, "broadcast_group_members"):
            _add_columns_if_missing(con, "broadcast_group_members", _BGM_NEW_COLUMNS)

        # Create franchises + schema_version tables
        if not _table_exists(con, "franchises"):
            con.execute("""
                CREATE TABLE franchises (
                  franchise_id TEXT PRIMARY KEY,
                  franchise_name TEXT NOT NULL UNIQUE,
                  created_at TEXT NOT NULL
                )
            """)
        if not _table_exists(con, "schema_version"):
            con.execute("""
                CREATE TABLE schema_version (
                  version INTEGER NOT NULL,
                  migrated_at TEXT NOT NULL
                )
            """)

        # Step 3: Promote path_metadata fields
        stats["path_metadata_promoted"] = _promote_path_metadata(con, args.dry_run)

        # Step 4: Promote broadcast fields
        stats["broadcasts_promoted"] = _promote_broadcasts(con, args.dry_run)

        # Step 5: Populate franchises
        stats["franchises_populated"] = _populate_franchises(con, franchise_rules, args.dry_run)

        # Step 6: Rebuild programs as series-level
        stats["programs_rebuild"] = _rebuild_programs_series(con, aliases_path, args.dry_run)

        # Step 7: Set broadcast_group_members.broadcast_id
        stats["bgm_broadcast_ids_set"] = _set_bgm_broadcast_ids(con, args.dry_run)

        # Step 8: Create indexes
        v3_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_path_metadata_program_title ON path_metadata(program_title)",
            "CREATE INDEX IF NOT EXISTS idx_path_metadata_npk ON path_metadata(normalized_program_key)",
            "CREATE INDEX IF NOT EXISTS idx_path_metadata_air_date ON path_metadata(air_date)",
            "CREATE INDEX IF NOT EXISTS idx_path_metadata_needs_review ON path_metadata(needs_review)",
            "CREATE INDEX IF NOT EXISTS idx_broadcasts_official_title ON broadcasts(official_title)",
            "CREATE INDEX IF NOT EXISTS idx_programs_franchise ON programs(franchise_id)",
            "CREATE INDEX IF NOT EXISTS idx_bgm_broadcast ON broadcast_group_members(broadcast_id)",
        ]
        for idx in v3_indexes:
            con.execute(idx)

        # Insert schema_version
        if not args.dry_run:
            con.execute(
                "INSERT INTO schema_version (version, migrated_at) VALUES (?, ?)",
                (3, now_iso()),
            )

        if not args.dry_run:
            con.commit()

            # Step 9: Integrity checks
            integrity = con.execute("PRAGMA integrity_check").fetchone()
            fk_check = con.execute("PRAGMA foreign_key_check").fetchall()
            stats["integrity_check"] = str(integrity[0]) if integrity else "unknown"
            stats["foreign_key_violations"] = len(fk_check)
            if fk_check:
                stats["fk_violations_sample"] = [
                    {"table": r[0], "rowid": r[1], "parent": r[2], "fkid": r[3]}
                    for r in fk_check[:10]
                ]
        else:
            con.rollback()

    except Exception:
        if not args.dry_run:
            con.rollback()
        raise
    finally:
        con.close()

    stats["ok"] = True
    if backup_path:
        stats["backup"] = backup_path
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
