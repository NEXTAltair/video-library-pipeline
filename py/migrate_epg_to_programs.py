"""Migrate legacy path_metadata(source='edcb_epg') into programs/broadcasts.

Usage:
  python migrate_epg_to_programs.py --db <db-path> [--apply] [--delete-source]
"""

from __future__ import annotations

import argparse
import json
import os
import uuid

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from pathscan_common import now_iso


def _normalize_program_key(title: str) -> str:
    return " ".join(str(title or "").strip().lower().split())


def _program_id_for_key(program_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"program:{program_key}"))


def _broadcast_id_for_match_key(match_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"broadcast:{match_key}"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--delete-source", action="store_true", help="Also delete migrated source rows from path_metadata")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    con = connect_db(args.db)
    create_schema_if_needed(con)

    rows = con.execute(
        "SELECT path_id, data_json FROM path_metadata WHERE source='edcb_epg'"
    ).fetchall()

    migrated = 0
    skipped = 0
    invalid = 0
    candidates: list[dict] = []

    for path_id, data_json in rows:
        try:
            data = json.loads(data_json)
        except Exception:
            invalid += 1
            continue
        if not isinstance(data, dict):
            invalid += 1
            continue

        title = str(data.get("official_title") or "").strip()
        match_key = str(data.get("match_key") or "").strip()
        if not title or not match_key:
            skipped += 1
            continue

        program_key = _normalize_program_key(title)
        candidates.append(
            {
                "path_id": path_id,
                "program_id": _program_id_for_key(program_key),
                "program_key": program_key,
                "canonical_title": title,
                "broadcast_id": _broadcast_id_for_match_key(match_key),
                "match_key": match_key,
                "air_date": data.get("air_date"),
                "start_time": data.get("start_time"),
                "end_time": data.get("end_time"),
                "broadcaster": data.get("broadcaster"),
                "data_json": json.dumps(data, ensure_ascii=False),
                "created_at": now_iso(),
            }
        )

    if not args.apply:
        print(json.dumps({
            "ok": True,
            "tool": "migrate_epg_to_programs",
            "apply": False,
            "totalLegacyRows": len(rows),
            "wouldMigrate": len(candidates),
            "skipped": skipped,
            "invalid": invalid,
            "deleteSource": bool(args.delete_source),
        }, ensure_ascii=False))
        con.close()
        return 0

    begin_immediate(con)
    try:
        for row in candidates:
            con.execute(
                """
                INSERT INTO programs (program_id, program_key, canonical_title, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(program_id) DO UPDATE SET canonical_title=excluded.canonical_title
                """,
                (row["program_id"], row["program_key"], row["canonical_title"], row["created_at"]),
            )
            con.execute(
                """
                INSERT INTO broadcasts (broadcast_id, program_id, air_date, start_time, end_time, broadcaster, match_key, data_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(broadcast_id) DO UPDATE SET
                  program_id=excluded.program_id,
                  air_date=excluded.air_date,
                  start_time=excluded.start_time,
                  end_time=excluded.end_time,
                  broadcaster=excluded.broadcaster,
                  match_key=excluded.match_key,
                  data_json=excluded.data_json
                """,
                (
                    row["broadcast_id"], row["program_id"], row["air_date"], row["start_time"], row["end_time"],
                    row["broadcaster"], row["match_key"], row["data_json"], row["created_at"],
                ),
            )
            migrated += 1

        if args.delete_source:
            con.execute("DELETE FROM path_metadata WHERE source='edcb_epg'")

        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    print(json.dumps({
        "ok": True,
        "tool": "migrate_epg_to_programs",
        "apply": True,
        "totalLegacyRows": len(rows),
        "migrated": migrated,
        "skipped": skipped,
        "invalid": invalid,
        "deleteSource": bool(args.delete_source),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
