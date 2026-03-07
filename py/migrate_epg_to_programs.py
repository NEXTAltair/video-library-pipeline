"""Migrate legacy EPG rows from path_metadata(source='edcb_epg') to programs/broadcasts.

Usage:
  python migrate_epg_to_programs.py --db /path/to/mediaops.sqlite [--apply] [--delete-source]
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import unicodedata
import uuid

from edcb_program_parser import datetime_key_from_epg, match_key_from_epg
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed

_WS = re.compile(r"[\s\u3000]+")


def normalize_program_key(title: str) -> str:
    t = unicodedata.normalize("NFKC", str(title or "")).strip().lower()
    return _WS.sub(" ", t)


def program_id_for_key(program_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"program:{program_key}"))


def broadcast_id_for(match_key: str, fallback: str) -> str:
    token = match_key or fallback
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"broadcast:{token}"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--delete-source", action="store_true", help="Delete migrated source rows from path_metadata.")
    args = ap.parse_args()

    con = connect_db(args.db)
    create_schema_if_needed(con)

    rows = con.execute(
        "SELECT path_id, data_json FROM path_metadata WHERE source='edcb_epg'"
    ).fetchall()

    planned = []
    parse_failed = 0
    for row in rows:
        try:
            data = json.loads(row["data_json"])
        except Exception:
            parse_failed += 1
            continue
        if not isinstance(data, dict):
            parse_failed += 1
            continue

        title = str(data.get("official_title") or "").strip()
        if not title:
            parse_failed += 1
            continue

        program_key = normalize_program_key(title)
        program_id = program_id_for_key(program_key)

        mk = str(data.get("match_key") or "").strip() or match_key_from_epg(data) or ""
        dk = str(data.get("datetime_key") or "").strip() or datetime_key_from_epg(data)
        if dk and "datetime_key" not in data:
            data["datetime_key"] = dk
        if mk and "match_key" not in data:
            data["match_key"] = mk

        fallback = "::".join(
            [
                row["path_id"],
                str(data.get("air_date") or ""),
                str(data.get("start_time") or ""),
                str(data.get("broadcaster") or ""),
            ]
        )
        broadcast_id = broadcast_id_for(mk, fallback)

        planned.append(
            {
                "path_id": row["path_id"],
                "program_id": program_id,
                "program_key": program_key,
                "canonical_title": title,
                "broadcast_id": broadcast_id,
                "air_date": data.get("air_date"),
                "start_time": data.get("start_time"),
                "end_time": data.get("end_time"),
                "broadcaster": data.get("broadcaster"),
                "match_key": mk or None,
                "data_json": json.dumps(data, ensure_ascii=False),
            }
        )

    if not args.apply:
        print(
            json.dumps(
                {
                    "ok": True,
                    "tool": "migrate_epg_to_programs",
                    "apply": False,
                    "sourceRows": len(rows),
                    "parseFailed": parse_failed,
                    "wouldMigrate": len(planned),
                    "deleteSource": bool(args.delete_source),
                },
                ensure_ascii=False,
            )
        )
        return 0

    inserted_or_updated = 0
    deleted = 0
    begin_immediate(con)
    for x in planned:
        con.execute(
            """
            INSERT INTO programs (program_id, program_key, canonical_title, created_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(program_key) DO UPDATE SET canonical_title=excluded.canonical_title
            """,
            (x["program_id"], x["program_key"], x["canonical_title"]),
        )
        con.execute(
            """
            INSERT INTO broadcasts (
              broadcast_id, program_id, air_date, start_time, end_time,
              broadcaster, match_key, data_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(match_key) DO UPDATE SET
              program_id=excluded.program_id,
              air_date=excluded.air_date,
              start_time=excluded.start_time,
              end_time=excluded.end_time,
              broadcaster=excluded.broadcaster,
              data_json=excluded.data_json
            """,
            (
                x["broadcast_id"],
                x["program_id"],
                x["air_date"],
                x["start_time"],
                x["end_time"],
                x["broadcaster"],
                x["match_key"],
                x["data_json"],
            ),
        )
        inserted_or_updated += 1

    if args.delete_source:
        deleted = con.execute("DELETE FROM path_metadata WHERE source='edcb_epg'").rowcount

    con.commit()
    print(
        json.dumps(
            {
                "ok": True,
                "tool": "migrate_epg_to_programs",
                "apply": True,
                "sourceRows": len(rows),
                "parseFailed": parse_failed,
                "migrated": inserted_or_updated,
                "deletedSourceRows": deleted,
                "deleteSource": bool(args.delete_source),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
