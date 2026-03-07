"""Migrate legacy EPG records from path_metadata(source='edcb_epg') to programs/broadcasts.

Default is dry-run. Use --apply to write.
Optionally use --delete-legacy to remove old path_metadata rows after migration.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
import uuid
from typing import Any

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from pathscan_common import now_iso

_WS = re.compile(r"[\s\u3000]+")
_BAD = re.compile(r"[<>:\"/\\\\|?*]")


def _program_key_from_title(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(title or "")).lower()
    normalized = _BAD.sub("", normalized)
    normalized = _WS.sub("_", normalized).strip("_")
    return normalized or "unknown"


def _program_id_from_key(program_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"program:{program_key}"))


def _broadcast_id(program_id: str, data: dict[str, Any], match_key: str | None) -> str:
    if match_key:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"broadcast:{match_key}"))
    seed = "::".join(
        [
            "broadcast",
            program_id,
            str(data.get("air_date") or ""),
            str(data.get("start_time") or ""),
            str(data.get("end_time") or ""),
            str(data.get("broadcaster") or ""),
        ]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--delete-legacy", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    con = connect_db(args.db)
    create_schema_if_needed(con)

    rows = con.execute(
        "SELECT path_id, data_json FROM path_metadata WHERE source='edcb_epg'"
    ).fetchall()

    parsed = 0
    parse_failed = 0
    prepared = 0
    programs: dict[str, tuple[str, str]] = {}
    broadcasts: list[dict[str, Any]] = []

    for row in rows:
        try:
            data = json.loads(row[1])
        except Exception:
            parse_failed += 1
            continue
        if not isinstance(data, dict):
            parse_failed += 1
            continue

        parsed += 1
        title = str(data.get("official_title") or "")
        program_key = _program_key_from_title(title)
        program_id = _program_id_from_key(program_key)
        programs[program_id] = (program_key, title or "UNKNOWN")

        match_key = data.get("match_key")
        if not isinstance(match_key, str):
            match_key = None

        broadcasts.append(
            {
                "broadcast_id": _broadcast_id(program_id, data, match_key),
                "program_id": program_id,
                "air_date": data.get("air_date"),
                "start_time": data.get("start_time"),
                "end_time": data.get("end_time"),
                "broadcaster": data.get("broadcaster"),
                "match_key": match_key,
                "data_json": json.dumps(data, ensure_ascii=False),
            }
        )
        prepared += 1

    if args.apply:
        try:
            begin_immediate(con)
            ts_now = now_iso()
            for program_id, (program_key, canonical_title) in programs.items():
                con.execute(
                    """
                    INSERT INTO programs (program_id, program_key, canonical_title, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(program_id) DO UPDATE SET
                      canonical_title=excluded.canonical_title
                    """,
                    (program_id, program_key, canonical_title, ts_now),
                )

            for b in broadcasts:
                con.execute(
                    """
                    INSERT INTO broadcasts (
                      broadcast_id, program_id, air_date, start_time, end_time, broadcaster, match_key, data_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(match_key) DO UPDATE SET
                      program_id=excluded.program_id,
                      air_date=excluded.air_date,
                      start_time=excluded.start_time,
                      end_time=excluded.end_time,
                      broadcaster=excluded.broadcaster,
                      data_json=excluded.data_json
                    """,
                    (
                        b["broadcast_id"],
                        b["program_id"],
                        b["air_date"],
                        b["start_time"],
                        b["end_time"],
                        b["broadcaster"],
                        b["match_key"],
                        b["data_json"],
                        ts_now,
                    ),
                )

            deleted_legacy = 0
            if args.delete_legacy:
                cur = con.execute("DELETE FROM path_metadata WHERE source='edcb_epg'")
                deleted_legacy = int(cur.rowcount or 0)

            con.commit()
        except Exception:
            con.rollback()
            raise
    else:
        deleted_legacy = 0

    summary = {
        "ok": True,
        "tool": "migrate_epg_to_programs",
        "apply": bool(args.apply),
        "deleteLegacy": bool(args.delete_legacy),
        "legacyRows": len(rows),
        "parsed": parsed,
        "parseFailed": parse_failed,
        "programsPrepared": len(programs),
        "broadcastsPrepared": prepared,
        "deletedLegacyRows": deleted_legacy,
    }
    print(json.dumps(summary, ensure_ascii=False))
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
