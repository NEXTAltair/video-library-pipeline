"""Migrate legacy EPG rows from path_metadata(source='edcb_epg') to programs/broadcasts.

This script is idempotent. It does not run automatically.
"""

from __future__ import annotations

import argparse
import json

from epg_common import broadcast_id_for, normalize_program_key, program_id_for
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from pathscan_common import now_iso


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--delete-legacy", action="store_true", help="Delete migrated source='edcb_epg' rows from path_metadata")
    args = ap.parse_args()

    con = connect_db(args.db)
    create_schema_if_needed(con)

    rows = con.execute(
        """
        SELECT path_id, data_json
        FROM path_metadata
        WHERE source='edcb_epg'
        """
    ).fetchall()

    migrated = 0
    skipped = 0
    deleted = 0

    try:
        if args.apply:
            begin_immediate(con)

        for path_id, data_json in rows:
            try:
                data = json.loads(data_json)
            except Exception:
                skipped += 1
                continue
            if not isinstance(data, dict):
                skipped += 1
                continue

            title = str(data.get("official_title") or data.get("program_title") or "").strip()
            if not title:
                skipped += 1
                continue

            match_key = str(data.get("match_key") or "").strip()
            program_key = normalize_program_key(title)
            program_id = program_id_for(program_key)
            b_seed = f"{program_id}::{data.get('air_date') or ''}::{data.get('start_time') or ''}::{data.get('broadcaster') or ''}"
            broadcast_id = broadcast_id_for(match_key, b_seed)
            ts = now_iso()

            if args.apply:
                con.execute(
                    """
                    INSERT INTO programs (program_id, program_key, canonical_title, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(program_id) DO UPDATE SET
                      canonical_title=excluded.canonical_title
                    """,
                    (program_id, program_key, title, ts),
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
                        broadcast_id,
                        program_id,
                        data.get("air_date"),
                        data.get("start_time"),
                        data.get("end_time"),
                        data.get("broadcaster"),
                        match_key or None,
                        json.dumps(data, ensure_ascii=False),
                        ts,
                    ),
                )
            migrated += 1

        if args.apply and args.delete_legacy:
            deleted = con.execute("DELETE FROM path_metadata WHERE source='edcb_epg'").rowcount

        if args.apply:
            con.commit()
    except Exception:
        if args.apply:
            con.rollback()
        raise
    finally:
        con.close()

    print(json.dumps({
        "ok": True,
        "tool": "migrate_epg_to_programs",
        "apply": bool(args.apply),
        "deleteLegacy": bool(args.delete_legacy),
        "legacyRows": len(rows),
        "migrated": migrated,
        "skipped": skipped,
        "deletedLegacy": deleted,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
