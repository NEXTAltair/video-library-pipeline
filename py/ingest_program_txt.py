"""Ingest EDCB .program.txt files into mediaops.sqlite.

Scans a TS recording directory for .program.txt companion files,
parses them, and stores structured EPG metadata in the database.

The metadata is stored in `path_metadata` with source='edcb_epg'.
Match keys are stored in data_json so that encoded files (MP4) can
be correlated with the original EPG data later.

Usage:
  cd <video-library-pipeline-dir>/py
  python ingest_program_txt.py --db <db-path> --ts-root <ts-root> [--apply]
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path
from typing import Any

from edcb_program_parser import (
    datetime_key_from_epg,
    match_key_from_epg,
    match_key_from_filename,
    parse_program_txt,
)
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchone
from pathscan_common import now_iso, path_id_for, split_win, wsl_to_windows_path
from source_history import make_entry


def find_program_txt_files(ts_root: Path) -> list[Path]:
    """Find all .program.txt files recursively under ts_root."""
    return sorted(ts_root.rglob("*.program.txt"))


def ts_path_from_program_txt(program_txt_path: Path) -> Path | None:
    """Derive the .ts file path from its .program.txt companion.

    EDCB naming: "title_date.ts.program.txt" → "title_date.ts"
    """
    name = program_txt_path.name
    if name.endswith(".ts.program.txt"):
        ts_name = name[: -len(".program.txt")]
        return program_txt_path.parent / ts_name
    return None


def _migrate_match_keys(db_path: str, *, dry_run: bool = False) -> int:
    """Re-generate match_keys for existing edcb_epg records (old→new format).

    New format includes broadcaster: title::broadcaster::date::time
    """
    con = connect_db(db_path)
    rows = con.execute(
        "SELECT path_id, data_json FROM path_metadata WHERE source='edcb_epg'",
    ).fetchall()

    updated = 0
    skipped = 0
    for path_id, data_json_str in rows:
        try:
            data = json.loads(data_json_str)
        except Exception:
            skipped += 1
            continue
        if not isinstance(data, dict):
            skipped += 1
            continue

        title = data.get("official_title", "")
        broadcaster = data.get("broadcaster", "")
        air_date = data.get("air_date", "")
        start_time = data.get("start_time", "")
        if not title or not air_date or not start_time:
            skipped += 1
            continue

        new_mk = match_key_from_epg({
            "official_title": title,
            "broadcaster": broadcaster,
            "air_date": air_date,
            "start_time": start_time,
        })
        old_mk = data.get("match_key")
        if new_mk == old_mk:
            skipped += 1
            continue

        data["match_key"] = new_mk
        updated += 1

        if not dry_run:
            con.execute(
                "UPDATE path_metadata SET data_json=?, updated_at=? WHERE path_id=? AND source='edcb_epg'",
                (json.dumps(data, ensure_ascii=False), now_iso(), path_id),
            )

    if not dry_run:
        con.commit()
    con.close()

    result = {"tool": "migrate_match_keys", "dry_run": dry_run, "total": len(rows), "updated": updated, "skipped": skipped}
    print(json.dumps(result, ensure_ascii=False))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--ts-root", help="WSL path to TS recording directory (e.g. /mnt/j/TVFile)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--migrate-match-keys", action="store_true", help="Re-generate match_keys for existing edcb_epg records")
    ap.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = ap.parse_args()

    if args.migrate_match_keys:
        return _migrate_match_keys(args.db, dry_run=args.dry_run)

    if not args.ts_root:
        ap.error("--ts-root is required unless --migrate-match-keys is used")

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    ts_root = Path(args.ts_root).resolve()
    if not ts_root.exists():
        raise SystemExit(f"TS root not found: {ts_root}")

    con = connect_db(args.db)
    create_schema_if_needed(con)

    program_txts = find_program_txt_files(ts_root)
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    total = 0
    parsed = 0
    skipped_already_ingested = 0
    skipped_parse_failed = 0
    ingested = 0
    errors: list[str] = []

    rows_to_insert: list[dict[str, Any]] = []

    try:
        for ptxt in program_txts:
            total += 1
            if args.limit and total > args.limit:
                break

            # Parse the program.txt
            epg = parse_program_txt(ptxt)
            if not epg:
                skipped_parse_failed += 1
                errors.append(f"parse_failed: {ptxt.name}")
                continue
            parsed += 1

            # Derive TS file path and its Windows equivalent
            ts_path = ts_path_from_program_txt(ptxt)
            if not ts_path:
                skipped_parse_failed += 1
                errors.append(f"cannot_derive_ts_path: {ptxt.name}")
                continue

            ts_win_path = wsl_to_windows_path(str(ts_path))
            pid = path_id_for(ts_win_path)

            # Check if already ingested (idempotency)
            existing = fetchone(
                con,
                "SELECT path_id FROM path_metadata WHERE path_id = ? AND source = 'edcb_epg'",
                (pid,),
            )
            if existing:
                skipped_already_ingested += 1
                continue

            # Generate match keys for correlation with encoded files
            match_key = match_key_from_epg(epg)
            dt_key = datetime_key_from_epg(epg)

            # Build the data payload
            data = {
                "match_key": match_key,
                "datetime_key": dt_key,
                "air_date": epg["air_date"],
                "start_time": epg["start_time"],
                "end_time": epg["end_time"],
                "broadcaster": epg["broadcaster"],
                "broadcaster_raw": epg["broadcaster_raw"],
                "official_title": epg["official_title"],
                "title_raw": epg["title_raw"],
                "annotations": epg["annotations"],
                "is_rebroadcast_flag": epg["is_rebroadcast_flag"],
                "description": epg["description"][:500] if epg["description"] else None,
                "epg_genres": epg["epg_genres"],
                "network_ids": epg["network_ids"],
                "ts_path": ts_win_path,
                "program_txt_path": str(ptxt),
                "ingested_at": now_iso(),
            }
            data["source_history"] = [make_entry("edcb_epg", list(data.keys()))]

            rows_to_insert.append({
                "pid": pid,
                "ts_win_path": ts_win_path,
                "data": data,
            })

        if not args.apply:
            summary = {
                "ok": True,
                "tool": "ingest_program_txt",
                "apply": False,
                "tsRoot": str(ts_root),
                "total": total,
                "parsed": parsed,
                "alreadyIngested": skipped_already_ingested,
                "parseFailed": skipped_parse_failed,
                "wouldIngest": len(rows_to_insert),
                "errors": errors[:20],
            }
            print(json.dumps(summary, ensure_ascii=False))
            return 0

        # Apply: write to DB
        begin_immediate(con)
        con.execute(
            """
            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, "epg_ingest", str(ts_root), started_at, None, "ingest_program_txt.py", None),
        )

        for row in rows_to_insert:
            pid = row["pid"]
            ts_win_path = row["ts_win_path"]
            data = row["data"]
            drive, dir_, name, ext = split_win(ts_win_path)
            ts_now = now_iso()

            # Ensure path exists in paths table
            con.execute(
                """
                INSERT INTO paths (path_id, path, drive, dir, name, ext, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path_id) DO UPDATE SET updated_at=excluded.updated_at
                """,
                (pid, ts_win_path, drive, dir_, name, ext, ts_now, ts_now),
            )

            # Insert EPG metadata
            data_json = json.dumps(data, ensure_ascii=False)
            con.execute(
                """
                INSERT INTO path_metadata (path_id, source, data_json, updated_at)
                VALUES (?, 'edcb_epg', ?, ?)
                ON CONFLICT(path_id) DO UPDATE SET
                  source='edcb_epg',
                  data_json=excluded.data_json,
                  updated_at=excluded.updated_at
                """,
                (pid, data_json, ts_now),
            )
            ingested += 1

        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
        con.commit()

    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    summary = {
        "ok": len(errors) == 0 or ingested > 0,
        "tool": "ingest_program_txt",
        "apply": True,
        "runId": run_id,
        "tsRoot": str(ts_root),
        "total": total,
        "parsed": parsed,
        "alreadyIngested": skipped_already_ingested,
        "parseFailed": skipped_parse_failed,
        "ingested": ingested,
        "errors": errors[:20],
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
