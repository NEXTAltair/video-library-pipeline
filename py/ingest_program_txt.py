"""Ingest EDCB .program.txt files into program/broadcast tables.

Scans a TS recording directory for .program.txt companion files,
parses them, and stores EPG metadata in `programs` + `broadcasts`.

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

from db_helpers import split_broadcast_data
from edcb_program_parser import datetime_key_from_epg, match_key_from_epg, parse_program_txt
from epg_common import broadcast_id_for, normalize_program_key, program_id_for
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from pathscan_common import now_iso
from series_name_extractor import series_program_id, series_program_key


def find_program_txt_files(ts_root: Path) -> list[Path]:
    """Find all .program.txt files recursively under ts_root."""
    return sorted(ts_root.rglob("*.program.txt"))


def _migrate_match_keys(db_path: str, *, dry_run: bool = False) -> int:
    """Re-generate match_keys for existing broadcasts (old→new format)."""
    con = connect_db(db_path)
    rows = con.execute("SELECT broadcast_id, data_json FROM broadcasts").fetchall()

    updated = 0
    skipped = 0
    for broadcast_id, data_json_str in rows:
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
                "UPDATE broadcasts SET match_key=?, data_json=? WHERE broadcast_id=?",
                (new_mk, json.dumps(data, ensure_ascii=False), broadcast_id),
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
    ap.add_argument("--migrate-match-keys", action="store_true", help="Re-generate match_keys for existing broadcast records")
    ap.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    ap.add_argument("--aliases", default="", help="Path to program_aliases.yaml for series-level grouping")
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

    # Auto-detect aliases path
    aliases_path = args.aliases or str(Path(__file__).resolve().parent.parent / "rules" / "program_aliases.yaml")

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

            epg = parse_program_txt(ptxt)
            if not epg:
                skipped_parse_failed += 1
                errors.append(f"parse_failed: {ptxt.name}")
                continue
            parsed += 1

            match_key = match_key_from_epg(epg)
            dt_key = datetime_key_from_epg(epg)
            official_title = str(epg.get("official_title") or "").strip()

            # Series-level program key
            s_key = series_program_key(official_title, aliases_path=aliases_path)
            s_pid = program_id_for(s_key)

            fallback_seed = "::".join([
                s_pid,
                str(epg.get("air_date") or ""),
                str(epg.get("start_time") or ""),
                str(epg.get("broadcaster") or ""),
            ])
            broadcast_id = broadcast_id_for(match_key, fallback_seed)

            existing = con.execute(
                "SELECT broadcast_id FROM broadcasts WHERE match_key = ?",
                (match_key,),
            ).fetchone()
            if existing:
                skipped_already_ingested += 1
                continue

            data = {
                "match_key": match_key,
                "datetime_key": dt_key,
                "air_date": epg["air_date"],
                "start_time": epg["start_time"],
                "end_time": epg["end_time"],
                "broadcaster": epg["broadcaster"],
                "broadcaster_raw": epg["broadcaster_raw"],
                "official_title": official_title,
                "title_raw": epg["title_raw"],
                "annotations": epg["annotations"],
                "is_rebroadcast_flag": epg["is_rebroadcast_flag"],
                "description": epg["description"][:500] if epg["description"] else None,
                "epg_genres": epg["epg_genres"],
                "detail_sections": epg.get("detail_sections"),
                "network_ids": epg["network_ids"],
                "program_txt_path": str(ptxt),
                "ingested_at": now_iso(),
            }

            rows_to_insert.append({
                "program_id": s_pid,
                "program_key": s_key,
                "canonical_title": official_title or "UNKNOWN",
                "broadcast_id": broadcast_id,
                "match_key": match_key,
                "air_date": epg.get("air_date"),
                "start_time": epg.get("start_time"),
                "end_time": epg.get("end_time"),
                "broadcaster": epg.get("broadcaster"),
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

        begin_immediate(con)
        con.execute(
            """
            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, "epg_ingest", str(ts_root), started_at, None, "ingest_program_txt.py", None),
        )

        for row in rows_to_insert:
            ts_now = now_iso()
            con.execute(
                """
                INSERT INTO programs (program_id, program_key, canonical_title, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(program_id) DO UPDATE SET
                  canonical_title=excluded.canonical_title
                """,
                (row["program_id"], row["program_key"], row["canonical_title"], ts_now),
            )

            # Split broadcast data into promoted columns + residual data_json
            promoted, data_json = split_broadcast_data(row["data"])

            con.execute(
                """
                INSERT INTO broadcasts (broadcast_id, program_id, air_date, start_time,
                  end_time, broadcaster, match_key, data_json, created_at,
                  is_rebroadcast_flag, epg_genres, description, official_title, annotations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(broadcast_id) DO UPDATE SET
                  program_id=excluded.program_id,
                  air_date=excluded.air_date,
                  start_time=excluded.start_time,
                  end_time=excluded.end_time,
                  broadcaster=excluded.broadcaster,
                  match_key=excluded.match_key,
                  data_json=excluded.data_json,
                  is_rebroadcast_flag=excluded.is_rebroadcast_flag,
                  epg_genres=excluded.epg_genres,
                  description=excluded.description,
                  official_title=excluded.official_title,
                  annotations=excluded.annotations
                """,
                (
                    row["broadcast_id"],
                    row["program_id"],
                    row["air_date"],
                    row["start_time"],
                    row["end_time"],
                    row["broadcaster"],
                    row["match_key"],
                    data_json,
                    ts_now,
                    promoted.get("is_rebroadcast_flag"),
                    promoted.get("epg_genres"),
                    promoted.get("description"),
                    promoted.get("official_title"),
                    promoted.get("annotations"),
                ),
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
