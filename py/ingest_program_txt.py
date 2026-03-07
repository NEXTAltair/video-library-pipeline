"""Ingest EDCB .program.txt files into mediaops.sqlite.

Scans a TS recording directory for .program.txt companion files,
parses them, and stores EPG metadata in programs/broadcasts tables.

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

from edcb_program_parser import match_key_from_epg, parse_program_txt
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchone
from pathscan_common import now_iso


def find_program_txt_files(ts_root: Path) -> list[Path]:
    """Find all .program.txt files recursively under ts_root."""
    return sorted(ts_root.rglob("*.program.txt"))


def _normalize_program_key(title: str) -> str:
    return " ".join(str(title or "").strip().lower().split())


def _program_id_for_key(program_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"program:{program_key}"))


def _broadcast_id_for_match_key(match_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"broadcast:{match_key}"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--ts-root", required=True, help="WSL path to TS recording directory (e.g. /mnt/j/TVFile)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

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

            epg = parse_program_txt(ptxt)
            if not epg:
                skipped_parse_failed += 1
                errors.append(f"parse_failed: {ptxt.name}")
                continue
            parsed += 1

            match_key = match_key_from_epg(epg)
            if not match_key:
                skipped_parse_failed += 1
                errors.append(f"missing_match_key: {ptxt.name}")
                continue

            existing = fetchone(
                con,
                "SELECT broadcast_id FROM broadcasts WHERE match_key = ?",
                (match_key,),
            )
            if existing:
                skipped_already_ingested += 1
                continue

            canonical_title = str(epg.get("official_title") or "").strip()
            if not canonical_title:
                skipped_parse_failed += 1
                errors.append(f"missing_title: {ptxt.name}")
                continue

            program_key = _normalize_program_key(canonical_title)
            program_id = _program_id_for_key(program_key)
            broadcast_id = _broadcast_id_for_match_key(match_key)

            payload = dict(epg)
            payload["match_key"] = match_key
            payload["ingested_at"] = now_iso()

            rows_to_insert.append(
                {
                    "program_id": program_id,
                    "program_key": program_key,
                    "canonical_title": canonical_title,
                    "broadcast_id": broadcast_id,
                    "match_key": match_key,
                    "air_date": epg.get("air_date"),
                    "start_time": epg.get("start_time"),
                    "end_time": epg.get("end_time"),
                    "broadcaster": epg.get("broadcaster"),
                    "data_json": json.dumps(payload, ensure_ascii=False),
                    "created_at": now_iso(),
                }
            )

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
            con.execute(
                """
                INSERT INTO programs (program_id, program_key, canonical_title, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(program_id) DO UPDATE SET
                  canonical_title=excluded.canonical_title
                """,
                (row["program_id"], row["program_key"], row["canonical_title"], row["created_at"]),
            )
            con.execute(
                """
                INSERT INTO broadcasts (broadcast_id, program_id, air_date, start_time, end_time, broadcaster, match_key, data_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(broadcast_id) DO UPDATE SET
                  air_date=excluded.air_date,
                  start_time=excluded.start_time,
                  end_time=excluded.end_time,
                  broadcaster=excluded.broadcaster,
                  match_key=excluded.match_key,
                  data_json=excluded.data_json
                """,
                (
                    row["broadcast_id"],
                    row["program_id"],
                    row["air_date"],
                    row["start_time"],
                    row["end_time"],
                    row["broadcaster"],
                    row["match_key"],
                    row["data_json"],
                    row["created_at"],
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
