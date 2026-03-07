"""Ingest EDCB .program.txt files into mediaops.sqlite.

Scans a TS recording directory for .program.txt companion files,
parses them, and stores EPG metadata into file-independent tables:
- programs
- broadcasts

Usage:
  cd <video-library-pipeline-dir>/py
  python ingest_program_txt.py --db <db-path> --ts-root <ts-root> [--apply]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
import uuid
from pathlib import Path
from typing import Any

from edcb_program_parser import datetime_key_from_epg, match_key_from_epg, parse_program_txt
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from pathscan_common import now_iso
from source_history import make_entry

_WS = re.compile(r"[\s\u3000]+")
_BAD = re.compile(r"[<>:\"/\\\\|?*]")


def find_program_txt_files(ts_root: Path) -> list[Path]:
    """Find all .program.txt files recursively under ts_root."""
    return sorted(ts_root.rglob("*.program.txt"))


def _program_key_from_title(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(title or "")).lower()
    normalized = _BAD.sub("", normalized)
    normalized = _WS.sub("_", normalized).strip("_")
    return normalized or "unknown"


def _program_id_from_key(program_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"program:{program_key}"))


def _broadcast_id(program_id: str, epg: dict[str, Any], match_key: str | None) -> str:
    if match_key:
        seed = f"broadcast:{match_key}"
    else:
        seed = "::".join(
            [
                "broadcast",
                program_id,
                str(epg.get("air_date") or ""),
                str(epg.get("start_time") or ""),
                str(epg.get("end_time") or ""),
                str(epg.get("broadcaster") or ""),
            ]
        )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


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
    skipped_parse_failed = 0
    ingested_programs = 0
    ingested_broadcasts = 0
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

            title = str(epg.get("official_title") or "")
            program_key = _program_key_from_title(title)
            program_id = _program_id_from_key(program_key)

            match_key = match_key_from_epg(epg)
            dt_key = datetime_key_from_epg(epg)
            data = {
                "match_key": match_key,
                "datetime_key": dt_key,
                "air_date": epg.get("air_date"),
                "start_time": epg.get("start_time"),
                "end_time": epg.get("end_time"),
                "broadcaster": epg.get("broadcaster"),
                "broadcaster_raw": epg.get("broadcaster_raw"),
                "official_title": epg.get("official_title"),
                "title_raw": epg.get("title_raw"),
                "annotations": epg.get("annotations"),
                "is_rebroadcast_flag": epg.get("is_rebroadcast_flag"),
                "description": (epg.get("description") or "")[:500] or None,
                "epg_genres": epg.get("epg_genres"),
                "detail_sections": epg.get("detail_sections"),
                "network_ids": epg.get("network_ids"),
                "program_txt_path": str(ptxt),
                "ingested_at": now_iso(),
            }
            data["source_history"] = [make_entry("edcb_epg", list(data.keys()))]

            rows_to_insert.append(
                {
                    "program_id": program_id,
                    "program_key": program_key,
                    "canonical_title": title or "UNKNOWN",
                    "broadcast_id": _broadcast_id(program_id, epg, match_key),
                    "air_date": epg.get("air_date"),
                    "start_time": epg.get("start_time"),
                    "end_time": epg.get("end_time"),
                    "broadcaster": epg.get("broadcaster"),
                    "match_key": match_key,
                    "data_json": json.dumps(data, ensure_ascii=False),
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
                "parseFailed": skipped_parse_failed,
                "wouldIngestBroadcasts": len(rows_to_insert),
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

        seen_program_ids: set[str] = set()
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
            if row["program_id"] not in seen_program_ids:
                seen_program_ids.add(row["program_id"])
                ingested_programs += 1

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
                    row["broadcast_id"],
                    row["program_id"],
                    row["air_date"],
                    row["start_time"],
                    row["end_time"],
                    row["broadcaster"],
                    row["match_key"],
                    row["data_json"],
                    ts_now,
                ),
            )
            ingested_broadcasts += 1

        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
        con.commit()

    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    summary = {
        "ok": len(errors) == 0 or ingested_broadcasts > 0,
        "tool": "ingest_program_txt",
        "apply": True,
        "runId": run_id,
        "tsRoot": str(ts_root),
        "total": total,
        "parsed": parsed,
        "parseFailed": skipped_parse_failed,
        "ingestedPrograms": ingested_programs,
        "ingestedBroadcasts": ingested_broadcasts,
        "errors": errors[:20],
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
