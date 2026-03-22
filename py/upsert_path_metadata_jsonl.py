"""Upsert LLM-extracted metadata JSONL into `path_metadata`.

Input JSONL: one object per line.
- Must include either `path_id` or `path`.
- Any additional fields are stored verbatim in `data_json`.

We also stamp:
- source (arg --source)
- updated_at (now)

Usage:
  cd <video-library-pipeline-dir>/py
  python upsert_path_metadata_jsonl.py --in extracted.jsonl --source rule_based

Safety:
- DB write only. No file operations.
"""

from __future__ import annotations

import argparse
import json
import os

from db_helpers import reconstruct_path_metadata, split_path_metadata
from franchise_resolver import resolve_franchise
from genre_resolver import resolve_genre
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchone
from pathscan_common import iter_jsonl, now_iso
from source_history import merge_data

DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}


def validate_db_contract(rec: dict) -> tuple[bool, str]:
    missing = sorted([k for k in DB_CONTRACT_REQUIRED if k not in rec])
    if missing:
        return False, f"missing DB contract keys: {missing}"
    if not isinstance(rec.get("needs_review"), bool):
        return False, "needs_review must be bool"
    return True, ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--source", default="rule_based")
    ap.add_argument("--franchise-rules", default="")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db or configure plugin db")
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    if not os.path.exists(args.inp):
        raise SystemExit(f"Input JSONL not found: {args.inp}")

    con = connect_db(args.db)
    create_schema_if_needed(con)

    updated_at = now_iso()
    to_upsert = []
    missing = 0

    try:
        for rec in iter_jsonl(args.inp):
            if "_meta" in rec:
                continue
            ok, reason = validate_db_contract(rec)
            if not ok:
                raise SystemExit(f"invalid metadata contract: {reason}")
            path_id = rec.get("path_id")
            if not path_id:
                p = rec.get("path")
                if not p:
                    missing += 1
                    continue
                row = fetchone(con, "SELECT path_id FROM paths WHERE path = ?", (p,))
                path_id = row["path_id"] if row else None

            if not path_id:
                missing += 1
                continue

            prior = fetchone(
                con,
                """SELECT source, data_json, program_title, air_date, needs_review,
                          episode_no, subtitle, broadcaster, human_reviewed
                   FROM path_metadata WHERE path_id=?""",
                (path_id,),
            )
            existing = {}
            if prior:
                existing = reconstruct_path_metadata(prior)

            merged = merge_data(existing, rec, args.source)
            merged["genre"] = resolve_genre(merged)
            merged["franchise"] = resolve_franchise(merged, args.franchise_rules or None)

            promoted, data_json = split_path_metadata(merged)
            to_upsert.append((
                path_id, args.source, data_json, updated_at,
                promoted.get("program_title"),
                promoted.get("air_date"),
                promoted.get("needs_review", 0),
                promoted.get("episode_no"),
                promoted.get("subtitle"),
                promoted.get("broadcaster"),
                promoted.get("human_reviewed", 0),
            ))

        if args.dry_run:
            print("DRY_RUN")
            print("rows:", len(to_upsert))
            print("missing:", missing)
            return 0

        begin_immediate(con)
        if to_upsert:
            con.executemany(
                """
                INSERT INTO path_metadata (path_id, source, data_json, updated_at,
                  program_title, air_date, needs_review,
                  episode_no, subtitle, broadcaster, human_reviewed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path_id) DO UPDATE SET
                  source=excluded.source,
                  data_json=excluded.data_json,
                  updated_at=excluded.updated_at,
                  program_title=excluded.program_title,
                  air_date=excluded.air_date,
                  needs_review=excluded.needs_review,
                  episode_no=excluded.episode_no,
                  subtitle=excluded.subtitle,
                  broadcaster=excluded.broadcaster,
                  human_reviewed=excluded.human_reviewed
                WHERE path_metadata.human_reviewed IS NOT 1
                   OR excluded.source = 'human_reviewed'
                """,
                to_upsert,
            )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    print("OK")
    print("upserted:", len(to_upsert))
    print("missing:", missing)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
