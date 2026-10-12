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
from path_placement_rules import DB_CONTRACT_REQUIRED
from pathscan_common import iter_jsonl, now_iso
from source_history import merge_data


def validate_db_contract(rec: dict) -> tuple[bool, str]:
    missing = sorted([k for k in DB_CONTRACT_REQUIRED if k not in rec])
    if missing:
        return False, f"missing DB contract keys: {missing}"
    if not isinstance(rec.get("needs_review"), bool):
        return False, "needs_review must be bool"
    return True, ""



def upsert_records(con, records, *, source="human_reviewed", franchise_rules="", strict=False):
    """Merge and write under the caller's BEGIN IMMEDIATE; never commit here."""
    if not con.in_transaction:
        raise ValueError("metadata upsert requires an active transaction")
    updated_at = now_iso()
    to_upsert = []
    missing = 0
    for rec in records:
        if "_meta" in rec:
            continue
        ok, reason = validate_db_contract(rec)
        if not ok:
            raise ValueError(f"invalid metadata contract: {reason}")
        path_id = rec.get("path_id")
        if not path_id:
            p = rec.get("path")
            if not p:
                if strict:
                    raise ValueError("metadata has no registered path identity")
                missing += 1
                continue
            row = fetchone(con, "SELECT path_id FROM paths WHERE path = ?", (p,))
            path_id = row["path_id"] if row else None

        if not path_id:
            if strict:
                raise ValueError("metadata path is not registered")
            missing += 1
            continue
        if strict:
            actual = fetchone(con, "SELECT path FROM paths WHERE path_id=?", (path_id,))
            if actual is None or (rec.get("path") and rec["path"] != actual["path"]):
                raise ValueError(f"metadata path identity mismatch: {path_id}")

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

        merged = merge_data(existing, rec, source)
        # Normalize: needs_review=false must not coexist with non-empty needs_review_reason
        if not merged.get("needs_review"):
            merged["needs_review_reason"] = ""
        merged["genre"] = resolve_genre(merged)
        merged["franchise"] = resolve_franchise(merged, franchise_rules or None)

        promoted, data_json = split_path_metadata(merged)
        to_upsert.append((
            path_id, source, data_json, updated_at,
            promoted.get("program_title"),
            promoted.get("air_date"),
            promoted.get("needs_review", 0),
            promoted.get("episode_no"),
            promoted.get("subtitle"),
            promoted.get("broadcaster"),
            promoted.get("human_reviewed", 0),
        ))
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
    return {"rows": len(to_upsert), "missing": missing}

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

    try:
        begin_immediate(con)
        create_schema_if_needed(con)
        summary = upsert_records(con, iter_jsonl(args.inp), source=args.source,
                                 franchise_rules=args.franchise_rules)
        if args.dry_run:
            con.rollback()
        else:
            con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()

    print("OK")
    print("upserted:", summary["rows"])
    print("missing:", summary["missing"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
