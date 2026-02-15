"""Upsert LLM-extracted metadata JSONL into `path_metadata`.

Input JSONL: one object per line.
- Must include either `path_id` or `path`.
- Any additional fields are stored verbatim in `data_json`.

We also stamp:
- source (arg --source)
- updated_at (now)

Usage:
  cd <video-library-pipeline-dir>/py
  python upsert_path_metadata_jsonl.py --in extracted.jsonl --source llm

Safety:
- DB write only. No file operations.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchone

DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


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
    ap.add_argument("--source", default="llm")
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

            data_json = json.dumps(rec, ensure_ascii=False)
            to_upsert.append((path_id, args.source, data_json, updated_at))

        if args.dry_run:
            print("DRY_RUN")
            print("rows:", len(to_upsert))
            print("missing:", missing)
            return 0

        begin_immediate(con)
        if to_upsert:
            con.executemany(
                """
                INSERT INTO path_metadata (path_id, source, data_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(path_id) DO UPDATE SET
                  source=excluded.source,
                  data_json=excluded.data_json,
                  updated_at=excluded.updated_at
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

