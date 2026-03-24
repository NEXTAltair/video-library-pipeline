"""Validate and upsert LLM-extracted metadata JSONL into path_metadata.

This script is the apply step after an LLM extractor has written output.
It performs additional sanity checks on top of the base upsert contract:
- subtitle separator (▽/▼/◇) must not remain in program_title
- program_title must not exceed 80 characters
- confidence must be a float 0.0–1.0
- Records failing checks are marked needs_review=True with a reason code appended

Usage:
  python apply_llm_extract_output.py --db mediaops.sqlite --in output.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, str(__file__).rsplit("/", 1)[0])  # ensure local imports work

from db_helpers import reconstruct_path_metadata, split_path_metadata
from genre_resolver import resolve_genre
from franchise_resolver import resolve_franchise
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchone
from path_placement_rules import SUBTITLE_SEPARATORS
from pathscan_common import iter_jsonl, now_iso
from source_history import make_entry, merge_data

DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}
AIR_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _append_reason(rec: dict, reason: str) -> None:
    existing = rec.get("needs_review_reason") or ""
    parts = [r.strip() for r in existing.split(",") if r.strip()]
    if reason not in parts:
        parts.append(reason)
    rec["needs_review_reason"] = ",".join(parts)


def validate_and_coerce(rec: dict) -> tuple[bool, str]:
    """Validate DB contract fields, run extra LLM-output checks, coerce types.
    Returns (ok, error_message). Mutates rec in place for coercible issues."""

    # --- DB contract ---
    missing = sorted([k for k in DB_CONTRACT_REQUIRED if k not in rec])
    if missing:
        return False, f"missing required fields: {missing}"

    pt = rec.get("program_title")
    if not isinstance(pt, str) or not pt.strip():
        return False, f"program_title must be a non-empty string, got {pt!r}"

    nr = rec.get("needs_review")
    if not isinstance(nr, bool):
        # coerce
        rec["needs_review"] = bool(nr)

    # --- Extra checks ---
    if SUBTITLE_SEPARATORS.search(pt):
        rec["needs_review"] = True
        _append_reason(rec, "subtitle_separator_in_program_title")

    if len(pt) > 80:
        rec["needs_review"] = True
        _append_reason(rec, "program_title_too_long")

    if pt in ("UNKNOWN", ""):
        rec["needs_review"] = True
        _append_reason(rec, "unknown_program_title")

    air = rec.get("air_date")
    if air is not None and not (isinstance(air, str) and AIR_DATE_RE.match(air)):
        rec["needs_review"] = True
        _append_reason(rec, "invalid_air_date_format")

    conf = rec.get("confidence")
    if conf is not None:
        try:
            rec["confidence"] = float(conf)
            if not (0.0 <= rec["confidence"] <= 1.0):
                raise ValueError
        except (ValueError, TypeError):
            rec["confidence"] = 0.5
            rec["needs_review"] = True
            _append_reason(rec, "invalid_confidence")

    # Normalize: needs_review=false must not coexist with non-empty needs_review_reason
    if not rec.get("needs_review"):
        rec["needs_review_reason"] = ""

    return True, ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--source", default="llm")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("--db is required")
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    if not os.path.exists(args.inp):
        raise SystemExit(f"Input JSONL not found: {args.inp}")

    con = connect_db(args.db)
    create_schema_if_needed(con)

    updated_at = now_iso()
    to_upsert: list[tuple] = []
    skipped = 0
    needs_review_count = 0
    validation_errors: list[str] = []

    for rec in iter_jsonl(args.inp):
        if "_meta" in rec:
            continue

        ok, err = validate_and_coerce(rec)
        if not ok:
            validation_errors.append(f"path_id={rec.get('path_id', '?')}: {err}")
            skipped += 1
            continue

        if rec.get("needs_review"):
            needs_review_count += 1

        path_id = rec.get("path_id")
        if not path_id:
            p = rec.get("path")
            if not p:
                skipped += 1
                continue
            row = fetchone(con, "SELECT path_id FROM paths WHERE path = ?", (p,))
            path_id = row["path_id"] if row else None

        if not path_id:
            skipped += 1
            continue

        # Genre / franchise resolution
        if not rec.get("genre"):
            rec["genre"] = resolve_genre(rec)
        if not rec.get("franchise"):
            rec["franchise"] = resolve_franchise(rec)

        # Merge with existing data in DB
        existing_row = fetchone(
            con,
            """SELECT source, data_json, program_title, air_date, needs_review,
                      episode_no, subtitle, broadcaster, human_reviewed
               FROM path_metadata WHERE path_id = ?""",
            (path_id,),
        )
        if existing_row:
            existing_data = reconstruct_path_metadata(existing_row)
            rec["source_history"] = [make_entry(args.source, [k for k, v in rec.items() if v is not None and k != "source_history"])]
            rec = merge_data(existing_data, rec, args.source)
        else:
            rec["source_history"] = [make_entry(args.source, [k for k, v in rec.items() if v is not None and k != "source_history"])]

        promoted, data_json = split_path_metadata(rec)
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

    if validation_errors:
        for e in validation_errors[:20]:
            print(f"VALIDATION_ERROR {e}")

    result = {
        "ok": True,
        "tool": "apply_llm_extract_output",
        "inputPath": str(args.inp),
        "upserted": len(to_upsert),
        "skipped": int(skipped),
        "needsReview": int(needs_review_count),
        "validationErrors": len(validation_errors),
        "dryRun": bool(args.dry_run),
    }

    if args.dry_run:
        print(json.dumps(result, ensure_ascii=False))
        return 0

    # human_reviewed=true のレコードが何件保護されるか事前カウント
    human_reviewed_protected = 0
    if to_upsert:
        pids = [r[0] for r in to_upsert]
        placeholders = ",".join("?" * len(pids))
        row = con.execute(
            f"SELECT COUNT(*) FROM path_metadata WHERE path_id IN ({placeholders})"
            f" AND human_reviewed = 1",
            pids,
        ).fetchone()
        human_reviewed_protected = row[0] if row else 0

    try:
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
                """,
                to_upsert,
            )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    result["humanReviewedProtected"] = int(human_reviewed_protected)

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
