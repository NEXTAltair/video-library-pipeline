#!/usr/bin/env python3
"""Clear stale needs_review flags for human_reviewed rows.

Targets rows where:
- source = 'human_reviewed'
- needs_review = 1
- needs_review_reason contains ONLY title-related reasons
- program_title passes suspicious-title checks (no separator / swallowed / shortened)

These are rows where the title was already corrected by YAML review
but needs_review was not cleared due to a prior bug.

Usage:
  python clear_stale_review_flags.py --db mediaops.sqlite         # dry-run
  python clear_stale_review_flags.py --db mediaops.sqlite --apply # apply
"""

from __future__ import annotations

import argparse
import json
import sys

from mediaops_schema import begin_immediate, connect_db
from path_placement_rules import TITLE_RELATED_REASONS, detect_subtitle_in_program_title
from plan_validation import detect_shortened_program_title, detect_swallowed_program_title


def has_only_title_related_reasons(reason: str) -> bool:
    parts = [rv.strip() for rv in str(reason or "").split(",") if rv.strip()]
    return all(part in TITLE_RELATED_REASONS for part in parts)


def title_still_looks_suspicious(path: str | None, program_title: str) -> bool:
    pt = str(program_title or "")
    if detect_subtitle_in_program_title(pt):
        return True
    if not path:
        return False
    md = {"program_title": pt}
    return detect_swallowed_program_title(path, md) or detect_shortened_program_title(path, md)


def find_stale_review_flag_candidates(con) -> list[dict]:
    rows = con.execute(
        """SELECT pm.path_id, pm.program_title, pm.needs_review, pm.data_json, p.path
           FROM path_metadata pm
           LEFT JOIN paths p ON p.path_id = pm.path_id
           WHERE pm.source = 'human_reviewed'
             AND pm.needs_review = 1"""
    ).fetchall()

    candidates: list[dict] = []
    for r in rows:
        pt = r["program_title"] or ""
        if title_still_looks_suspicious(r["path"], pt):
            continue

        data = {}
        try:
            data = json.loads(r["data_json"]) if r["data_json"] else {}
        except Exception:
            pass
        reason = str(data.get("needs_review_reason", ""))
        if not has_only_title_related_reasons(reason):
            continue

        candidates.append({
            "path_id": r["path_id"],
            "program_title": pt,
            "old_reason": reason,
            "path": r["path"],
        })

    return candidates


def apply_clear_stale_review_flags(con, candidates: list[dict]) -> int:
    if not candidates:
        return 0
    begin_immediate(con)
    path_ids = [(c["path_id"],) for c in candidates]
    con.executemany(
        """UPDATE path_metadata
           SET needs_review = 0,
               data_json = json_set(
                   COALESCE(data_json, '{}'),
                   '$.needs_review', json('false'),
                   '$.needs_review_reason', ''
               ),
               updated_at = datetime('now')
           WHERE path_id = ?
             AND source = 'human_reviewed'
             AND needs_review = 1""",
        path_ids,
    )
    return len(candidates)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    con = connect_db(args.db)
    candidates = find_stale_review_flag_candidates(con)

    if not args.apply:
        print(json.dumps({
            "ok": True,
            "dryRun": True,
            "candidateCount": len(candidates),
            "samples": candidates[:20],
        }, ensure_ascii=False))
        con.close()
        return 0

    if not candidates:
        print(json.dumps({"ok": True, "dryRun": False, "updatedRows": 0}, ensure_ascii=False))
        con.close()
        return 0

    updated_rows = apply_clear_stale_review_flags(con, candidates)
    con.commit()
    con.close()
    print(json.dumps({
        "ok": True,
        "dryRun": False,
        "updatedRows": updated_rows,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
