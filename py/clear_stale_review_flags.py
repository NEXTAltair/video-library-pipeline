#!/usr/bin/env python3
"""Clear stale needs_review flags for human_reviewed rows.

Targets rows where:
- source = 'human_reviewed'
- needs_review = 1
- needs_review_reason contains ONLY title-related reasons
- program_title does NOT contain subtitle separator characters

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
from path_placement_rules import SUBTITLE_SEPARATORS, TITLE_RELATED_REASONS


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    con = connect_db(args.db)
    rows = con.execute(
        """SELECT path_id, program_title, needs_review, data_json
           FROM path_metadata
           WHERE source = 'human_reviewed'
             AND needs_review = 1"""
    ).fetchall()

    candidates: list[dict] = []
    for r in rows:
        pt = r["program_title"] or ""
        # Title still contains separators → not yet fixed, skip
        if SUBTITLE_SEPARATORS.search(pt):
            continue

        # Check needs_review_reason from data_json
        data = {}
        try:
            data = json.loads(r["data_json"]) if r["data_json"] else {}
        except Exception:
            pass
        reason = str(data.get("needs_review_reason", ""))
        parts = [rv.strip() for rv in reason.split(",") if rv.strip()]
        remaining = [p for p in parts if p not in TITLE_RELATED_REASONS]
        if remaining:
            # Non-title reasons exist, skip
            continue

        candidates.append({
            "path_id": r["path_id"],
            "program_title": pt,
            "old_reason": reason,
        })

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
    con.commit()
    con.close()
    print(json.dumps({
        "ok": True,
        "dryRun": False,
        "updatedRows": len(candidates),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
