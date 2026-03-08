#!/usr/bin/env python3
"""Migrate source='llm_subagent' -> 'llm' in path_metadata.

Updates:
1. path_metadata.source column: 'llm_subagent' -> 'llm'
2. data_json.source_history[*].source: 'llm_subagent' -> 'llm'

Usage:
  python migrate_source_llm_subagent_to_llm.py --db mediaops.sqlite --dry-run
  python migrate_source_llm_subagent_to_llm.py --db mediaops.sqlite
"""

from __future__ import annotations

import argparse
import json

from mediaops_schema import begin_immediate, connect_db

FROM_SOURCE = "llm_subagent"
TO_SOURCE = "llm"


def _rewrite_source_history(data: object) -> tuple[object, bool]:
    if not isinstance(data, dict):
        return data, False

    source_history = data.get("source_history")
    if not isinstance(source_history, list):
        return data, False

    changed = False
    for entry in source_history:
        if isinstance(entry, dict) and entry.get("source") == FROM_SOURCE:
            entry["source"] = TO_SOURCE
            changed = True

    return data, changed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = connect_db(args.db)
    col_count = con.execute(
        "SELECT COUNT(*) AS c FROM path_metadata WHERE source=?",
        (FROM_SOURCE,),
    ).fetchone()["c"]

    rows = con.execute("SELECT path_id, data_json FROM path_metadata WHERE data_json IS NOT NULL").fetchall()

    json_updates: list[tuple[str, str]] = []
    for r in rows:
        raw = r["data_json"]
        try:
            data = json.loads(str(raw))
        except Exception:
            continue

        data, changed = _rewrite_source_history(data)
        if changed:
            json_updates.append((json.dumps(data, ensure_ascii=False), str(r["path_id"])))

    report = {
        "ok": True,
        "dryRun": args.dry_run,
        "sourceColumnRows": int(col_count),
        "sourceHistoryRows": len(json_updates),
    }

    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False))
        con.close()
        return 0

    begin_immediate(con)
    if col_count:
        con.execute(
            "UPDATE path_metadata SET source=? WHERE source=?",
            (TO_SOURCE, FROM_SOURCE),
        )
    if json_updates:
        con.executemany(
            "UPDATE path_metadata SET data_json=? WHERE path_id=?",
            json_updates,
        )
    con.commit()
    con.close()

    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
