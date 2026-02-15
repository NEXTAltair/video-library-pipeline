#!/usr/bin/env python3
r"""Update mediaops.sqlite paths table from a move_apply JSONL produced by apply_move_plan.ps1."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def split_win(p: str) -> tuple[str, str, str]:
    p = p.replace("/", "\\")
    dir_ = "\\".join(p.split("\\")[:-1])
    name = p.split("\\")[-1]
    return p, dir_, name


def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--applied", required=True)
    ap.add_argument("--notes", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db or configure plugin db")
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    if not os.path.exists(args.applied):
        raise SystemExit(f"Applied JSONL not found: {args.applied}")

    con = connect_db(args.db)
    create_schema_if_needed(con)

    rows_events = []
    updates = []

    for rec in iter_jsonl(args.applied):
        if rec.get("op") != "move":
            continue
        if not rec.get("ok"):
            continue
        pid = rec.get("path_id")
        src = rec.get("src")
        dst = rec.get("dst")
        if not pid or not dst:
            continue
        full, dir_, name = split_win(dst)
        updates.append((full, dir_, name, now_iso(), pid))
        rows_events.append(
            (
                "TBD",
                rec.get("ts") or now_iso(),
                "move",
                pid,
                None,
                json.dumps({"src": src, "dst": dst, "source": "apply_move_plan.ps1"}, ensure_ascii=False),
                1,
                None,
            )
        )

    if args.dry_run:
        print(json.dumps({"applied": args.applied, "would_update": len(updates), "would_events": len(rows_events)}, ensure_ascii=False))
        return 0

    run_id = uuid.uuid4().hex
    started = now_iso()

    try:
        begin_immediate(con)
        con.execute(
            """
            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                "apply",
                None,
                started,
                None,
                None,
                args.notes or f"update_db_paths_from_move_apply {os.path.basename(args.applied)}",
            ),
        )
        if updates:
            con.executemany(
                "UPDATE paths SET path=?, dir=?, name=?, updated_at=? WHERE path_id=?",
                updates,
            )
        if rows_events:
            rows_events = [(run_id, *row[1:]) for row in rows_events]
            con.executemany(
                """
                INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows_events,
            )
        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    print(json.dumps({"applied": args.applied, "updated": len(updates), "events": len(rows_events), "run_id": run_id}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

