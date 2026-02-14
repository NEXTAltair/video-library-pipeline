#!/usr/bin/env python3
r"""Update mediaops.sqlite paths table from a move_apply JSONL produced by apply_move_plan.ps1."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine
from mediaops_schema import paths, events, runs


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

    engine = create_engine(f"sqlite:///{args.db}")
    moved = 0
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
        updates.append((pid, full, dir_, name))
        moved += 1
        rows_events.append(
            {
                "run_id": "TBD",
                "ts": rec.get("ts") or now_iso(),
                "kind": "move",
                "src_path_id": pid,
                "dst_path_id": None,
                "detail_json": json.dumps({"src": src, "dst": dst, "source": "apply_move_plan.ps1"}, ensure_ascii=False),
                "ok": 1,
                "error": None,
            }
        )

    if args.dry_run:
        print(json.dumps({"applied": args.applied, "would_update": len(updates), "would_events": len(rows_events)}, ensure_ascii=False))
        return 0

    run_id = __import__("uuid").uuid4().hex
    started = now_iso()

    with engine.begin() as conn:
        conn.execute(
            runs.insert().values(
                run_id=run_id,
                kind="apply",
                target_root=None,
                started_at=started,
                finished_at=None,
                tool_version=None,
                notes=args.notes or f"update_db_paths_from_move_apply {os.path.basename(args.applied)}",
            )
        )
        for pid, full, dir_, name in updates:
            conn.execute(paths.update().where(paths.c.path_id == pid).values(path=full, dir=dir_, name=name, updated_at=now_iso()))
        for r in rows_events:
            r["run_id"] = run_id
        if rows_events:
            conn.execute(events.insert(), rows_events)
        conn.execute(runs.update().where(runs.c.run_id == run_id).values(finished_at=now_iso()))

    print(json.dumps({"applied": args.applied, "updated": len(updates), "events": len(rows_events), "run_id": run_id}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
