"""Ingest inventory JSONL into mediaops.sqlite.

Input: JSONL produced by scripts/unwatched_inventory.ps1
Each line is a JSON object like:
  { path, dir, name, ext, type, size, mtimeUtc, nameFlags }

This script:
- creates a new runs row (kind=inventory)
- upserts paths rows (path_id is deterministic uuid5 from normalized path)
- upserts observations rows for the run
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import dataclass

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall
from pathscan_common import iter_jsonl, now_iso, path_id_for, split_win


@dataclass
class Counters:
    lines: int = 0
    paths_upserted: int = 0
    obs_upserted: int = 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--target-root", default=None)
    ap.add_argument("--tool-version", default=None)
    args = ap.parse_args()

    if not args.db:
        raise SystemExit("db is required: pass --db or configure plugin db")
    if not os.path.exists(args.db):
        raise SystemExit(
            f"DB not found: {args.db} (set --db/plugin db to an existing mediaops.sqlite path)"
        )

    con = connect_db(args.db)
    create_schema_if_needed(con)

    run_id = str(uuid.uuid4())
    started_at = now_iso()
    c = Counters()

    try:
        begin_immediate(con)
        con.execute(
            """
            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, "inventory", args.target_root, started_at, None, args.tool_version, None),
        )

        for rec in iter_jsonl(args.jsonl):
            c.lines += 1
            p = rec.get("path")
            if not p:
                continue
            pid = path_id_for(p)
            drive, parent, name, ext = split_win(p)
            ts = now_iso()

            con.execute(
                """
                INSERT INTO paths (path_id, path, drive, dir, name, ext, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path_id) DO UPDATE SET
                  path=excluded.path,
                  updated_at=excluded.updated_at,
                  drive=excluded.drive,
                  dir=excluded.dir,
                  name=excluded.name,
                  ext=excluded.ext
                """,
                (pid, p, drive, parent, name, ext, ts, ts),
            )
            c.paths_upserted += 1

            name_flags = rec.get("nameFlags")
            try:
                name_flags_json = json.dumps(name_flags, ensure_ascii=False)
            except Exception:
                name_flags_json = None

            con.execute(
                """
                INSERT INTO observations (run_id, path_id, size_bytes, mtime_utc, type, name_flags)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, path_id) DO UPDATE SET
                  size_bytes=excluded.size_bytes,
                  mtime_utc=excluded.mtime_utc,
                  type=excluded.type,
                  name_flags=excluded.name_flags
                """,
                (
                    run_id,
                    pid,
                    int(rec.get("size") or 0),
                    rec.get("mtimeUtc"),
                    rec.get("type"),
                    name_flags_json,
                ),
            )
            c.obs_upserted += 1

        finished_at = now_iso()
        con.execute("UPDATE runs SET finished_at = ? WHERE run_id = ?", (finished_at, run_id))
        n_obs = fetchall(con, "SELECT path_id FROM observations WHERE run_id = ?", (run_id,))
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    print("OK")
    print("run_id:", run_id)
    print("lines:", c.lines)
    print("paths_upserted:", c.paths_upserted)
    print("observations_upserted:", c.obs_upserted)
    print("observations_rows:", len(n_obs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

