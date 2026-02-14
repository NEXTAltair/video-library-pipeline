"""Ingest inventory JSONL into mediaops.sqlite.

Input: JSONL produced by scripts/inventory_scan.ps1
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
from datetime import datetime, timezone
from pathlib import PureWindowsPath
from typing import Iterable

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from mediaops_schema import metadata, runs, paths, observations

DB_DEFAULT = "/mnt/b/_AI_WORK/db/mediaops.sqlite"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_win_path(p: str) -> str:
    p = p.replace("/", "\\")
    return p.lower()


PATH_NAMESPACE = uuid.UUID("f4f67a6f-90c6-4ee4-9c1a-2c0d25b3b0c4")


def path_id_for(p: str) -> str:
    norm = normalize_win_path(p)
    return str(uuid.uuid5(PATH_NAMESPACE, "winpath:" + norm))


def split_path(p: str) -> tuple[str | None, str | None, str | None, str | None]:
    wp = PureWindowsPath(p)
    drive = wp.drive[:-1] if wp.drive.endswith(":") else (wp.drive or None)
    name = wp.name or None
    ext = wp.suffix or None
    parent = str(wp.parent) if str(wp.parent) not in (".", "") else None
    return drive, parent, name, ext


@dataclass
class Counters:
    lines: int = 0
    paths_upserted: int = 0
    obs_upserted: int = 0


def iter_jsonl(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--target-root", default=None)
    ap.add_argument("--tool-version", default=None)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db} (did you run alembic upgrade head?)")

    engine = create_engine(f"sqlite:///{args.db}")

    run_id = str(uuid.uuid4())
    started_at = now_iso()
    c = Counters()

    with engine.begin() as conn:
        conn.execute(
            runs.insert().values(
                run_id=run_id,
                kind="inventory",
                target_root=args.target_root,
                started_at=started_at,
                finished_at=None,
                tool_version=args.tool_version,
                notes=None,
            )
        )

        for rec in iter_jsonl(args.jsonl):
            c.lines += 1
            p = rec.get("path")
            if not p:
                continue
            pid = path_id_for(p)
            drive, parent, name, ext = split_path(p)
            ts = now_iso()

            stmt_p = sqlite_insert(paths).values(
                path_id=pid,
                path=p,
                drive=drive,
                dir=parent,
                name=name,
                ext=ext,
                created_at=ts,
                updated_at=ts,
            )
            stmt_p = stmt_p.on_conflict_do_update(
                index_elements=[paths.c.path_id],
                set_={
                    "path": p,
                    "updated_at": ts,
                    "drive": drive,
                    "dir": parent,
                    "name": name,
                    "ext": ext,
                },
            )
            conn.execute(stmt_p)
            c.paths_upserted += 1

            name_flags = rec.get("nameFlags")
            try:
                name_flags_json = json.dumps(name_flags, ensure_ascii=False)
            except Exception:
                name_flags_json = None

            stmt_o = sqlite_insert(observations).values(
                run_id=run_id,
                path_id=pid,
                size_bytes=int(rec.get("size") or 0),
                mtime_utc=rec.get("mtimeUtc"),
                type=rec.get("type"),
                name_flags=name_flags_json,
            )
            stmt_o = stmt_o.on_conflict_do_update(
                index_elements=[observations.c.run_id, observations.c.path_id],
                set_={
                    "size_bytes": int(rec.get("size") or 0),
                    "mtime_utc": rec.get("mtimeUtc"),
                    "type": rec.get("type"),
                    "name_flags": name_flags_json,
                },
            )
            conn.execute(stmt_o)
            c.obs_upserted += 1

        finished_at = now_iso()
        conn.execute(
            runs.update().where(runs.c.run_id == run_id).values(finished_at=finished_at)
        )
        n_obs = conn.execute(select(observations.c.path_id).where(observations.c.run_id == run_id)).fetchall()

    print("OK")
    print("run_id:", run_id)
    print("lines:", c.lines)
    print("paths_upserted:", c.paths_upserted)
    print("observations_upserted:", c.obs_upserted)
    print("observations_rows:", len(n_obs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
