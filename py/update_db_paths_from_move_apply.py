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


def _json_obj(s: str | None) -> dict:
    if not s:
        return {}
    try:
        v = json.loads(s)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _metadata_rank(row) -> tuple[int, str]:
    if row is None:
        return (-1, "")
    data = _json_obj(row["data_json"])
    human_reviewed = bool(data.get("human_reviewed"))
    return (1 if human_reviewed else 0, str(row["updated_at"] or ""))


def _merge_path_metadata(con, src_path_id: str, dst_path_id: str) -> str:
    src = con.execute(
        "SELECT path_id, source, data_json, updated_at FROM path_metadata WHERE path_id=?",
        (src_path_id,),
    ).fetchone()
    if src is None:
        return "none"
    dst = con.execute(
        "SELECT path_id, source, data_json, updated_at FROM path_metadata WHERE path_id=?",
        (dst_path_id,),
    ).fetchone()
    if dst is None:
        con.execute("UPDATE path_metadata SET path_id=? WHERE path_id=?", (dst_path_id, src_path_id))
        return "moved_src_to_dst"
    preferred = src if _metadata_rank(src) > _metadata_rank(dst) else dst
    if preferred["path_id"] == src_path_id:
        con.execute(
            "UPDATE path_metadata SET source=?, data_json=?, updated_at=? WHERE path_id=?",
            (src["source"], src["data_json"], src["updated_at"], dst_path_id),
        )
        action = "src_overwrote_dst"
    else:
        action = "kept_dst"
    con.execute("DELETE FROM path_metadata WHERE path_id=?", (src_path_id,))
    return action


def _repoint_observations(con, src_path_id: str, dst_path_id: str) -> int:
    rows = con.execute(
        "SELECT run_id, size_bytes, mtime_utc, type, name_flags FROM observations WHERE path_id=?",
        (src_path_id,),
    ).fetchall()
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO observations (run_id, path_id, size_bytes, mtime_utc, type, name_flags)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, path_id) DO UPDATE SET
          size_bytes=excluded.size_bytes,
          mtime_utc=excluded.mtime_utc,
          type=excluded.type,
          name_flags=excluded.name_flags
        """,
        [(r["run_id"], dst_path_id, r["size_bytes"], r["mtime_utc"], r["type"], r["name_flags"]) for r in rows],
    )
    con.execute("DELETE FROM observations WHERE path_id=?", (src_path_id,))
    return len(rows)


def _repoint_file_paths(con, src_path_id: str, dst_path_id: str) -> int:
    rows = con.execute(
        "SELECT file_id, is_current, first_seen_run_id, last_seen_run_id FROM file_paths WHERE path_id=?",
        (src_path_id,),
    ).fetchall()
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO file_paths (file_id, path_id, is_current, first_seen_run_id, last_seen_run_id)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(file_id, path_id) DO UPDATE SET
          is_current=MAX(file_paths.is_current, excluded.is_current),
          first_seen_run_id=COALESCE(file_paths.first_seen_run_id, excluded.first_seen_run_id),
          last_seen_run_id=COALESCE(excluded.last_seen_run_id, file_paths.last_seen_run_id)
        """,
        [(r["file_id"], dst_path_id, r["is_current"], r["first_seen_run_id"], r["last_seen_run_id"]) for r in rows],
    )
    con.execute("DELETE FROM file_paths WHERE path_id=?", (src_path_id,))
    return len(rows)


def _repoint_path_tags(con, src_path_id: str, dst_path_id: str) -> int:
    rows = con.execute(
        "SELECT tag_id, source, updated_at FROM path_tags WHERE path_id=?",
        (src_path_id,),
    ).fetchall()
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO path_tags (path_id, tag_id, source, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(path_id, tag_id, source) DO UPDATE SET
          updated_at=CASE
            WHEN excluded.updated_at > path_tags.updated_at THEN excluded.updated_at
            ELSE path_tags.updated_at
          END
        """,
        [(dst_path_id, r["tag_id"], r["source"], r["updated_at"]) for r in rows],
    )
    con.execute("DELETE FROM path_tags WHERE path_id=?", (src_path_id,))
    return len(rows)


def _repoint_events(con, src_path_id: str, dst_path_id: str) -> tuple[int, int]:
    cur1 = con.execute("UPDATE events SET src_path_id=? WHERE src_path_id=?", (dst_path_id, src_path_id))
    cur2 = con.execute("UPDATE events SET dst_path_id=? WHERE dst_path_id=?", (dst_path_id, src_path_id))
    return int(cur1.rowcount or 0), int(cur2.rowcount or 0)


def _merge_path_collision(con, src_path_id: str, dst_path_id: str, dst_full: str, dst_dir: str, dst_name: str, ts_now: str) -> dict:
    obs = _repoint_observations(con, src_path_id, dst_path_id)
    md_action = _merge_path_metadata(con, src_path_id, dst_path_id)
    fp = _repoint_file_paths(con, src_path_id, dst_path_id)
    tags = _repoint_path_tags(con, src_path_id, dst_path_id)
    ev_src, ev_dst = _repoint_events(con, src_path_id, dst_path_id)
    con.execute(
        "UPDATE paths SET path=?, dir=?, name=?, updated_at=? WHERE path_id=?",
        (dst_full, dst_dir, dst_name, ts_now, dst_path_id),
    )
    con.execute("DELETE FROM paths WHERE path_id=?", (src_path_id,))
    return {
        "observations_repointed": obs,
        "path_metadata_action": md_action,
        "file_paths_repointed": fp,
        "path_tags_repointed": tags,
        "events_repointed_src": ev_src,
        "events_repointed_dst": ev_dst,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--applied", required=True)
    ap.add_argument("--notes", default=None)
    ap.add_argument("--run-kind", default="apply")
    ap.add_argument("--event-kind", default="move")
    ap.add_argument("--detail-source", default="apply_move_plan.ps1")
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

    move_rows = []

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
        move_rows.append({"path_id": pid, "src": src, "dst": full, "dir": dir_, "name": name, "ts": rec.get("ts") or now_iso()})

    if args.dry_run:
        # Detect uniqueness collisions against current DB state so callers can preview merge work.
        collisions = 0
        same_path = 0
        missing_src = 0
        for row in move_rows:
            src_row = con.execute("SELECT path_id FROM paths WHERE path_id=?", (row["path_id"],)).fetchone()
            dst_row = con.execute("SELECT path_id FROM paths WHERE path=?", (row["dst"],)).fetchone()
            if src_row is None:
                missing_src += 1
            if dst_row is not None:
                if dst_row["path_id"] == row["path_id"]:
                    same_path += 1
                else:
                    collisions += 1
        con.close()
        print(
            json.dumps(
                {
                    "applied": args.applied,
                    "would_update": len(move_rows),
                    "would_events": len(move_rows),
                    "collisions_with_existing_dst_path": collisions,
                    "already_at_destination_path": same_path,
                    "missing_src_path_rows": missing_src,
                },
                ensure_ascii=False,
            )
        )
        return 0

    run_id = uuid.uuid4().hex
    started = now_iso()

    updated_count = 0
    merged_conflicts = 0
    already_applied = 0
    missing_src_rows = 0
    rows_events = []
    merge_details = []
    try:
        begin_immediate(con)
        con.execute(
            """
            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                str(args.run_kind or "apply"),
                None,
                started,
                None,
                None,
                args.notes or f"{str(args.run_kind or 'apply')} update_db_paths_from_move_apply {os.path.basename(args.applied)}",
            ),
        )
        for row in move_rows:
            pid = row["path_id"]
            dst_full = row["dst"]
            dst_dir = row["dir"]
            dst_name = row["name"]
            ts_now = now_iso()
            src_row = con.execute("SELECT path_id, path FROM paths WHERE path_id=?", (pid,)).fetchone()
            dst_row = con.execute("SELECT path_id, path FROM paths WHERE path=?", (dst_full,)).fetchone()
            event_path_id = pid
            event_detail = {"src": row["src"], "dst": dst_full, "source": str(args.detail_source or "apply_move_plan.ps1")}
            if src_row is None:
                if dst_row is not None:
                    # Idempotent rerun after previous DB merge/update success.
                    already_applied += 1
                    event_path_id = dst_row["path_id"]
                    event_detail["db_update"] = "already_applied_src_path_missing_dst_exists"
                else:
                    missing_src_rows += 1
                    event_path_id = None
                    event_detail["db_update"] = "src_path_id_missing_and_dst_not_found"
            elif dst_row is None or dst_row["path_id"] == pid:
                con.execute(
                    "UPDATE paths SET path=?, dir=?, name=?, updated_at=? WHERE path_id=?",
                    (dst_full, dst_dir, dst_name, ts_now, pid),
                )
                updated_count += 1
                if dst_row is not None and dst_row["path_id"] == pid:
                    event_detail["db_update"] = "already_at_destination_path"
                else:
                    event_detail["db_update"] = "updated_path_row"
            else:
                merge_info = _merge_path_collision(con, pid, dst_row["path_id"], dst_full, dst_dir, dst_name, ts_now)
                merged_conflicts += 1
                event_path_id = dst_row["path_id"]
                event_detail["db_update"] = "merged_into_existing_destination_path_row"
                event_detail["db_collision_with_path_id"] = dst_row["path_id"]
                event_detail["db_merge"] = merge_info
                merge_details.append({"src_path_id": pid, "dst_path_id": dst_row["path_id"], "dst": dst_full, **merge_info})

            rows_events.append(
                (
                    run_id,
                    row["ts"],
                    str(args.event_kind or "move"),
                    event_path_id,
                    None,
                    json.dumps(event_detail, ensure_ascii=False),
                    1,
                    None,
                )
            )

        if rows_events:
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

    print(
        json.dumps(
            {
                "applied": args.applied,
                "updated": updated_count,
                "merged_conflicts": merged_conflicts,
                "already_applied": already_applied,
                "missing_src_path_rows": missing_src_rows,
                "events": len(rows_events),
                "run_id": run_id,
                "run_kind": str(args.run_kind or "apply"),
                "event_kind": str(args.event_kind or "move"),
                "merge_details": merge_details[:20],
                "merge_details_truncated": len(merge_details) > 20,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
