#!/usr/bin/env python3
"""Detect rebroadcast candidates and classify with EPG rebroadcast flags.

This script groups recordings of the same episode (same normalized_program_key +
episode_no/subtitle). Classification rule priority:

1) If any member has broadcasts.data_json.is_rebroadcast_flag=true, those are
   "rebroadcast" and non-true members become "original".
2) If no true flag exists but at least one explicit false exists (EPG confirms
   not a rebroadcast), all members are "original".
3) If no EPG flags are available at all, all members are "unknown" to avoid
   date-only misclassification.

Rebroadcast candidates are linked in broadcast_groups / broadcast_group_members.
No files are deleted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from typing import Any

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall
from pathscan_common import now_iso


def safe_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def _build_episode_key(md: dict[str, Any]) -> str | None:
    """Build a grouping key from normalized_program_key + episode identifier."""
    npk = str(md.get("normalized_program_key") or "").strip()
    if not npk:
        return None
    ep = md.get("episode_no")
    sub = str(md.get("subtitle") or "").strip()
    if ep is not None and str(ep).strip():
        return f"{npk}::ep::{str(ep).strip()}"
    if sub:
        return f"{npk}::sub::{sub.lower()}"
    # No episode/subtitle — use air_date to distinguish
    air = str(md.get("air_date") or "").strip()
    if air:
        return f"{npk}::date::{air}"
    return None


def _stable_group_id(episode_key: str) -> str:
    """Deterministic group_id from episode_key for idempotency."""
    return hashlib.sha256(episode_key.encode("utf-8")).hexdigest()[:16]


def _extract_rebroadcast_flag(data_json_raw: Any) -> bool | None:
    if not data_json_raw:
        return None
    try:
        obj = json.loads(str(data_json_raw))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    v = obj.get("is_rebroadcast_flag")
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"true", "1", "yes"}:
            return True
        if s in {"false", "0", "no"}:
            return False
    return None


def _classify_group(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flags = [e.get("is_rebroadcast_flag") for e in entries]
    has_true = any(f is True for f in flags)
    has_known = any(f is not None for f in flags)

    classified: list[dict[str, Any]] = []
    for e in entries:
        if has_true:
            btype = "rebroadcast" if e.get("is_rebroadcast_flag") is True else "original"
        elif has_known:
            btype = "original"
        else:
            btype = "unknown"
        classified.append({**e, "broadcast_type": btype})
    return classified


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-groups", type=int, default=0)
    args = ap.parse_args()

    con = connect_db(args.db)
    create_schema_if_needed(con)

    errors: list[str] = []
    try:
        # Load EPG rebroadcast flags via path_programs -> broadcasts
        epg_rows = fetchall(
            con,
            """
            SELECT pp.path_id, b.data_json
            FROM path_programs pp
            JOIN broadcasts b ON b.broadcast_id = pp.broadcast_id
            WHERE pp.broadcast_id IS NOT NULL
            """,
            (),
        )
        path_rebroadcast_flag: dict[str, bool | None] = {}
        for r in epg_rows:
            pid = str(r["path_id"])
            flag = _extract_rebroadcast_flag(r["data_json"])
            if flag is True:
                path_rebroadcast_flag[pid] = True
            elif pid not in path_rebroadcast_flag:
                path_rebroadcast_flag[pid] = flag

        # Load all non-EPG metadata for grouping keys
        md_rows = fetchall(
            con,
            """
            SELECT pm.path_id, pm.data_json, p.path
            FROM path_metadata pm
            JOIN paths p ON p.path_id = pm.path_id
            WHERE pm.source != 'edcb_epg'
            """,
            (),
        )

        # Group by episode key
        grouped: dict[str, list[dict[str, Any]]] = {}
        skipped_no_key = 0
        for r in md_rows:
            path_id = str(r["path_id"])
            try:
                md = json.loads(str(r["data_json"]))
            except Exception:
                errors.append(f"invalid metadata json: path_id={path_id}")
                continue
            if not isinstance(md, dict):
                continue
            ep_key = _build_episode_key(md)
            if not ep_key:
                skipped_no_key += 1
                continue
            entry = {
                "path_id": path_id,
                "path": str(r["path"]),
                "program_title": str(md.get("program_title") or ""),
                "air_date": str(md.get("air_date") or ""),
                "broadcaster": md.get("broadcaster") or md.get("channel") or None,
                "episode_key": ep_key,
                "metadata": md,
                "is_rebroadcast_flag": path_rebroadcast_flag.get(path_id),
            }
            grouped.setdefault(ep_key, []).append(entry)

        # Filter to groups with 2+ members (potential rebroadcasts)
        multi_groups = {k: v for k, v in grouped.items() if len(v) >= 2}

        # Further filter: only groups where air_date OR broadcaster differs
        rebroadcast_groups: dict[str, list[dict[str, Any]]] = {}
        for ep_key, entries in multi_groups.items():
            dates = {e["air_date"] for e in entries if e["air_date"]}
            broadcasters = {e["broadcaster"] for e in entries if e["broadcaster"]}
            if len(dates) > 1 or len(broadcasters) > 1:
                rebroadcast_groups[ep_key] = entries

        group_keys = sorted(rebroadcast_groups.keys())
        max_groups = max(0, int(args.max_groups or 0))
        if max_groups > 0:
            group_keys = group_keys[:max_groups]

        plan_rows: list[dict[str, Any]] = []
        groups_processed = 0
        members_total = 0

        for ep_key in group_keys:
            entries = rebroadcast_groups[ep_key]
            entries_sorted = sorted(entries, key=lambda e: (e["air_date"] or "9999", e["path_id"]))
            entries_classified = _classify_group(entries_sorted)
            group_id = _stable_group_id(ep_key)
            program_title = entries_sorted[0]["program_title"]

            group_plan: list[dict[str, Any]] = []
            for entry in entries_classified:
                member = {
                    "group_id": group_id,
                    "path_id": entry["path_id"],
                    "path": entry["path"],
                    "broadcast_type": entry["broadcast_type"],
                    "air_date": entry["air_date"] or None,
                    "broadcaster": entry["broadcaster"],
                    "program_title": program_title,
                    "episode_key": ep_key,
                    "is_rebroadcast_flag": entry["is_rebroadcast_flag"],
                }
                group_plan.append(member)
                members_total += 1
            plan_rows.extend(group_plan)
            groups_processed += 1

        # Apply to DB if requested
        db_inserted_groups = 0
        db_inserted_members = 0
        if args.apply and plan_rows:
            try:
                begin_immediate(con)
                for ep_key in group_keys:
                    entries = rebroadcast_groups[ep_key]
                    entries_sorted = sorted(entries, key=lambda e: (e["air_date"] or "9999", e["path_id"]))
                    entries_classified = _classify_group(entries_sorted)
                    group_id = _stable_group_id(ep_key)
                    program_title = entries_sorted[0]["program_title"]

                    # Upsert group
                    con.execute(
                        """
                        INSERT INTO broadcast_groups (group_id, program_title, episode_key, created_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(group_id) DO UPDATE SET
                          program_title = excluded.program_title,
                          episode_key = excluded.episode_key
                        """,
                        (group_id, program_title, ep_key, now_iso()),
                    )
                    db_inserted_groups += 1

                    for entry in entries_classified:
                        con.execute(
                            """
                            INSERT INTO broadcast_group_members
                              (group_id, path_id, broadcast_type, air_date, broadcaster, added_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(group_id, path_id) DO UPDATE SET
                              broadcast_type = excluded.broadcast_type,
                              air_date = excluded.air_date,
                              broadcaster = excluded.broadcaster,
                              added_at = excluded.added_at
                            """,
                            (
                                group_id,
                                entry["path_id"],
                                entry["broadcast_type"],
                                entry["air_date"] or None,
                                entry["broadcaster"],
                                now_iso(),
                            ),
                        )
                        db_inserted_members += 1
                con.commit()
            except Exception as e:
                con.rollback()
                errors.append(f"DB write failed: {e}")

        summary = {
            "ok": len(errors) == 0,
            "tool": "video_pipeline_detect_rebroadcasts",
            "apply": bool(args.apply),
            "totalMetadataRows": len(md_rows),
            "skippedNoKey": skipped_no_key,
            "multiEpisodeGroups": len(multi_groups),
            "rebroadcastGroups": len(rebroadcast_groups),
            "groupsProcessed": groups_processed,
            "membersTotal": members_total,
            "dbInsertedGroups": db_inserted_groups,
            "dbInsertedMembers": db_inserted_members,
            "plan": plan_rows[:200],  # Cap output for readability
            "errors": errors,
        }
        print(safe_json(summary))
        return 0 if summary["ok"] else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
