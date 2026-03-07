#!/usr/bin/env python3
"""Detect rebroadcasts and group same-episode recordings by air_date/broadcaster.

This script groups recordings of the same episode (same normalized_program_key +
episode_no/subtitle) and identifies original/rebroadcast/unknown entries based
on EPG rebroadcast flags from broadcasts.data_json. Rebroadcasts are NOT
deleted — they are linked in the broadcast_groups / broadcast_group_members
tables.
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


def _parse_rebroadcast_flag(data_json: Any) -> bool | None:
    """Extract is_rebroadcast_flag from broadcasts.data_json if available."""
    if data_json is None:
        return None
    try:
        payload = json.loads(str(data_json))
    except Exception:
        return None
    if not isinstance(payload, dict) or "is_rebroadcast_flag" not in payload:
        return None
    flag = payload.get("is_rebroadcast_flag")
    if isinstance(flag, bool):
        return flag
    if isinstance(flag, (int, float)):
        return bool(flag)
    if isinstance(flag, str):
        normalized = flag.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _decide_broadcast_types(entries: list[dict[str, Any]]) -> dict[str, str]:
    """Decide broadcast_type per path_id with EPG-first 3-stage rules.

    Rule priority:
    1) Any is_rebroadcast_flag=true -> that member is rebroadcast.
    2) If EPG flags exist but none true -> all members original.
    3) If no EPG flags are available -> all members unknown.
    """
    flags = [e.get("is_rebroadcast_flag") for e in entries]
    has_true = any(flag is True for flag in flags)
    has_known = any(flag is not None for flag in flags)

    if has_true:
        return {
            str(e["path_id"]): ("rebroadcast" if e.get("is_rebroadcast_flag") is True else "original")
            for e in entries
        }
    if has_known:
        return {str(e["path_id"]): "original" for e in entries}
    return {str(e["path_id"]): "unknown" for e in entries}


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
        # Load all LLM metadata
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

        # Load path_id -> EPG rebroadcast flag (if any) via path_programs/broadcasts.
        epg_flag_by_path: dict[str, bool | None] = {}
        epg_rows = fetchall(
            con,
            """
            SELECT pp.path_id, b.data_json, pp.updated_at
            FROM path_programs pp
            LEFT JOIN broadcasts b ON b.broadcast_id = pp.broadcast_id
            ORDER BY pp.path_id ASC, pp.updated_at DESC
            """,
            (),
        )
        for r in epg_rows:
            path_id = str(r["path_id"])
            parsed_flag = _parse_rebroadcast_flag(r["data_json"])
            if path_id not in epg_flag_by_path:
                epg_flag_by_path[path_id] = parsed_flag
                continue
            # Prefer known values over unknown when multiple path_program rows exist.
            if epg_flag_by_path[path_id] is None and parsed_flag is not None:
                epg_flag_by_path[path_id] = parsed_flag

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
                "is_rebroadcast_flag": epg_flag_by_path.get(path_id),
                "metadata": md,
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
            entries_sorted = sorted(entries, key=lambda e: e["air_date"] or "9999")
            type_by_path = _decide_broadcast_types(entries_sorted)
            group_id = _stable_group_id(ep_key)
            program_title = entries_sorted[0]["program_title"]

            group_plan: list[dict[str, Any]] = []
            for entry in entries_sorted:
                btype = type_by_path.get(str(entry["path_id"]), "unknown")
                member = {
                    "group_id": group_id,
                    "path_id": entry["path_id"],
                    "path": entry["path"],
                    "broadcast_type": btype,
                    "air_date": entry["air_date"] or None,
                    "broadcaster": entry["broadcaster"],
                    "program_title": program_title,
                    "episode_key": ep_key,
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
                    entries_sorted = sorted(entries, key=lambda e: e["air_date"] or "9999")
                    type_by_path = _decide_broadcast_types(entries_sorted)
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

                    for entry in entries_sorted:
                        btype = type_by_path.get(str(entry["path_id"]), "unknown")
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
                            (group_id, entry["path_id"], btype, entry["air_date"] or None, entry["broadcaster"], now_iso()),
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
