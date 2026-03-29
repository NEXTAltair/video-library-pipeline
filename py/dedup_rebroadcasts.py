#!/usr/bin/env python3
r"""Detect and quarantine same-episode recordings from different channels/dates.

Groups recordings by normalized_program_key + episode_no (or subtitle fallback),
then isolates lower-quality copies to a quarantine directory.

Run order: czkawka hash dedup -> dedup_recordings -> this tool.
"""

from __future__ import annotations

import argparse
import re
import uuid
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import Any

from db_helpers import reconstruct_path_metadata
from dedup_common import (
    build_group_key,
    choose_keep,
    classify_broadcast_bucket,
    load_bucket_rules,
    normalize_subtitle,
    parse_confidence,
    parse_resolution_score,
    safe_group_key,
)
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall
from move_apply_stats import aggregate_move_apply
from pathscan_common import (
    as_bool,
    canonicalize_windows_path,
    iter_jsonl,
    now_iso,
    safe_json,
    split_win,
    ts_compact,
    windows_to_wsl_path,
    wsl_to_windows_path,
)
from windows_pwsh_bridge import run_pwsh_json


_FILENAME_DATETIME_RE = re.compile(
    r"(\d{4})[_\s](\d{2})[_\s](\d{2})[_\s](\d{2})[_\s](\d{2})(?:[_\s]\d{2})?(?:\.\w+)?$"
)


def extract_filename_datetime(path: str) -> str | None:
    """ファイル名末尾の録画日時を抽出する (例: '2026 03 24 21 00')。"""
    name = PureWindowsPath(path).stem
    m = _FILENAME_DATETIME_RE.search(name)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}"


@dataclass
class Candidate:
    path_id: str
    path: str
    group_key: str
    confidence: float
    needs_review: bool
    program_title: str
    air_date: str | None
    broadcaster: str | None
    is_rebroadcast_flag: bool | None
    episode_no: str | None
    subtitle: str | None
    bucket: str
    bucket_reason: str
    size_bytes: int
    mtime_ts: float
    resolution_score: int
    not_corrupt: int
    filename_datetime: str | None
    raw_meta: dict[str, Any]


def _load_epg_data(con) -> tuple[dict[str, bool | None], dict[str, str]]:
    """path_id -> (is_rebroadcast_flag, broadcaster) の2つのマップを返す。

    path_programs + broadcasts を JOIN して取得。
    is_rebroadcast_flag: 1件でも True があれば True、全部 False なら False、なければ None。
    broadcaster: broadcasts.broadcaster の最初の非 NULL 値（path_metadata.broadcaster 補完用）。
    """
    rows = fetchall(
        con,
        """
        SELECT pp.path_id,
               MAX(CASE WHEN b.is_rebroadcast_flag = 1 THEN 1 ELSE 0 END) AS has_rebroadcast,
               MIN(CASE WHEN b.is_rebroadcast_flag = 0 THEN 0 ELSE 1 END) AS all_nonzero,
               MAX(b.broadcaster) AS broadcaster
        FROM path_programs pp
        JOIN broadcasts b ON b.broadcast_id = pp.broadcast_id
        WHERE b.is_rebroadcast_flag IS NOT NULL OR b.broadcaster IS NOT NULL
        GROUP BY pp.path_id
        """,
        (),
    )
    flags: dict[str, bool | None] = {}
    broadcasters: dict[str, str] = {}
    for r in rows:
        pid = str(r["path_id"])
        if r["has_rebroadcast"]:
            flags[pid] = True
        elif r["all_nonzero"] == 0:
            flags[pid] = False
        if r["broadcaster"]:
            broadcasters[pid] = str(r["broadcaster"])
    return flags, broadcasters


def _apply_program_title_filter(rows: list, contains: str) -> list:
    needle = contains.strip().lower()
    if not needle:
        return rows
    return [r for r in rows if needle in str(r.get("program_title") or "").lower()]


def _apply_genre_filter(rows: list, contains: str) -> list:
    needle = contains.strip().lower()
    if not needle:
        return rows
    out = []
    for r in rows:
        import json as _json
        try:
            dj = _json.loads(r["data_json"] or "{}")
        except Exception:
            dj = {}
        genre = str(dj.get("genre") or "").lower()
        if needle in genre:
            out.append(r)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-groups", type=int, default=0)
    ap.add_argument("--confidence-threshold", type=float, default=0.85)
    ap.add_argument("--allow-needs-review", default="false")
    ap.add_argument("--keep-terrestrial-and-bscs", default="true")
    ap.add_argument("--bucket-rules-path", default="")
    ap.add_argument("--program-title-contains", default="")
    ap.add_argument("--genre-contains", default="")
    args = ap.parse_args()

    import os
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(args.windows_ops_root).resolve()
    move_dir = ops_root / "move"
    quarantine_root = ops_root / "duplicates" / "rebroadcast_quarantine"
    move_dir.mkdir(parents=True, exist_ok=True)
    quarantine_root.mkdir(parents=True, exist_ok=True)

    bucket_rules_path = (
        Path(args.bucket_rules_path)
        if args.bucket_rules_path.strip()
        else Path(__file__).resolve().parent.parent / "rules" / "broadcast_buckets.yaml"
    )
    bucket_rules = load_bucket_rules(bucket_rules_path)

    allow_needs_review = as_bool(args.allow_needs_review, False)
    keep_terrestrial_and_bscs = as_bool(args.keep_terrestrial_and_bscs, True)
    max_groups = max(0, int(args.max_groups or 0))

    con = connect_db(args.db)
    create_schema_if_needed(con)

    ts = ts_compact()
    plan_path = move_dir / f"rebroadcast_dedup_plan_{ts}.jsonl"
    apply_path = move_dir / f"rebroadcast_dedup_apply_{ts}.jsonl"
    errors: list[str] = []
    move_apply_file: Path | None = None

    try:
        rebroadcast_flags, epg_broadcasters = _load_epg_data(con)

        md_rows = fetchall(
            con,
            """
            SELECT pm.path_id, pm.data_json, p.path,
                   pm.program_title, pm.air_date, pm.needs_review,
                   pm.episode_no, pm.subtitle,
                   pm.broadcaster, pm.human_reviewed
            FROM path_metadata pm
            JOIN paths p ON p.path_id = pm.path_id
            WHERE pm.source != 'edcb_epg'
            """,
            (),
        )

        if args.program_title_contains:
            md_rows = _apply_program_title_filter(list(md_rows), args.program_title_contains)
        if args.genre_contains:
            md_rows = _apply_genre_filter(list(md_rows), args.genre_contains)

        grouped: dict[str, list[Candidate]] = {}
        dropped_for_missing_key = 0

        for r in md_rows:
            path_id = str(r["path_id"])
            path_val = canonicalize_windows_path(str(r["path"]))
            md = reconstruct_path_metadata(r)
            group_key, reason = build_group_key(md)
            if not group_key:
                dropped_for_missing_key += 1
                continue

            conf = parse_confidence(md.get("confidence"))
            needs_review = bool(md.get("needs_review"))
            # path_metadata.broadcaster が未設定の場合 broadcasts テーブルから補完
            epg_broadcaster = epg_broadcasters.get(path_id)
            effective_broadcaster = md.get("broadcaster") or md.get("channel") or epg_broadcaster or ""
            bucket, bucket_reason = classify_broadcast_bucket(
                {"path": path_val, "broadcaster": effective_broadcaster, **md},
                bucket_rules,
            )
            path_wsl = Path(windows_to_wsl_path(path_val))
            size_bytes = 0
            mtime_ts = 0.0
            not_corrupt = 0
            try:
                st = path_wsl.stat()
                size_bytes = int(st.st_size)
                mtime_ts = float(st.st_mtime)
                not_corrupt = 1 if size_bytes > 0 else 0
            except Exception:
                not_corrupt = 0

            c = Candidate(
                path_id=path_id,
                path=path_val,
                group_key=group_key,
                confidence=conf,
                needs_review=needs_review,
                program_title=str(md.get("program_title") or ""),
                air_date=str(md.get("air_date")) if md.get("air_date") else None,
                broadcaster=effective_broadcaster or None,
                is_rebroadcast_flag=rebroadcast_flags.get(path_id),
                episode_no=str(md.get("episode_no")) if md.get("episode_no") is not None else None,
                subtitle=str(md.get("subtitle")) if md.get("subtitle") is not None else None,
                bucket=bucket,
                bucket_reason=bucket_reason,
                size_bytes=size_bytes,
                mtime_ts=mtime_ts,
                resolution_score=parse_resolution_score(md.get("resolution")),
                not_corrupt=not_corrupt,
                filename_datetime=extract_filename_datetime(path_val),
                raw_meta=md,
            )
            grouped.setdefault(group_key, []).append(c)

        group_keys = [k for k, arr in grouped.items() if len(arr) > 1]
        group_keys.sort()
        if max_groups > 0:
            group_keys = group_keys[:max_groups]

        rows_plan: list[dict[str, Any]] = []
        rows_drop: list[dict[str, Any]] = []
        groups_auto_processed = 0
        groups_manual_review = 0
        groups_split_by_broadcast = 0
        files_kept = 0
        files_kept_by_broadcast_policy = 0
        files_dropped = 0

        def _make_row(c: Candidate, decision: str, reason: str) -> dict[str, Any]:
            return {
                "group_key": c.group_key,
                "path_id": c.path_id,
                "path": c.path,
                "decision": decision,
                "reason": reason,
                "bucket": c.bucket,
                "bucket_reason": c.bucket_reason,
                "confidence": c.confidence,
                "needs_review": c.needs_review,
                "air_date": c.air_date,
                "broadcaster": c.broadcaster,
                "is_rebroadcast_flag": c.is_rebroadcast_flag,
                "size_bytes": c.size_bytes,
                "resolution_score": c.resolution_score,
                "filename_datetime": c.filename_datetime,
                "ts": now_iso(),
            }

        for gk in group_keys:
            arr = grouped[gk]
            auto_eligible = [
                x for x in arr
                if (allow_needs_review or not x.needs_review) and x.confidence >= args.confidence_threshold
            ]
            if len(auto_eligible) < 2:
                groups_manual_review += 1
                for x in arr:
                    rows_plan.append(_make_row(x, "manual_review_required", "low_confidence_or_needs_review"))
                continue

            cohorts: list[tuple[str, list[Candidate]]] = []
            if keep_terrestrial_and_bscs:
                by_bucket: dict[str, list[Candidate]] = {"terrestrial": [], "bs_cs": [], "unknown": []}
                for x in auto_eligible:
                    by_bucket.setdefault(x.bucket, []).append(x)
                if by_bucket["unknown"] and (by_bucket["terrestrial"] or by_bucket["bs_cs"]):
                    groups_manual_review += 1
                    for x in arr:
                        rows_plan.append(_make_row(x, "manual_review_required", "unknown_bucket_mixed"))
                    continue
                if by_bucket["terrestrial"] and by_bucket["bs_cs"]:
                    groups_split_by_broadcast += 1
                    files_kept_by_broadcast_policy += 1
                for b in ("terrestrial", "bs_cs", "unknown"):
                    if by_bucket[b]:
                        cohorts.append((b, by_bucket[b]))
            else:
                cohorts.append(("all", auto_eligible))

            group_has_drop = False
            for bucket_name, cohort in cohorts:
                if len(cohort) == 1:
                    files_kept += 1
                    rows_plan.append(_make_row(cohort[0], "keep", f"single_in_bucket:{bucket_name}"))
                    continue
                keep = choose_keep(cohort)
                files_kept += 1
                rows_plan.append(_make_row(keep, "keep", f"best_ranked:{bucket_name}"))
                for x in cohort:
                    if x.path_id == keep.path_id:
                        continue
                    group_has_drop = True
                    files_dropped += 1
                    row = _make_row(x, "drop", f"lower_rank_in_bucket:{bucket_name}")
                    rows_plan.append(row)
                    rows_drop.append(row)

            if group_has_drop:
                groups_auto_processed += 1

        with plan_path.open("w", encoding="utf-8") as w:
            w.write(
                safe_json({
                    "_meta": {
                        "kind": "rebroadcast_dedup_plan",
                        "generated_at": now_iso(),
                        "db": args.db,
                        "groups_total": len(group_keys),
                        "allow_needs_review": allow_needs_review,
                        "confidence_threshold": args.confidence_threshold,
                        "keep_terrestrial_and_bscs": keep_terrestrial_and_bscs,
                        "bucket_rules_path": str(bucket_rules_path),
                        "dropped_for_missing_key": dropped_for_missing_key,
                        "program_title_contains": args.program_title_contains or None,
                        "genre_contains": args.genre_contains or None,
                    }
                }) + "\n"
            )
            for row in rows_plan:
                w.write(safe_json(row) + "\n")

        apply_rows: list[dict[str, Any]] = []
        run_id: str | None = None
        files_moved = 0
        move_backend = "wsl_shutil_move"

        if args.apply:
            move_backend = "pwsh7_apply_move_plan"
            scripts_root = ops_root / "scripts"
            apply_move_script = scripts_root / "apply_move_plan.ps1"
            if not apply_move_script.exists():
                errors.append(f"apply_move_plan.ps1 not found: {apply_move_script}")
            else:
                ops_root_win = wsl_to_windows_path(str(ops_root))
                quarantine_root_win = wsl_to_windows_path(str(quarantine_root))
                internal_move_plan = move_dir / f"rebroadcast_dedup_move_plan_internal_{ts}.jsonl"
                drop_by_path_id = {str(r["path_id"]): r for r in rows_drop if r.get("path_id")}

                with internal_move_plan.open("w", encoding="utf-8") as w:
                    w.write(
                        safe_json({
                            "_meta": {
                                "kind": "rebroadcast_dedup_move_plan_internal",
                                "generated_at": now_iso(),
                                "source": "dedup_rebroadcasts.py",
                                "rows": len(rows_drop),
                            }
                        }) + "\n"
                    )
                    for row in rows_drop:
                        src_win = canonicalize_windows_path(str(row["path"]))
                        group_dir_win = canonicalize_windows_path(
                            quarantine_root_win + "\\" + safe_group_key(str(row["group_key"]))
                        )
                        base_name = PureWindowsPath(src_win).name
                        dst_win = canonicalize_windows_path(group_dir_win + "\\" + base_name)
                        w.write(
                            safe_json({
                                "op": "move",
                                "path_id": row["path_id"],
                                "src": src_win,
                                "dst": dst_win,
                            }) + "\n"
                        )

                try:
                    apply_meta = run_pwsh_json(
                        str(apply_move_script),
                        [
                            "-PlanJsonl",
                            wsl_to_windows_path(str(internal_move_plan)),
                            "-OpsRoot",
                            ops_root_win,
                            "-OnDstExists",
                            "rename_suffix",
                        ],
                    )
                    out_jsonl = str(apply_meta.get("out_jsonl") or "").strip()
                    if not out_jsonl:
                        raise RuntimeError("apply_move_plan.ps1 did not return out_jsonl")
                    move_apply_file = Path(windows_to_wsl_path(out_jsonl))
                    if not move_apply_file.exists():
                        raise RuntimeError(f"move apply JSONL not found: {move_apply_file}")
                except Exception as e:
                    errors.append(f"rebroadcast dedup apply move engine failed: {e}")

                if move_apply_file is not None:
                    run_id = str(uuid.uuid4())
                    try:
                        begin_immediate(con)
                        con.execute(
                            """
                            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                run_id,
                                "dedup_rebroadcasts",
                                str(quarantine_root),
                                now_iso(),
                                None,
                                "dedup_rebroadcasts.py",
                                f"groups={len(group_keys)} drops={len(rows_drop)}",
                            ),
                        )

                        for rec in iter_jsonl(str(move_apply_file)):
                            if rec.get("op") != "move":
                                continue
                            pid = str(rec.get("path_id") or "")
                            src_win = canonicalize_windows_path(str(rec.get("src") or ""))
                            dst_win_val = canonicalize_windows_path(str(rec.get("dst") or ""))
                            ok = bool(rec.get("ok"))
                            err_text = None if ok else str(rec.get("error") or "")
                            src_row = drop_by_path_id.get(pid, {})
                            group_key = src_row.get("group_key")

                            if ok and pid and dst_win_val:
                                drive, dir_, name, ext = split_win(dst_win_val)
                                con.execute(
                                    "UPDATE paths SET path=?, drive=?, dir=?, name=?, ext=?, updated_at=? WHERE path_id=?",
                                    (dst_win_val, drive, dir_, name, ext, now_iso(), pid),
                                )
                                con.execute(
                                    """
                                    INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        run_id, now_iso(), "rebroadcast_dedup_move",
                                        pid, None,
                                        safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}),
                                        1, None,
                                    ),
                                )
                                files_moved += 1
                            else:
                                if src_win or dst_win_val or pid:
                                    errors.append(
                                        f"move failed: {src_win or '(empty)'} -> {dst_win_val or '(empty)'} :: {err_text or 'unknown_error'}"
                                    )
                                if pid:
                                    con.execute(
                                        """
                                        INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            run_id, now_iso(), "rebroadcast_dedup_move",
                                            pid, None,
                                            safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}),
                                            0, err_text or "move_failed",
                                        ),
                                    )

                            apply_rows.append({
                                "group_key": group_key,
                                "path_id": pid or None,
                                "src": src_win,
                                "dst": dst_win_val,
                                "ok": ok,
                                "error": err_text,
                                "ts": str(rec.get("ts") or now_iso()),
                            })

                        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
                        con.commit()
                    except Exception:
                        con.rollback()
                        raise

            with apply_path.open("w", encoding="utf-8") as w:
                w.write(
                    safe_json({
                        "_meta": {
                            "kind": "rebroadcast_dedup_apply",
                            "generated_at": now_iso(),
                            "run_id": run_id,
                            "rows": len(apply_rows),
                        }
                    }) + "\n"
                )
                for row in apply_rows:
                    w.write(safe_json(row) + "\n")

        move_apply_stats = aggregate_move_apply(str(move_apply_file)) if move_apply_file else None

        summary = {
            "ok": len(errors) == 0,
            "tool": "video_pipeline_dedup_rebroadcasts",
            "apply": bool(args.apply),
            "planPath": str(plan_path),
            "applyPath": str(apply_path) if args.apply else None,
            "groupsTotal": len(group_keys),
            "groupsAutoProcessed": groups_auto_processed,
            "groupsManualReview": groups_manual_review,
            "groupsSplitByBroadcast": groups_split_by_broadcast,
            "filesKept": files_kept,
            "filesKeptByBroadcastPolicy": files_kept_by_broadcast_policy,
            "filesDropped": files_dropped,
            "filesMoved": files_moved,
            "moveBackend": move_backend,
            "moveApplyStats": move_apply_stats,
            "errors": errors,
        }
        print(safe_json(summary))
        return 0 if summary["ok"] else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
