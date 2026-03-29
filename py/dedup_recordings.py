#!/usr/bin/env python3
r"""Detect and isolate duplicate recordings.

This script reads latest LLM metadata from DB, groups duplicate episode candidates,
and outputs dedup plan/apply artifacts.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path, PureWindowsPath
from typing import Any

from db_helpers import reconstruct_path_metadata
from dedup_common import (
    Candidate,
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


FILE_NAMESPACE = uuid.UUID("88e78892-867e-4e54-b432-2f251e75ac9f")


def unique_dst_path(dst: Path) -> Path:
    if not dst.exists():
        return dst
    stem = dst.stem
    suf = dst.suffix
    parent = dst.parent
    for i in range(1, 10000):
        p = parent / f"{stem}__dup{i}{suf}"
        if not p.exists():
            return p
    raise RuntimeError(f"failed to resolve unique dst path: {dst}")


def load_hash_groups(path: str) -> list[frozenset[str]]:
    """czkawka compact JSON を読んでハッシュ一致グループのリストを返す。

    compact JSON 形式: {"<size>": [[{path, size, ...}, ...], ...], ...}
    各サブ配列がバイト一致グループ。path は Windows スタイル (B:\\...) の場合がある。
    """
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    groups: list[frozenset[str]] = []
    for size_key, size_val in data.items():
        if not isinstance(size_val, list):
            continue
        for group in size_val:
            if not isinstance(group, list) or len(group) < 2:
                continue
            paths: set[str] = set()
            for entry in group:
                if not isinstance(entry, dict):
                    continue
                p = str(entry.get("path") or "").strip()
                if p:
                    paths.add(canonicalize_windows_path(p))
            if len(paths) >= 2:
                groups.append(frozenset(paths))
    return groups


def load_hash_entries_from_raw(raw_json_path: str) -> list[dict[str, Any]]:
    """czkawka raw JSON からハッシュ付きエントリを読み込む。"""
    if not raw_json_path or not os.path.exists(raw_json_path):
        return []
    try:
        with open(raw_json_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []

    entries: list[dict[str, Any]] = []
    for size_val in data.values():
        if not isinstance(size_val, list):
            continue
        for group in size_val:
            if not isinstance(group, list):
                continue
            for entry in group:
                if not isinstance(entry, dict):
                    continue
                win_path = canonicalize_windows_path(str(entry.get("path") or "").strip())
                hash_val = str(entry.get("hash") or "").strip()
                if not win_path or not hash_val:
                    continue
                try:
                    size_bytes = int(entry.get("size") or 0)
                except Exception:
                    size_bytes = 0
                entries.append({"path": win_path, "size": max(0, size_bytes), "hash": hash_val})
    return entries


def store_hash_entries_to_files_table(
    con,
    entries: list[dict[str, Any]],
    path_to_path_id: dict[str, str],
) -> int:
    if not entries:
        return 0
    written = 0
    ts = now_iso()
    for entry in entries:
        win_path = canonicalize_windows_path(str(entry.get("path") or ""))
        path_id = path_to_path_id.get(win_path)
        hash_val = str(entry.get("hash") or "").strip()
        if not path_id or not hash_val:
            continue
        try:
            size_bytes = int(entry.get("size") or 0)
        except Exception:
            size_bytes = 0
        file_id = str(uuid.uuid5(FILE_NAMESPACE, f"path:{path_id}"))
        con.execute(
            """
            INSERT INTO files (file_id, size_bytes, content_hash, hash_algo, created_at, updated_at)
            VALUES (?, ?, ?, 'BLAKE3', ?, ?)
            ON CONFLICT(file_id) DO UPDATE SET
              size_bytes=excluded.size_bytes,
              content_hash=excluded.content_hash,
              hash_algo=excluded.hash_algo,
              updated_at=excluded.updated_at
            """,
            (file_id, max(0, size_bytes), hash_val, ts, ts),
        )
        con.execute(
            """
            INSERT INTO file_paths (file_id, path_id, is_current)
            VALUES (?, ?, 1)
            ON CONFLICT(file_id, path_id) DO UPDATE SET is_current=1
            """,
            (file_id, path_id),
        )
        written += 1
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-groups", type=int, default=0)
    ap.add_argument("--keep-terrestrial-and-bscs", default="true")
    ap.add_argument("--bucket-rules-path", default="")
    ap.add_argument("--hash-scan-path", default="", help="czkawka compact JSON path for hash-exact dedup pre-pass")
    ap.add_argument("--hash-raw-json", default="", help="czkawka raw JSON path (with BLAKE3 hash values)")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(args.windows_ops_root).resolve()
    move_dir = ops_root / "move"
    quarantine_root = ops_root / "duplicates" / "quarantine"
    move_dir.mkdir(parents=True, exist_ok=True)
    quarantine_root.mkdir(parents=True, exist_ok=True)

    bucket_rules_path = (
        Path(args.bucket_rules_path)
        if args.bucket_rules_path.strip()
        else Path(__file__).resolve().parent.parent / "rules" / "broadcast_buckets.yaml"
    )
    bucket_rules = load_bucket_rules(bucket_rules_path)

    keep_terrestrial_and_bscs = as_bool(args.keep_terrestrial_and_bscs, True)
    max_groups = max(0, int(args.max_groups or 0))

    con = connect_db(args.db)
    create_schema_if_needed(con)

    hash_entries = load_hash_entries_from_raw(args.hash_raw_json)
    files_hash_rows_upserted = 0
    if hash_entries:
        all_paths = fetchall(con, "SELECT path_id, path FROM paths", ())
        path_to_path_id = {canonicalize_windows_path(str(r["path"])): str(r["path_id"]) for r in all_paths}
        begin_immediate(con)
        try:
            files_hash_rows_upserted = store_hash_entries_to_files_table(con, hash_entries, path_to_path_id)
            con.commit()
        except Exception:
            con.rollback()
            raise

    ts = ts_compact()
    plan_path = move_dir / f"dedup_plan_{ts}.jsonl"
    apply_path = move_dir / f"dedup_apply_{ts}.jsonl"
    errors: list[str] = []

    try:
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
            bucket, bucket_reason = classify_broadcast_bucket(
                {"path": path_val, **md},
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
                episode_no=str(md.get("episode_no")) if md.get("episode_no") is not None else None,
                subtitle=str(md.get("subtitle")) if md.get("subtitle") is not None else None,
                bucket=bucket,
                bucket_reason=bucket_reason,
                size_bytes=size_bytes,
                mtime_ts=mtime_ts,
                resolution_score=parse_resolution_score(md.get("resolution")),
                not_corrupt=not_corrupt,
                raw_meta=md,
            )
            grouped.setdefault(group_key, []).append(c)

        # path -> Candidate インデックス（ハッシュ方式での検索用）
        path_to_candidate: dict[str, Candidate] = {}
        for candidates_list in grouped.values():
            for c in candidates_list:
                path_to_candidate[c.path] = c

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

        # --- Phase 1: czkawka ハッシュ一致グループによる優先 dedup ---
        hash_claimed_paths: set[str] = set()
        hash_groups_total = 0
        hash_groups_processed = 0
        hash_group_details: list[dict[str, Any]] = []

        hash_groups = load_hash_groups(args.hash_scan_path)
        for hg in hash_groups:
            hash_groups_total += 1
            candidates = [path_to_candidate[p] for p in hg if p in path_to_candidate]
            if len(candidates) < 2:
                continue
            # keep/drop 判定（confidence・needs_review チェックをスキップ: バイト一致は確実）
            keep = choose_keep(candidates)
            hash_groups_processed += 1
            hash_group_details.append({
                "group_key": keep.group_key or f"hash::{keep.path_id}",
                "keep": 1,
                "drop": len(candidates) - 1,
            })
            groups_auto_processed += 1
            files_kept += 1
            rows_plan.append(
                {
                    "group_key": f"hash::{keep.path_id}",
                    "path_id": keep.path_id,
                    "path": keep.path,
                    "bucket": keep.bucket,
                    "decision": "keep",
                    "reason": "hash_exact_best_ranked",
                    "confidence": keep.confidence,
                    "needs_review": keep.needs_review,
                    "bucket_reason": keep.bucket_reason,
                    "method": "hash_exact",
                    "ts": now_iso(),
                }
            )
            for x in candidates:
                if x.path_id == keep.path_id:
                    continue
                files_dropped += 1
                row = {
                    "group_key": f"hash::{keep.path_id}",
                    "path_id": x.path_id,
                    "path": x.path,
                    "bucket": x.bucket,
                    "decision": "drop",
                    "reason": "hash_exact_lower_rank",
                    "confidence": x.confidence,
                    "needs_review": x.needs_review,
                    "bucket_reason": x.bucket_reason,
                    "method": "hash_exact",
                    "ts": now_iso(),
                }
                rows_plan.append(row)
                rows_drop.append(row)
            # ハッシュ方式で処理したすべてのパスを記録
            for p in hg:
                hash_claimed_paths.add(p)

        # --- Phase 2: メタデータ方式（ハッシュ済みパスを除外） ---
        for gk in group_keys:
            arr = [x for x in grouped[gk] if x.path not in hash_claimed_paths]
            if len(arr) < 2:
                continue
            cohorts: list[tuple[str, list[Candidate]]] = []
            if keep_terrestrial_and_bscs:
                by_bucket: dict[str, list[Candidate]] = {"terrestrial": [], "bs_cs": [], "unknown": []}
                for x in arr:
                    by_bucket.setdefault(x.bucket, []).append(x)
                if by_bucket["unknown"] and (by_bucket["terrestrial"] or by_bucket["bs_cs"]):
                    groups_manual_review += 1
                    for x in arr:
                        rows_plan.append(
                            {
                                "group_key": gk,
                                "path_id": x.path_id,
                                "path": x.path,
                                "bucket": x.bucket,
                                "decision": "manual_review_required",
                                "reason": "unknown_bucket_mixed",
                                "confidence": x.confidence,
                                "needs_review": x.needs_review,
                                "bucket_reason": x.bucket_reason,
                                "ts": now_iso(),
                            }
                        )
                    continue
                if by_bucket["terrestrial"] and by_bucket["bs_cs"]:
                    groups_split_by_broadcast += 1
                    files_kept_by_broadcast_policy += 1
                for b in ("terrestrial", "bs_cs", "unknown"):
                    if by_bucket[b]:
                        cohorts.append((b, by_bucket[b]))
            else:
                cohorts.append(("all", arr))

            group_has_drop = False
            for bucket_name, cohort in cohorts:
                if len(cohort) == 1:
                    keep = cohort[0]
                    rows_plan.append(
                        {
                            "group_key": gk,
                            "path_id": keep.path_id,
                            "path": keep.path,
                            "bucket": keep.bucket,
                            "decision": "keep",
                            "reason": f"single_in_bucket:{bucket_name}",
                            "confidence": keep.confidence,
                            "needs_review": keep.needs_review,
                            "bucket_reason": keep.bucket_reason,
                            "ts": now_iso(),
                        }
                    )
                    files_kept += 1
                    continue

                keep = choose_keep(cohort)
                files_kept += 1
                rows_plan.append(
                    {
                        "group_key": gk,
                        "path_id": keep.path_id,
                        "path": keep.path,
                        "bucket": keep.bucket,
                        "decision": "keep",
                        "reason": f"best_ranked:{bucket_name}",
                        "confidence": keep.confidence,
                        "needs_review": keep.needs_review,
                        "bucket_reason": keep.bucket_reason,
                        "ts": now_iso(),
                    }
                )
                for x in cohort:
                    if x.path_id == keep.path_id:
                        continue
                    group_has_drop = True
                    files_dropped += 1
                    row = {
                        "group_key": gk,
                        "path_id": x.path_id,
                        "path": x.path,
                        "bucket": x.bucket,
                        "decision": "drop",
                        "reason": f"lower_rank_in_bucket:{bucket_name}",
                        "confidence": x.confidence,
                        "needs_review": x.needs_review,
                        "bucket_reason": x.bucket_reason,
                        "ts": now_iso(),
                    }
                    rows_plan.append(row)
                    rows_drop.append(row)

            if group_has_drop:
                groups_auto_processed += 1

        with plan_path.open("w", encoding="utf-8") as w:
            w.write(
                safe_json(
                    {
                        "_meta": {
                            "kind": "dedup_plan",
                            "generated_at": now_iso(),
                            "db": args.db,
                            "groups_total": len(group_keys),
                            "keep_terrestrial_and_bscs": keep_terrestrial_and_bscs,
                            "bucket_rules_path": str(bucket_rules_path),
                            "dropped_for_missing_key": dropped_for_missing_key,
                        }
                    }
                )
                + "\n"
            )
            for row in rows_plan:
                w.write(safe_json(row) + "\n")

        files_moved = 0
        run_id: str | None = None
        apply_rows: list[dict[str, Any]] = []
        move_backend = "wsl_shutil_move"
        move_apply_file: Path | None = None
        if args.apply:
            # --apply は Phase 1 hash 完全一致の drop のみ auto-apply する
            # Phase 2 メタデータベースの drop は YAML レビュー経由で適用
            hash_rows_drop = [r for r in rows_drop if r.get("method") == "hash_exact"]
            move_backend = "pwsh7_apply_move_plan"
            scripts_root = ops_root / "scripts"
            apply_move_script = scripts_root / "apply_move_plan.ps1"
            if not hash_rows_drop:
                pass  # no hash drops to apply
            elif not apply_move_script.exists():
                errors.append(f"apply_move_plan.ps1 not found: {apply_move_script}")
            else:
                ops_root_win = wsl_to_windows_path(str(ops_root))
                quarantine_root_win = wsl_to_windows_path(str(quarantine_root))
                internal_move_plan = move_dir / f"dedup_move_plan_internal_{ts}.jsonl"
                drop_by_path_id = {str(r["path_id"]): r for r in hash_rows_drop if r.get("path_id")}
                with internal_move_plan.open("w", encoding="utf-8") as w:
                    w.write(
                        safe_json(
                            {
                                "_meta": {
                                    "kind": "dedup_move_plan_internal",
                                    "generated_at": now_iso(),
                                    "source": "dedup_recordings.py",
                                    "rows": len(hash_rows_drop),
                                }
                            }
                        )
                        + "\n"
                    )
                    for row in hash_rows_drop:
                        src_win = canonicalize_windows_path(str(row["path"]))
                        group_dir_win = canonicalize_windows_path(
                            quarantine_root_win + "\\" + safe_group_key(str(row["group_key"]))
                        )
                        base_name = PureWindowsPath(src_win).name
                        dst_win = canonicalize_windows_path(group_dir_win + "\\" + base_name)
                        w.write(
                            safe_json(
                                {
                                    "op": "move",
                                    "path_id": row["path_id"],
                                    "src": src_win,
                                    "dst": dst_win,
                                }
                            )
                            + "\n"
                        )

                move_apply_file: Path | None = None
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
                    errors.append(f"dedup apply move engine failed: {e}")

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
                                "dedup",
                                str(quarantine_root),
                                now_iso(),
                                None,
                                "dedup_recordings.py",
                                f"hash_auto_apply groups={hash_groups_processed} drops={len(hash_rows_drop)}",
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
                                        run_id,
                                        now_iso(),
                                        "dedup_move",
                                        pid,
                                        None,
                                        safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}),
                                        1,
                                        None,
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
                                            run_id,
                                            now_iso(),
                                            "dedup_move",
                                            pid,
                                            None,
                                            safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}),
                                            0,
                                            err_text or "move_failed",
                                        ),
                                    )

                            apply_rows.append(
                                {
                                    "group_key": group_key,
                                    "path_id": pid or None,
                                    "src": src_win,
                                    "dst": dst_win_val,
                                    "ok": ok,
                                    "error": err_text,
                                    "ts": str(rec.get("ts") or now_iso()),
                                }
                            )

                        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
                        con.commit()
                    except Exception:
                        con.rollback()
                        raise

            with apply_path.open("w", encoding="utf-8") as w:
                w.write(
                    safe_json(
                        {
                            "_meta": {
                                "kind": "dedup_apply",
                                "generated_at": now_iso(),
                                "run_id": run_id,
                                "rows": len(apply_rows),
                            }
                        }
                    )
                    + "\n"
                )
                for row in apply_rows:
                    w.write(safe_json(row) + "\n")

        move_apply_stats = aggregate_move_apply(str(move_apply_file)) if move_apply_file else None

        summary = {
            "ok": len(errors) == 0,
            "tool": "video_pipeline_dedup_recordings",
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
            "hashScanPath": args.hash_scan_path or None,
            "hashRawJson": args.hash_raw_json or None,
            "files_hash_rows_upserted": files_hash_rows_upserted,
            "hashGroupsTotal": hash_groups_total,
            "hashGroupsProcessed": hash_groups_processed,
            "hashGroupDetails": hash_group_details,
            "errors": errors,
        }
        print(safe_json(summary))
        return 0 if summary["ok"] else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
