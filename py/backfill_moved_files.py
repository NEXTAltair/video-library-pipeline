#!/usr/bin/env python3
r"""Backfill moved files into mediaops.sqlite.

This script scans configured roots, compares against DB paths/observations,
and writes backfill plan/apply artifacts under windowsOpsRoot/move.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall, fetchone
from path_placement_rules import DB_CONTRACT_REQUIRED
from pathscan_common import (
    DEFAULT_EXTENSIONS,
    DEFAULT_SCAN_RETRY_COUNT,
    MAX_SUMMARY_WARNINGS,
    as_bool,
    ensure_exts,
    normalize_win_for_id,
    now_iso,
    parse_json_arg,
    parse_simple_yaml_lists,
    path_id_for,
    safe_json,
    scan_files,
    split_win,
    ts_compact,
    windows_to_wsl_path,
    wsl_to_windows_path,
)


def normalize_drive_key(d: str) -> str:
    x = d.strip().upper()
    if x.endswith(":"):
        return x
    if len(x) == 1 and x.isalpha():
        return x + ":"
    return x


def build_drive_map(obj: dict[str, str] | None) -> dict[str, str]:
    if not obj:
        return {}
    out: dict[str, str] = {}
    for k, v in obj.items():
        kk = normalize_drive_key(str(k))
        vv = normalize_drive_key(str(v))
        if re.match(r"^[A-Z]:$", kk) and re.match(r"^[A-Z]:$", vv):
            out[kk] = vv
    return out


def metadata_row_needs_queue(row) -> bool:
    """Check if a path_metadata row needs re-queuing for extraction.

    Accepts a sqlite3.Row with promoted columns (program_title, air_date, needs_review, etc.)
    or a raw data_json string for backward compat.
    """
    if row is None:
        return True
    # If it's a string, use legacy parsing
    if isinstance(row, str):
        try:
            md = json.loads(row)
            if not isinstance(md, dict):
                return True
        except Exception:
            return True
        missing = [k for k in DB_CONTRACT_REQUIRED if k not in md]
        if missing:
            return True
        if not isinstance(md.get("needs_review"), bool):
            return True
        if md.get("needs_review") is True:
            return True
        if not md.get("program_title"):
            return True
        if md.get("air_date") is None:
            return True
        return False

    # Use promoted columns from the row
    program_title = row["program_title"] if "program_title" in row.keys() else None
    air_date = row["air_date"] if "air_date" in row.keys() else None
    needs_review = row["needs_review"] if "needs_review" in row.keys() else None

    if not program_title:
        return True
    if air_date is None:
        return True
    if needs_review is None:
        return True
    if bool(needs_review):
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--dest-root", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--roots-json", default="")
    ap.add_argument("--roots-file-path", default="")
    ap.add_argument("--extensions-json", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--include-observations", default="true")
    ap.add_argument("--queue-missing-metadata", default="false")
    ap.add_argument("--drive-map-json", default="")
    ap.add_argument("--detect-corruption", default="true")
    ap.add_argument("--corruption-read-bytes", type=int, default=4096)
    ap.add_argument("--scan-error-policy", choices=["warn", "fail", "threshold"], default="warn")
    ap.add_argument("--scan-error-threshold", type=int, default=0)
    ap.add_argument("--scan-retry-count", type=int, default=DEFAULT_SCAN_RETRY_COUNT)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(args.windows_ops_root).resolve()
    move_dir = ops_root / "move"
    llm_dir = ops_root / "llm"
    move_dir.mkdir(parents=True, exist_ok=True)
    llm_dir.mkdir(parents=True, exist_ok=True)

    roots_from_param = parse_json_arg(args.roots_json, [])
    if roots_from_param and not isinstance(roots_from_param, list):
        raise SystemExit("roots must be JSON array")
    roots: list[str] = []
    if isinstance(roots_from_param, list) and roots_from_param:
        roots = [windows_to_wsl_path(str(x)) for x in roots_from_param if str(x).strip()]
    else:
        roots_file_path = args.roots_file_path.strip() or str((Path(__file__).resolve().parent.parent / "rules" / "backfill_roots.yaml"))
        try:
            parsed = parse_simple_yaml_lists(Path(roots_file_path))
            roots_yaml = parsed.get("roots", [])
            if isinstance(roots_yaml, list):
                roots = [windows_to_wsl_path(str(x)) for x in roots_yaml if str(x).strip()]
            if not args.extensions_json.strip():
                yaml_ext = parsed.get("extensions", [])
                if isinstance(yaml_ext, list):
                    args.extensions_json = safe_json(yaml_ext)
        except FileNotFoundError:
            if not args.roots_file_path.strip():
                roots = []
            else:
                raise SystemExit(f"rootsFilePath not found: {roots_file_path}")

    if not roots:
        roots = [windows_to_wsl_path(args.dest_root)]

    exts_raw = parse_json_arg(args.extensions_json, DEFAULT_EXTENSIONS)
    if exts_raw and not isinstance(exts_raw, list):
        raise SystemExit("extensions must be JSON array")
    extensions = ensure_exts(exts_raw if isinstance(exts_raw, list) else DEFAULT_EXTENSIONS)
    include_observations = as_bool(args.include_observations, True)
    queue_missing_metadata = as_bool(args.queue_missing_metadata, False)
    detect_corruption = as_bool(args.detect_corruption, True)
    read_bytes = max(1, int(args.corruption_read_bytes or 4096))
    limit = max(0, int(args.limit or 0))
    scan_retry_count = max(0, int(args.scan_retry_count or DEFAULT_SCAN_RETRY_COUNT))
    drive_map_obj = parse_json_arg(args.drive_map_json, {})
    if drive_map_obj and not isinstance(drive_map_obj, dict):
        raise SystemExit("driveMap must be JSON object")
    drive_map = build_drive_map({str(k): str(v) for k, v in (drive_map_obj or {}).items()})

    scanned, scan_warnings, scan_errors, fallback_stats = scan_files(
        roots=roots,
        exts=set(extensions),
        detect_corruption=detect_corruption,
        read_bytes=read_bytes,
        scan_retry_count=scan_retry_count,
        windows_ops_root=str(ops_root),
    )
    if limit > 0:
        scanned = scanned[:limit]

    con = connect_db(args.db)
    create_schema_if_needed(con)
    ts = ts_compact()
    plan_path = move_dir / f"backfill_plan_{ts}.jsonl"
    apply_path = move_dir / f"backfill_apply_{ts}.jsonl"
    queue_path = llm_dir / f"backfill_metadata_queue_{ts}.jsonl"

    rows_for_apply: list[dict[str, Any]] = []
    rows_for_plan: list[dict[str, Any]] = []
    remapped_paths = 0
    rename_detected = 0
    corrupt_candidates = 0
    skipped_existing = 0
    missing_in_paths = 0
    errors: list[str] = []
    queue_candidates: list[dict[str, Any]] = []

    try:
        for err in scan_errors:
            errors.append(err)
        for sf in scanned:
            if sf.corrupt_candidate:
                corrupt_candidates += 1
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": path_id_for(sf.win_path),
                        "status": "error",
                        "reason": f"corrupt_candidate:{sf.corrupt_reason}",
                        "ts": now_iso(),
                    }
                )
                continue

            existing = fetchone(con, "SELECT path_id, path FROM paths WHERE path = ?", (sf.win_path,))
            if existing:
                pid = str(existing["path_id"])
                if include_observations:
                    obs = fetchone(con, "SELECT path_id FROM observations WHERE path_id = ? LIMIT 1", (pid,))
                    if obs is None:
                        rows_for_apply.append(
                            {
                                "op": "obs_only",
                                "path_id": pid,
                                "path": sf.win_path,
                                "drive": sf.drive,
                                "dir": sf.dir,
                                "name": sf.name,
                                "ext": sf.ext,
                                "size_bytes": sf.size,
                                "mtime_utc": sf.mtime_utc,
                                "type": sf.ext,
                            }
                        )
                        rows_for_plan.append(
                            {
                                "path": sf.win_path,
                                "path_id": pid,
                                "status": "planned",
                                "reason": "missing_observation",
                                "ts": now_iso(),
                            }
                        )
                    else:
                        skipped_existing += 1
                else:
                    skipped_existing += 1
                continue

            missing_in_paths += 1

            mapped_old_candidates: list[dict[str, Any]] = []
            if drive_map:
                inv_map = {v: k for k, v in drive_map.items()}
                if len(sf.win_path) >= 3 and sf.win_path[1] == ":":
                    cur_drive = sf.win_path[:2].upper()
                    if cur_drive in inv_map:
                        old_drive = inv_map[cur_drive]
                        old_path = old_drive + sf.win_path[2:]
                        old_row = fetchone(con, "SELECT path_id, path FROM paths WHERE path = ?", (old_path,))
                        if old_row:
                            new_conflict = fetchone(con, "SELECT path_id FROM paths WHERE path = ?", (sf.win_path,))
                            if new_conflict and str(new_conflict["path_id"]) != str(old_row["path_id"]):
                                rows_for_plan.append(
                                    {
                                        "path": sf.win_path,
                                        "path_id": str(old_row["path_id"]),
                                        "status": "skipped",
                                        "reason": "conflict_skip",
                                        "ts": now_iso(),
                                    }
                                )
                            else:
                                mapped_old_candidates.append(
                                    {
                                        "path_id": str(old_row["path_id"]),
                                        "old_path": old_path,
                                        "new_path": sf.win_path,
                                    }
                                )

            if mapped_old_candidates:
                c = mapped_old_candidates[0]
                remapped_paths += 1
                rows_for_apply.append(
                    {
                        "op": "remap_update",
                        "path_id": c["path_id"],
                        "path": sf.win_path,
                        "old_path": c["old_path"],
                        "drive": sf.drive,
                        "dir": sf.dir,
                        "name": sf.name,
                        "ext": sf.ext,
                        "size_bytes": sf.size,
                        "mtime_utc": sf.mtime_utc,
                        "type": sf.ext,
                    }
                )
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": c["path_id"],
                        "status": "remapped",
                        "reason": "drive_map",
                        "ts": now_iso(),
                    }
                )
                continue

            rename_rows = fetchall(
                con,
                """
                SELECT p.path_id, p.path
                FROM paths p
                JOIN observations o ON o.path_id = p.path_id
                WHERE p.name = ? AND o.size_bytes = ?
                GROUP BY p.path_id, p.path
                LIMIT 20
                """,
                (sf.name, sf.size),
            )
            if len(rename_rows) == 1:
                old = rename_rows[0]
                rename_detected += 1
                rows_for_apply.append(
                    {
                        "op": "rename_update",
                        "path_id": str(old["path_id"]),
                        "path": sf.win_path,
                        "old_path": str(old["path"]),
                        "drive": sf.drive,
                        "dir": sf.dir,
                        "name": sf.name,
                        "ext": sf.ext,
                        "size_bytes": sf.size,
                        "mtime_utc": sf.mtime_utc,
                        "type": sf.ext,
                    }
                )
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": str(old["path_id"]),
                        "status": "planned",
                        "reason": "rename_detected",
                        "ts": now_iso(),
                    }
                )
                continue
            if len(rename_rows) > 1:
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": path_id_for(sf.win_path),
                        "status": "skipped",
                        "reason": "rename_ambiguous",
                        "ts": now_iso(),
                    }
                )
                continue

            pid = path_id_for(sf.win_path)
            rows_for_apply.append(
                {
                    "op": "insert_or_upsert",
                    "path_id": pid,
                    "path": sf.win_path,
                    "drive": sf.drive,
                    "dir": sf.dir,
                    "name": sf.name,
                    "ext": sf.ext,
                    "size_bytes": sf.size,
                    "mtime_utc": sf.mtime_utc,
                    "type": sf.ext,
                }
            )
            rows_for_plan.append(
                {
                    "path": sf.win_path,
                    "path_id": pid,
                    "status": "planned",
                    "reason": "missing_path",
                    "ts": now_iso(),
                }
            )

        with plan_path.open("w", encoding="utf-8") as w:
            meta = {
                "_meta": {
                    "kind": "backfill_plan",
                    "generated_at": now_iso(),
                    "db": args.db,
                    "roots": roots,
                    "extensions": extensions,
                    "apply": bool(args.apply),
                    "queue_missing_metadata": queue_missing_metadata,
                }
            }
            w.write(safe_json(meta) + "\n")
            for r in rows_for_plan:
                w.write(safe_json(r) + "\n")

        upserted_paths = 0
        upserted_obs = 0
        run_id: str | None = None
        if args.apply:
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
                        "backfill",
                        args.dest_root,
                        now_iso(),
                        None,
                        "backfill_moved_files.py",
                        f"roots={len(roots)} scanned={len(scanned)}",
                    ),
                )
                for row in rows_for_apply:
                    pid = str(row["path_id"])
                    pth = str(row["path"])
                    con.execute(
                        """
                        INSERT INTO paths (path_id, path, drive, dir, name, ext, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(path_id) DO UPDATE SET
                          path=excluded.path,
                          drive=excluded.drive,
                          dir=excluded.dir,
                          name=excluded.name,
                          ext=excluded.ext,
                          updated_at=excluded.updated_at
                        """,
                        (pid, pth, row.get("drive"), row.get("dir"), row.get("name"), row.get("ext"), now_iso(), now_iso()),
                    )
                    upserted_paths += 1

                    if include_observations:
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
                                int(row.get("size_bytes") or 0),
                                row.get("mtime_utc"),
                                row.get("type"),
                                None,
                            ),
                        )
                        upserted_obs += 1

                    event_kind = "backfill_register"
                    detail: dict[str, Any] = {"path": pth, "op": row.get("op")}
                    if row.get("op") == "remap_update":
                        event_kind = "backfill_remap"
                        detail["old_path"] = row.get("old_path")
                        detail["new_path"] = pth
                    elif row.get("op") == "rename_update":
                        event_kind = "backfill_rename_detected"
                        detail["old_path"] = row.get("old_path")
                        detail["new_path"] = pth

                    con.execute(
                        """
                        INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (run_id, now_iso(), event_kind, pid, None, safe_json(detail), 1, None),
                    )

                con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
                con.commit()
            except Exception:
                con.rollback()
                raise

            with apply_path.open("w", encoding="utf-8") as w:
                meta = {
                    "_meta": {
                        "kind": "backfill_apply",
                        "generated_at": now_iso(),
                        "run_id": run_id,
                        "rows": len(rows_for_apply),
                    }
                }
                w.write(safe_json(meta) + "\n")
                for row in rows_for_apply:
                    w.write(
                        safe_json(
                            {
                                "path": row.get("path"),
                                "path_id": row.get("path_id"),
                                "status": "upserted",
                                "reason": row.get("op"),
                                "ts": now_iso(),
                            }
                        )
                        + "\n"
                    )

        target_rows: list[dict[str, Any]] = []
        for row in rows_for_apply:
            target_rows.append(
                {
                    "path_id": str(row["path_id"]),
                    "path": str(row["path"]),
                    "name": row.get("name"),
                    "mtime_utc": row.get("mtime_utc"),
                }
            )

        metadata_queue_planned_count = 0
        metadata_queue_path: str | None = None
        if queue_missing_metadata and target_rows:
            for tr in target_rows:
                md_row = fetchone(
                    con,
                    """
                    SELECT data_json, program_title, air_date, needs_review,
                           episode_no, subtitle, broadcaster, human_reviewed
                    FROM path_metadata
                    WHERE path_id=?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (tr["path_id"],),
                )
                if metadata_row_needs_queue(md_row):
                    queue_candidates.append(tr)
            metadata_queue_planned_count = len(queue_candidates)
            if args.apply:
                metadata_queue_path = str(queue_path)
                with queue_path.open("w", encoding="utf-8") as w:
                    w.write(
                        safe_json(
                            {
                                "_meta": {
                                    "kind": "backfill_metadata_queue",
                                    "generated_at": now_iso(),
                                    "source": "backfill_moved_files.py",
                                    "rows": len(queue_candidates),
                                }
                            }
                        )
                        + "\n"
                    )
                    for row in queue_candidates:
                        w.write(safe_json(row) + "\n")

        if args.scan_error_policy == "fail" and scan_warnings:
            errors.append(f"scan warnings treated as fatal by policy=fail: count={len(scan_warnings)}")
        if args.scan_error_policy == "threshold":
            threshold = max(0, int(args.scan_error_threshold or 0))
            if threshold <= 0:
                errors.append("scanErrorThreshold must be > 0 when scanErrorPolicy=threshold")
            elif len(scan_warnings) > threshold:
                errors.append(f"scan warnings exceeded threshold: {len(scan_warnings)} > {threshold}")

        warning_count = len(scan_warnings)
        warnings_out = scan_warnings
        warnings_truncated = False
        if warning_count > MAX_SUMMARY_WARNINGS:
            warnings_out = scan_warnings[:MAX_SUMMARY_WARNINGS]
            warnings_truncated = True

        summary = {
            "ok": len(errors) == 0,
            "tool": "video_pipeline_backfill_moved_files",
            "apply": bool(args.apply),
            "db": args.db,
            "planPath": str(plan_path),
            "applyPath": str(apply_path) if args.apply else None,
            "metadataQueuePath": metadata_queue_path,
            "scannedFiles": len(scanned),
            "missingInPaths": missing_in_paths,
            "upsertedPaths": upserted_paths if args.apply else 0,
            "upsertedObservations": upserted_obs if args.apply else 0,
            "remappedPaths": remapped_paths,
            "renameDetected": rename_detected,
            "corruptCandidates": corrupt_candidates,
            "skippedExisting": skipped_existing,
            "metadataQueuePlannedCount": metadata_queue_planned_count,
            "scanErrorPolicy": args.scan_error_policy,
            "scanErrorThreshold": int(args.scan_error_threshold or 0),
            "scanRetryCount": scan_retry_count,
            "warningCount": warning_count,
            "warningsTruncated": warnings_truncated,
            "warnings": warnings_out,
            "windowsFallbackUsed": bool(fallback_stats.get("windowsFallbackUsed")),
            "windowsFallbackDirs": int(fallback_stats.get("windowsFallbackDirs") or 0),
            "windowsFallbackFiles": int(fallback_stats.get("windowsFallbackFiles") or 0),
            "windowsFallbackErrorCount": int(fallback_stats.get("windowsFallbackErrorCount") or 0),
            "errors": errors,
        }
        print(safe_json(summary))
        return 0 if summary["ok"] else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
