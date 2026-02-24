#!/usr/bin/env python3
r"""Relocate existing files under arbitrary roots to current placement rules based on DB metadata.

- Dry-run: writes relocate_plan JSONL, no file moves
- Apply: executes Windows move plan via apply_move_plan.ps1 and updates DB paths
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchone
from path_placement_rules import build_expected_dest_path, has_required_db_contract
from pathscan_common import (
    DEFAULT_EXTENSIONS,
    DEFAULT_SCAN_RETRY_COUNT,
    MAX_SUMMARY_WARNINGS,
    as_bool,
    canonicalize_windows_path,
    ensure_exts,
    now_iso,
    parse_json_arg,
    parse_simple_yaml_lists,
    safe_json,
    scan_files,
    split_win,
    ts_compact,
    normalize_win_for_id,
    windows_to_wsl_path,
    wsl_to_windows_path,
)
from windows_pwsh_bridge import run_pwsh_json

PATH_NAMESPACE = uuid.UUID("f4f67a6f-90c6-4ee4-9c1a-2c0d25b3b0c4")


def path_id_for(p: str) -> str:
    return str(uuid.uuid5(PATH_NAMESPACE, "winpath:" + normalize_win_for_id(p)))


def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def latest_metadata_for_path(con, path_id: str) -> tuple[dict[str, Any] | None, str | None]:
    row = fetchone(
        con,
        """
        SELECT source, data_json
        FROM path_metadata
        WHERE path_id=?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (path_id,),
    )
    if not row:
        return None, None
    try:
        md = json.loads(str(row["data_json"]))
    except Exception:
        return None, str(row["source"]) if row["source"] is not None else None
    return md if isinstance(md, dict) else None, str(row["source"]) if row["source"] is not None else None


def metadata_needs_queue(md: dict[str, Any] | None) -> bool:
    if not md or not isinstance(md, dict):
        return True
    if not has_required_db_contract(md):
        return True
    if md.get("needs_review") is True:
        return True
    if not md.get("program_title"):
        return True
    if not md.get("air_date"):
        return True
    return False


def parse_last_json_object(stdout: str) -> dict[str, Any] | None:
    for line in reversed((stdout or "").splitlines()):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def run_uv_python_json(script: Path, args: list[str], cwd: str | None = None) -> tuple[dict[str, Any] | None, str, str, int]:
    cp = subprocess.run(
        ["uv", "run", "python", str(script), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=cwd,
    )
    stdout = cp.stdout or ""
    stderr = cp.stderr or ""
    return parse_last_json_object(stdout), stdout, stderr, int(cp.returncode or 0)


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
    ap.add_argument("--allow-needs-review", default="false")
    ap.add_argument("--queue-missing-metadata", default="false")
    ap.add_argument("--write-metadata-queue-on-dry-run", default="false")
    ap.add_argument("--scan-error-policy", choices=["warn", "fail", "threshold"], default="warn")
    ap.add_argument("--scan-error-threshold", type=int, default=0)
    ap.add_argument("--scan-retry-count", type=int, default=DEFAULT_SCAN_RETRY_COUNT)
    ap.add_argument("--on-dst-exists", choices=["error", "rename_suffix"], default="error")
    args = ap.parse_args()

    db_path = windows_to_wsl_path(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(windows_to_wsl_path(args.windows_ops_root)).resolve()
    move_dir = ops_root / "move"
    llm_dir = ops_root / "llm"
    move_dir.mkdir(parents=True, exist_ok=True)
    llm_dir.mkdir(parents=True, exist_ok=True)

    dest_root_win = canonicalize_windows_path(args.dest_root)
    ops_root_win = canonicalize_windows_path(args.windows_ops_root)
    scripts_root_win = canonicalize_windows_path(str(ops_root / "scripts"))

    roots: list[str] = []
    roots_param = parse_json_arg(args.roots_json, None)
    if roots_param is not None:
        if not isinstance(roots_param, list):
            raise SystemExit("roots must be JSON array")
        roots = [windows_to_wsl_path(str(x)) for x in roots_param if str(x).strip()]
    else:
        rfp = str(args.roots_file_path or "").strip()
        if not rfp:
            raise SystemExit("roots or rootsFilePath is required")
        parsed = parse_simple_yaml_lists(Path(windows_to_wsl_path(rfp)))
        roots_yaml = parsed.get("roots", [])
        if not isinstance(roots_yaml, list):
            raise SystemExit("rootsFilePath must contain list key 'roots'")
        roots = [windows_to_wsl_path(str(x)) for x in roots_yaml if str(x).strip()]
        if not args.extensions_json.strip():
            yaml_ext = parsed.get("extensions", [])
            if isinstance(yaml_ext, list):
                args.extensions_json = safe_json(yaml_ext)

    if not roots:
        raise SystemExit("roots or rootsFilePath is required (no roots resolved)")
    roots_win = [canonicalize_windows_path(r) for r in roots]

    exts_raw = parse_json_arg(args.extensions_json, DEFAULT_EXTENSIONS)
    if exts_raw and not isinstance(exts_raw, list):
        raise SystemExit("extensions must be JSON array")
    extensions = ensure_exts(exts_raw if isinstance(exts_raw, list) else DEFAULT_EXTENSIONS)
    limit = max(0, int(args.limit or 0))
    allow_needs_review = as_bool(args.allow_needs_review, False)
    queue_missing_metadata = as_bool(args.queue_missing_metadata, False)
    write_metadata_queue_on_dry_run = as_bool(args.write_metadata_queue_on_dry_run, False)
    scan_retry_count = max(0, int(args.scan_retry_count or DEFAULT_SCAN_RETRY_COUNT))

    scanned, scan_warnings, scan_errors, fallback_stats = scan_files(
        roots=roots,
        exts=set(extensions),
        detect_corruption=True,
        read_bytes=4096,
        scan_retry_count=scan_retry_count,
        windows_ops_root=str(ops_root),
    )
    if limit > 0:
        scanned = scanned[:limit]

    con = connect_db(db_path)
    create_schema_if_needed(con)

    ts = ts_compact()
    plan_path = move_dir / f"relocate_plan_{ts}.jsonl"
    apply_path = move_dir / f"relocate_apply_{ts}.jsonl"
    internal_move_plan_path = move_dir / f"_relocate_move_plan_internal_{ts}.jsonl"
    queue_path = llm_dir / f"relocate_metadata_queue_{ts}.jsonl"

    rows_for_plan: list[dict[str, Any]] = []
    rows_for_move: list[dict[str, Any]] = []
    rows_for_autoreg: list[dict[str, Any]] = []
    queue_candidates: list[dict[str, Any]] = []
    plan_reason_map: dict[tuple[str | None, str | None, str | None], str] = {}

    registered_files = 0
    planned_moves = 0
    already_correct = 0
    unregistered_skipped = 0
    metadata_missing_skipped = 0
    invalid_contract_skipped = 0
    needs_review_skipped = 0
    corrupt_candidates = 0
    dst_exists_errors = 0
    auto_registered_paths = 0
    auto_registered_observations = 0
    errors: list[str] = list(scan_errors)

    try:
        for sf in scanned:
            ts_row = now_iso()
            if sf.corrupt_candidate:
                corrupt_candidates += 1
                rows_for_plan.append(
                    {
                        "src": sf.win_path,
                        "status": "error",
                        "reason": f"corrupt_candidate:{sf.corrupt_reason}",
                        "ts": ts_row,
                    }
                )
                continue

            path_row = fetchone(con, "SELECT path_id, path FROM paths WHERE path = ?", (sf.win_path,))
            if not path_row:
                unregistered_skipped += 1
                pending_auto = bool(args.apply)
                rows_for_plan.append(
                    {
                        "src": sf.win_path,
                        "status": "skipped",
                        "reason": "unregistered_path",
                        "auto_register_on_apply": pending_auto,
                        "ts": ts_row,
                    }
                )
                if pending_auto:
                    pid = path_id_for(sf.win_path)
                    drive, dir_path, name, ext = split_win(sf.win_path)
                    rows_for_autoreg.append(
                        {
                            "path_id": pid,
                            "path": sf.win_path,
                            "drive": drive,
                            "dir": dir_path,
                            "name": name or sf.name,
                            "ext": ext or sf.ext,
                            "size_bytes": int(sf.size or 0),
                            "mtime_utc": sf.mtime_utc,
                            "type": "file",
                        }
                    )
                    if queue_missing_metadata:
                        queue_candidates.append(
                            {"path_id": pid, "path": sf.win_path, "name": sf.name, "mtime_utc": sf.mtime_utc}
                        )
                continue

            registered_files += 1
            path_id = str(path_row["path_id"])
            md, md_source = latest_metadata_for_path(con, path_id)
            if md is None:
                metadata_missing_skipped += 1
                rows_for_plan.append(
                    {"path_id": path_id, "src": sf.win_path, "status": "skipped", "reason": "missing_metadata", "ts": ts_row}
                )
                if queue_missing_metadata:
                    queue_candidates.append(
                        {"path_id": path_id, "path": sf.win_path, "name": sf.name, "mtime_utc": sf.mtime_utc}
                    )
                continue

            if not has_required_db_contract(md):
                invalid_contract_skipped += 1
                rows_for_plan.append(
                    {
                        "path_id": path_id,
                        "src": sf.win_path,
                        "status": "skipped",
                        "reason": "invalid_metadata_contract",
                        "metadata_source": md_source,
                        "ts": ts_row,
                    }
                )
                if queue_missing_metadata:
                    queue_candidates.append(
                        {"path_id": path_id, "path": sf.win_path, "name": sf.name, "mtime_utc": sf.mtime_utc}
                    )
                continue

            if bool(md.get("needs_review")) and not allow_needs_review:
                needs_review_skipped += 1
                rows_for_plan.append(
                    {
                        "path_id": path_id,
                        "src": sf.win_path,
                        "status": "skipped",
                        "reason": "needs_review",
                        "metadata_source": md_source,
                        "ts": ts_row,
                    }
                )
                if queue_missing_metadata:
                    queue_candidates.append(
                        {"path_id": path_id, "path": sf.win_path, "name": sf.name, "mtime_utc": sf.mtime_utc}
                    )
                continue

            dst, dst_err = build_expected_dest_path(dest_root_win, sf.win_path, md)
            if not dst or dst_err:
                invalid_contract_skipped += 1
                rows_for_plan.append(
                    {
                        "path_id": path_id,
                        "src": sf.win_path,
                        "status": "skipped",
                        "reason": dst_err or "build_expected_dest_failed",
                        "metadata_source": md_source,
                        "ts": ts_row,
                    }
                )
                if queue_missing_metadata and metadata_needs_queue(md):
                    queue_candidates.append(
                        {"path_id": path_id, "path": sf.win_path, "name": sf.name, "mtime_utc": sf.mtime_utc}
                    )
                continue

            dst = canonicalize_windows_path(dst)
            if sf.win_path == dst:
                already_correct += 1
                rows_for_plan.append(
                    {
                        "path_id": path_id,
                        "src": sf.win_path,
                        "dst": dst,
                        "status": "skipped",
                        "reason": "already_correct",
                        "program_title": md.get("program_title"),
                        "air_date": md.get("air_date"),
                        "ts": ts_row,
                    }
                )
                continue

            planned_moves += 1
            reason = "recompute_destination"
            rows_for_plan.append(
                {
                    "path_id": path_id,
                    "src": sf.win_path,
                    "dst": dst,
                    "status": "planned",
                    "reason": reason,
                    "program_title": md.get("program_title"),
                    "air_date": md.get("air_date"),
                    "ts": ts_row,
                }
            )
            row_move = {"path_id": path_id, "src": sf.win_path, "dst": dst}
            rows_for_move.append(row_move)
            plan_reason_map[(path_id, sf.win_path, dst)] = reason

        with plan_path.open("w", encoding="utf-8") as w:
            w.write(
                safe_json(
                    {
                        "_meta": {
                            "kind": "relocate_plan",
                            "generated_at": now_iso(),
                            "db": db_path,
                            "dest_root": dest_root_win,
                            "roots": roots_win,
                            "apply": bool(args.apply),
                            "allow_needs_review": allow_needs_review,
                            "queue_missing_metadata": queue_missing_metadata,
                            "extensions": extensions,
                            "on_dst_exists": str(args.on_dst_exists),
                        }
                    }
                )
                + "\n"
            )
            for row in rows_for_plan:
                w.write(safe_json(row) + "\n")

        raw_move_apply_path: str | None = None
        relocate_apply_rows: list[dict[str, Any]] = []
        moved_files = 0
        db_updated_paths = 0
        metadata_queue_path: str | None = None
        internal_move_plan_out: str | None = None

        if args.apply and rows_for_autoreg:
            prereg_run_id = str(uuid.uuid4())
            try:
                begin_immediate(con)
                con.execute(
                    """
                    INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        prereg_run_id,
                        "relocate_preregister",
                        dest_root_win,
                        now_iso(),
                        None,
                        "relocate_existing_files.py",
                        f"roots={len(roots)} autoreg={len(rows_for_autoreg)}",
                    ),
                )
                for row in rows_for_autoreg:
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
                        (
                            pid,
                            pth,
                            row.get("drive"),
                            row.get("dir"),
                            row.get("name"),
                            row.get("ext"),
                            now_iso(),
                            now_iso(),
                        ),
                    )
                    auto_registered_paths += 1
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
                            prereg_run_id,
                            pid,
                            int(row.get("size_bytes") or 0),
                            row.get("mtime_utc"),
                            row.get("type"),
                            None,
                        ),
                    )
                    auto_registered_observations += 1
                    con.execute(
                        """
                        INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            prereg_run_id,
                            now_iso(),
                            "relocate_register",
                            pid,
                            None,
                            safe_json({"path": pth, "op": "register_missing_path_for_relocate"}),
                            1,
                            None,
                        ),
                    )
                con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), prereg_run_id))
                con.commit()
            except Exception as e:
                con.rollback()
                errors.append(f"relocate auto-register failed: {e}")

        if args.apply and rows_for_move:

            if errors:
                raw_move_apply_path = None
                internal_move_plan_out = None
                moved_files = 0
                db_updated_paths = 0
            else:
                with internal_move_plan_path.open("w", encoding="utf-8") as w:
                    w.write(safe_json({"_meta": {"kind": "relocate_move_plan_internal", "generated_at": now_iso()}}) + "\n")
                    for row in rows_for_move:
                        w.write(safe_json(row) + "\n")
                internal_move_plan_out = str(internal_move_plan_path)

                apply_meta = run_pwsh_json(
                    scripts_root_win + r"\apply_move_plan.ps1",
                    [
                        "-PlanJsonl",
                        wsl_to_windows_path(str(internal_move_plan_path)),
                        "-OpsRoot",
                        ops_root_win,
                        "-OnDstExists",
                        str(args.on_dst_exists),
                    ],
                )
                raw_move_apply_win = str(apply_meta.get("out_jsonl") or "")
                raw_move_apply_path = windows_to_wsl_path(raw_move_apply_win) if raw_move_apply_win else None
                if not raw_move_apply_path or not os.path.exists(raw_move_apply_path):
                    errors.append("apply_move_plan.ps1 did not return valid out_jsonl")
                else:
                    for rec in iter_jsonl(raw_move_apply_path):
                        if rec.get("op") != "move":
                            continue
                        path_id = str(rec.get("path_id") or "") or None
                        src = str(rec.get("src") or "") or None
                        dst = str(rec.get("dst") or "") or None
                        ok = bool(rec.get("ok"))
                        err = str(rec.get("error") or "") if rec.get("error") is not None else None
                        if (err or "") == "dst_exists":
                            dst_exists_errors += 1
                        if ok:
                            moved_files += 1
                        relocate_apply_rows.append(
                            {
                                "path_id": path_id,
                                "src": src,
                                "dst": dst,
                                "ok": ok,
                                "status": "moved" if ok else "error",
                                "error": err,
                                "reason": plan_reason_map.get((path_id, src, dst), "recompute_destination"),
                                "ts": rec.get("ts") or now_iso(),
                            }
                        )

                    if any(not bool(r.get("ok")) for r in relocate_apply_rows):
                        fail_count = sum(1 for r in relocate_apply_rows if not bool(r.get("ok")))
                        errors.append(f"move apply had failures: {fail_count}")

                    here = Path(__file__).resolve().parent
                    db_res, db_stdout, db_stderr, db_rc = run_uv_python_json(
                        here / "update_db_paths_from_move_apply.py",
                        [
                            "--db",
                            db_path,
                            "--applied",
                            str(raw_move_apply_path),
                            "--run-kind",
                            "relocate",
                            "--notes",
                            f"relocate_existing_files {Path(str(raw_move_apply_path)).name}",
                        ],
                        cwd=str(here),
                    )
                    if db_rc != 0:
                        errors.append(f"update_db_paths_from_move_apply failed rc={db_rc}: {(db_stderr or db_stdout).strip()}")
                        db_update_meta = {
                            "ok": False,
                            "exitCode": db_rc,
                            "stdout": db_stdout,
                            "stderr": db_stderr,
                        }
                    else:
                        db_updated_paths = int((db_res or {}).get("updated") or 0)
                        db_update_meta = {
                            "ok": True,
                            "exitCode": db_rc,
                            "result": db_res,
                        }

                    with apply_path.open("w", encoding="utf-8") as w:
                        w.write(
                            safe_json(
                                {
                                    "_meta": {
                                        "kind": "relocate_apply",
                                        "generated_at": now_iso(),
                                        "relocate_plan": str(plan_path),
                                        "raw_move_apply": str(raw_move_apply_path),
                                        "db_update": db_update_meta,
                                        "run_kind": "relocate",
                                    }
                                }
                            )
                            + "\n"
                        )
                        for row in relocate_apply_rows:
                            w.write(safe_json(row) + "\n")

        elif args.apply:
            # Apply requested but no candidate rows; still emit empty apply audit file.
            with apply_path.open("w", encoding="utf-8") as w:
                w.write(
                    safe_json(
                        {
                            "_meta": {
                                "kind": "relocate_apply",
                                "generated_at": now_iso(),
                                "relocate_plan": str(plan_path),
                                "raw_move_apply": None,
                                "db_update": {"ok": True, "result": {"updated": 0, "events": 0, "run_kind": "relocate"}},
                                "run_kind": "relocate",
                                "rows": 0,
                            }
                        }
                    )
                    + "\n"
                )
            internal_move_plan_out = None
            raw_move_apply_path = None
            moved_files = 0
            db_updated_paths = 0

        metadata_queue_planned_count = len(queue_candidates) if queue_missing_metadata else 0
        should_write_metadata_queue = bool(
            queue_missing_metadata and queue_candidates and (args.apply or write_metadata_queue_on_dry_run)
        )
        if should_write_metadata_queue:
            metadata_queue_path = str(queue_path)
            with queue_path.open("w", encoding="utf-8") as w:
                w.write(
                    safe_json(
                        {
                            "_meta": {
                                "kind": "relocate_metadata_queue",
                                "generated_at": now_iso(),
                                "source": "relocate_existing_files.py",
                                "rows": len(queue_candidates),
                                "apply": bool(args.apply),
                                "write_on_dry_run": bool(write_metadata_queue_on_dry_run),
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
            "tool": "video_pipeline_relocate_existing_files",
            "apply": bool(args.apply),
            "db": db_path,
            "destRoot": dest_root_win,
            "roots": roots_win,
            "planPath": str(plan_path),
            "applyPath": str(apply_path) if args.apply else None,
            "metadataQueuePath": metadata_queue_path,
            "writeMetadataQueueOnDryRun": bool(write_metadata_queue_on_dry_run),
            "internalMovePlanPath": internal_move_plan_out if args.apply else None,
            "scannedFiles": len(scanned),
            "registeredFiles": registered_files,
            "plannedMoves": planned_moves,
            "movedFiles": moved_files if args.apply else 0,
            "dbUpdatedPaths": db_updated_paths if args.apply else 0,
            "alreadyCorrect": already_correct,
            "unregisteredSkipped": unregistered_skipped,
            "autoRegisteredPaths": auto_registered_paths if args.apply else 0,
            "autoRegisteredObservations": auto_registered_observations if args.apply else 0,
            "metadataMissingSkipped": metadata_missing_skipped,
            "invalidContractSkipped": invalid_contract_skipped,
            "needsReviewSkipped": needs_review_skipped,
            "corruptCandidates": corrupt_candidates,
            "dstExistsErrors": dst_exists_errors,
            "metadataQueuePlannedCount": metadata_queue_planned_count,
            "scanErrorPolicy": args.scan_error_policy,
            "scanErrorThreshold": int(args.scan_error_threshold or 0),
            "scanRetryCount": scan_retry_count,
            "onDstExists": str(args.on_dst_exists),
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
