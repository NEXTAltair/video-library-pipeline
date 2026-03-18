#!/usr/bin/env python3
"""Normalize folder name casing based on relocate dry-run plan rows.

This tool focuses on Windows case-insensitive limitation where case-only moves are
reported as already_correct by relocate. It extracts case-only candidates from the
plan and performs directory rename via PowerShell (two-step temp rename), then
updates DB paths using synthetic move-apply JSONL rows.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from pathscan_common import (
    iter_jsonl,
    now_iso,
    safe_json,
    ts_compact,
    windows_to_wsl_path,
    wsl_to_windows_path,
)
from windows_pwsh_bridge import run_pwsh_json
from relocate_existing_files import run_uv_python_json


def _split_win(p: str) -> list[str]:
    return str(p or "").split("\\")


def _first_case_only_dir_pair(src: str, dst: str) -> tuple[str, str] | None:
    src_parts = _split_win(src)
    dst_parts = _split_win(dst)
    if len(src_parts) != len(dst_parts):
        return None
    diff_idx = -1
    for i, (a, b) in enumerate(zip(src_parts, dst_parts)):
        if a == b:
            continue
        if a.lower() != b.lower():
            return None
        diff_idx = i
        break
    if diff_idx <= 0:
        return None

    # Last segment is usually a file name; we need directory rename candidates.
    if diff_idx >= len(src_parts) - 1:
        return None

    src_dir = "\\".join(src_parts[: diff_idx + 1])
    dst_dir = "\\".join(dst_parts[: diff_idx + 1])
    if src_dir.lower() != dst_dir.lower() or src_dir == dst_dir:
        return None

    return src_dir, dst_dir


def _parse_plan_rows(plan_path: str) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str]]:
    file_rows: list[dict[str, Any]] = []
    dir_pairs_by_src_lower: dict[str, dict[str, str]] = {}
    errors: list[str] = []

    for rec in iter_jsonl(plan_path):
        if rec.get("_meta") is not None:
            continue
        src = str(rec.get("src") or "")
        dst = str(rec.get("dst") or "")
        if not src or not dst:
            continue

        # Keep scope narrow: only rows that relocate marked as already_correct.
        if str(rec.get("reason") or "") != "already_correct":
            continue
        if src.lower() != dst.lower() or src == dst:
            continue

        pair = _first_case_only_dir_pair(src, dst)
        if not pair:
            continue
        src_dir, dst_dir = pair

        src_key = src_dir.lower()
        existing = dir_pairs_by_src_lower.get(src_key)
        if existing and existing["dst"] != dst_dir:
            errors.append(
                f"conflict case destination for same source dir: {src_dir} -> {existing['dst']} vs {dst_dir}"
            )
            continue

        dir_pairs_by_src_lower[src_key] = {"src": src_dir, "dst": dst_dir}
        file_rows.append({
            "path_id": rec.get("path_id"),
            "src": src,
            "dst": dst,
            "reason": "normalize_folder_case",
            "ts": now_iso(),
        })

    dir_pairs = list(dir_pairs_by_src_lower.values())
    # Rename deeper paths first to avoid parent rename side effects.
    dir_pairs.sort(key=lambda x: len(_split_win(x["src"])), reverse=True)
    return file_rows, dir_pairs, errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--plan-path", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    db_path = windows_to_wsl_path(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {args.db}")

    plan_path = windows_to_wsl_path(args.plan_path)
    if not os.path.exists(plan_path):
        raise SystemExit(f"planPath not found: {args.plan_path}")

    ops_root = Path(windows_to_wsl_path(args.windows_ops_root)).resolve()
    move_dir = ops_root / "move"
    scripts_root_win = wsl_to_windows_path(str(ops_root / "scripts"))
    move_dir.mkdir(parents=True, exist_ok=True)

    ts = ts_compact()
    case_plan_path = move_dir / f"normalize_case_plan_{ts}.jsonl"
    case_apply_path = move_dir / f"normalize_case_apply_{ts}.jsonl"
    synthetic_move_apply_path = move_dir / f"normalize_case_move_apply_{ts}.jsonl"

    file_rows, dir_pairs, errors = _parse_plan_rows(plan_path)

    with case_plan_path.open("w", encoding="utf-8") as w:
        w.write(safe_json({"_meta": {"kind": "normalize_case_plan", "generated_at": now_iso(), "planPath": plan_path}}) + "\n")
        for row in dir_pairs:
            w.write(safe_json({"status": "planned", **row}) + "\n")

    normalized_dirs = 0
    apply_rows: list[dict[str, Any]] = []
    db_updated_paths = 0

    if args.apply and dir_pairs and not errors:
        dir_plan_win = wsl_to_windows_path(str(case_plan_path))
        apply_meta = run_pwsh_json(
            scripts_root_win + r"\normalize_case_dirs.ps1",
            ["-PlanJsonl", dir_plan_win, "-OpsRoot", wsl_to_windows_path(str(ops_root))],
        )
        raw_apply_win = str(apply_meta.get("out_jsonl") or "")
        raw_apply_path = windows_to_wsl_path(raw_apply_win) if raw_apply_win else ""
        if not raw_apply_path or not os.path.exists(raw_apply_path):
            errors.append("normalize_case_dirs.ps1 did not return valid out_jsonl")
        else:
            for rec in iter_jsonl(raw_apply_path):
                if rec.get("_meta") is not None:
                    continue
                ok = bool(rec.get("ok"))
                if ok:
                    normalized_dirs += 1
                else:
                    err = str(rec.get("error") or "normalize_failed")
                    errors.append(err)
                apply_rows.append(rec)

    if args.apply and file_rows and not errors:
        with synthetic_move_apply_path.open("w", encoding="utf-8") as w:
            w.write(safe_json({"_meta": {"kind": "move_apply", "generated_at": now_iso(), "source": "normalize_folder_case_from_plan.py"}}) + "\n")
            for row in file_rows:
                w.write(safe_json({"op": "move", "ok": True, **row}) + "\n")

        here = Path(__file__).resolve().parent
        db_res, db_stdout, db_stderr, db_rc = run_uv_python_json(
            here / "update_db_paths_from_move_apply.py",
            [
                "--db",
                db_path,
                "--applied",
                str(synthetic_move_apply_path),
                "--run-kind",
                "normalize_case",
                "--notes",
                f"normalize_folder_case_from_plan {Path(str(synthetic_move_apply_path)).name}",
            ],
            cwd=str(here),
        )
        if db_rc != 0:
            errors.append(f"update_db_paths_from_move_apply failed rc={db_rc}: {(db_stderr or db_stdout).strip()}")
        else:
            db_updated_paths = int((db_res or {}).get("updated") or 0)

    if args.apply:
        with case_apply_path.open("w", encoding="utf-8") as w:
            w.write(
                safe_json(
                    {
                        "_meta": {
                            "kind": "normalize_case_apply",
                            "generated_at": now_iso(),
                            "planPath": str(case_plan_path),
                            "rows": len(apply_rows),
                        }
                    }
                )
                + "\n"
            )
            for row in apply_rows:
                w.write(safe_json(row) + "\n")

    summary = {
        "ok": len(errors) == 0,
        "tool": "video_pipeline_normalize_folder_case",
        "apply": bool(args.apply),
        "planPath": str(case_plan_path),
        "applyPath": str(case_apply_path) if args.apply else None,
        "inputRelocatePlanPath": plan_path,
        "caseCandidateDirs": len(dir_pairs),
        "caseCandidateFiles": len(file_rows),
        "normalizedDirs": normalized_dirs if args.apply else 0,
        "dbUpdatedPaths": db_updated_paths if args.apply else 0,
        "errors": errors,
    }
    print(safe_json(summary))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
