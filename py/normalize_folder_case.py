#!/usr/bin/env python3
"""Normalize directory-name casing on Windows to match DB-derived destination paths."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import Any

from db_helpers import reconstruct_path_metadata
from mediaops_schema import connect_db, create_schema_if_needed, fetchone
from path_placement_rules import build_expected_dest_path, build_routed_dest_path, has_required_db_contract, load_drive_routes
from pathscan_common import (
    DEFAULT_EXTENSIONS,
    as_bool,
    canonicalize_windows_path,
    ensure_exts,
    iter_jsonl,
    now_iso,
    parse_json_arg,
    parse_simple_yaml_lists,
    safe_json,
    scan_files,
    split_win,
    ts_compact,
    windows_to_wsl_path,
)
from windows_pwsh_bridge import run_pwsh_json


@dataclass(frozen=True)
class CaseRenameOp:
    src_dir: str
    dst_dir: str


def latest_metadata_for_path(con, path_id: str) -> tuple[dict[str, Any] | None, str | None]:
    row = fetchone(
        con,
        """
        SELECT source, data_json, program_title, air_date, needs_review,
               episode_no, subtitle, broadcaster, human_reviewed
        FROM path_metadata
        WHERE path_id=?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (path_id,),
    )
    if not row:
        return None, None
    md = reconstruct_path_metadata(row)
    return md if md else None, str(row["source"]) if row["source"] is not None else None


def split_parts(win_path: str) -> tuple[str, list[str]]:
    p = canonicalize_windows_path(win_path)
    drive, _dir, _name, _ext = split_win(p)
    if not drive:
        return "", []
    rest = p[len(drive) :]
    parts = [seg for seg in rest.split("\\") if seg]
    return drive.upper(), parts


def join_parts(drive: str, parts: list[str]) -> str:
    if not parts:
        return drive
    return f"{drive}\\" + "\\".join(parts)


def derive_case_ops(src_dir: str, dst_dir: str) -> list[CaseRenameOp]:
    src = canonicalize_windows_path(src_dir)
    dst = canonicalize_windows_path(dst_dir)
    if src == dst or src.lower() != dst.lower():
        return []

    s_drive, s_parts = split_parts(src)
    d_drive, d_parts = split_parts(dst)
    if not s_drive or s_drive.lower() != d_drive.lower() or len(s_parts) != len(d_parts):
        return []

    effective = list(s_parts)
    ops: list[CaseRenameOp] = []
    for i, (s_seg, d_seg) in enumerate(zip(s_parts, d_parts)):
        if s_seg == d_seg:
            continue
        if s_seg.lower() != d_seg.lower():
            return []
        old_dir = join_parts(s_drive, effective[: i + 1])
        effective[i] = d_seg
        new_dir = join_parts(s_drive, effective[: i + 1])
        ops.append(CaseRenameOp(src_dir=old_dir, dst_dir=new_dir))
    return ops


def update_db_case_prefix(con, old_prefix: str, new_prefix: str) -> int:
    old_prefix = canonicalize_windows_path(old_prefix)
    new_prefix = canonicalize_windows_path(new_prefix)
    if old_prefix.lower() != new_prefix.lower() or old_prefix == new_prefix:
        return 0

    like_pattern = old_prefix + "\\%"
    rows = con.execute(
        """
        SELECT path_id, path
        FROM paths
        WHERE lower(path)=lower(?) OR lower(path) LIKE lower(?)
        """,
        (old_prefix, like_pattern),
    ).fetchall()

    updated = 0
    old_len = len(old_prefix)
    for row in rows:
        path_id = str(row["path_id"])
        old_path = str(row["path"])
        if old_path.lower() == old_prefix.lower():
            next_path = new_prefix
        else:
            suffix = old_path[old_len:]
            next_path = new_prefix + suffix
        drive, dir_path, name, ext = split_win(next_path)
        con.execute(
            """
            UPDATE paths
            SET path=?, drive=?, dir=?, name=?, ext=?, updated_at=?
            WHERE path_id=?
            """,
            (next_path, drive, dir_path, name, ext, now_iso(), path_id),
        )
        updated += 1
    return updated


def load_plan_ops(plan_path: str) -> list[CaseRenameOp]:
    ops: list[CaseRenameOp] = []
    for row in iter_jsonl(windows_to_wsl_path(plan_path)):
        if not isinstance(row, dict):
            continue
        if row.get("op") != "rename_dir_case":
            continue
        src = canonicalize_windows_path(str(row.get("src") or ""))
        dst = canonicalize_windows_path(str(row.get("dst") or ""))
        if not src or not dst:
            continue
        ops.append(CaseRenameOp(src_dir=src, dst_dir=dst))
    return ops


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
    ap.add_argument("--plan-path", default="")
    ap.add_argument("--allow-needs-review", default="false")
    ap.add_argument("--allow-unreviewed-metadata", default="false")
    ap.add_argument("--drive-routes", default="", help="Path to drive_routes.yaml for multi-dest routing")
    args = ap.parse_args()

    db_path = windows_to_wsl_path(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(windows_to_wsl_path(args.windows_ops_root)).resolve()
    move_dir = ops_root / "move"
    scripts_dir = ops_root / "scripts"
    move_dir.mkdir(parents=True, exist_ok=True)

    allow_needs_review = as_bool(args.allow_needs_review)
    allow_unreviewed_metadata = as_bool(args.allow_unreviewed_metadata)

    roots: list[str] = []
    extensions = list(DEFAULT_EXTENSIONS)
    scanned = []
    scan_warnings: list[str] = []
    scan_errors: list[str] = []

    plan_path_arg = canonicalize_windows_path(str(args.plan_path or "").strip())
    use_existing_plan = bool(args.apply and plan_path_arg)

    if not use_existing_plan:
        roots = parse_json_arg(args.roots_json)
        roots_file_path = windows_to_wsl_path(args.roots_file_path or "")
        if not isinstance(roots, list) or len(roots) == 0:
            roots = parse_simple_yaml_lists(roots_file_path) if roots_file_path else []
        roots = [canonicalize_windows_path(r) for r in roots if isinstance(r, str) and r.strip()]
        if not roots:
            raise SystemExit("roots is required: pass --roots-json or --roots-file-path")

        exts = parse_json_arg(args.extensions_json)
        if not isinstance(exts, list) or not exts:
            exts = list(DEFAULT_EXTENSIONS)
        extensions = ensure_exts(exts)

        scanned, scan_warnings, scan_errors, _fallback_stats = scan_files(
            roots=roots,
            exts=set(extensions),
            detect_corruption=False,
            read_bytes=4096,
            scan_retry_count=1,
            windows_ops_root=str(ops_root),
        )
        if args.limit > 0:
            scanned = scanned[: args.limit]

    con = connect_db(db_path)
    create_schema_if_needed(con)

    routes = None
    if args.drive_routes and os.path.exists(args.drive_routes):
        routes = load_drive_routes(args.drive_routes)

    plan_path = Path(windows_to_wsl_path(plan_path_arg)) if use_existing_plan else (move_dir / f"case_normalize_plan_{ts_compact()}.jsonl")
    if use_existing_plan and not plan_path.exists():
        raise SystemExit(f"plan file not found: {plan_path_arg}")

    rows_for_plan: list[dict[str, Any]] = []
    case_ops: dict[tuple[str, str], CaseRenameOp] = {}
    metadata_skipped = 0
    already_correct = 0
    case_mismatch_files = 0

    for sf in scanned:
        path_row = fetchone(con, "SELECT path_id FROM paths WHERE path = ?", (sf.win_path,))
        if not path_row:
            continue

        path_id = str(path_row["path_id"])
        md, md_source = latest_metadata_for_path(con, path_id)
        if md is None or not has_required_db_contract(md):
            metadata_skipped += 1
            continue
        if bool(md.get("needs_review")) and not allow_needs_review:
            metadata_skipped += 1
            continue

        source_norm = str(md_source or "").strip().lower()
        is_human_reviewed = source_norm == "human_reviewed"
        is_llm = source_norm in {"llm", "llm_subagent"}
        if not is_human_reviewed and not is_llm and not allow_unreviewed_metadata:
            metadata_skipped += 1
            continue

        if routes:
            dst, _genre, dst_err = build_routed_dest_path(routes, sf.win_path, md)
        else:
            dst, dst_err = build_expected_dest_path(args.dest_root, sf.win_path, md)
        if not dst or dst_err:
            continue

        src = canonicalize_windows_path(sf.win_path)
        dst = canonicalize_windows_path(dst)

        if src == dst:
            already_correct += 1
            continue
        if src.lower() != dst.lower():
            continue

        case_mismatch_files += 1
        src_dir = str(PureWindowsPath(src).parent)
        dst_dir = str(PureWindowsPath(dst).parent)
        ops = derive_case_ops(src_dir, dst_dir)
        for op in ops:
            case_ops[(op.src_dir.lower(), op.dst_dir.lower())] = op
        rows_for_plan.append(
            {
                "path_id": path_id,
                "src": src,
                "dst": dst,
                "status": "planned",
                "reason": "case_only_path_mismatch",
                "program_title": md.get("program_title"),
                "air_date": md.get("air_date"),
                "metadata_source": md_source,
                "ts": now_iso(),
            }
        )

    sorted_ops = sorted(case_ops.values(), key=lambda x: (x.src_dir.count("\\"), x.src_dir.lower()))

    if not use_existing_plan:
        with plan_path.open("w", encoding="utf-8") as w:
            w.write(
                safe_json(
                    {
                        "_meta": {
                            "kind": "case_normalize_plan",
                            "generated_at": now_iso(),
                            "db": db_path,
                            "roots": roots,
                            "dest_root": args.dest_root,
                            "apply": bool(args.apply),
                            "extensions": extensions,
                            "allow_needs_review": allow_needs_review,
                            "allow_unreviewed_metadata": allow_unreviewed_metadata,
                        }
                    }
                )
                + "\n"
            )
            for op in sorted_ops:
                w.write(safe_json({"op": "rename_dir_case", "src": op.src_dir, "dst": op.dst_dir}) + "\n")
    else:
        sorted_ops = load_plan_ops(str(plan_path))

    applied_ops = 0
    failed_ops = 0
    db_updated_rows = 0
    apply_log_path: str | None = None
    apply_errors: list[str] = []

    if args.apply and sorted_ops:
        script_path = scripts_dir / "normalize_case_dirs.ps1"
        if not script_path.exists():
            raise SystemExit(f"normalize_case_dirs.ps1 not found: {script_path}")

        apply_result = run_pwsh_json(
            str(script_path),
            [
                "-PlanJsonl",
                str(plan_path),
                "-OpsRoot",
                str(ops_root),
            ],
        )
        apply_log_path = str(apply_result.get("out_jsonl") or "") or None

        if apply_log_path:
            for row in iter_jsonl(windows_to_wsl_path(apply_log_path)):
                if not isinstance(row, dict):
                    continue
                if row.get("op") != "rename_dir_case":
                    continue
                if bool(row.get("ok")):
                    applied_ops += 1
                    db_updated_rows += update_db_case_prefix(con, str(row.get("src") or ""), str(row.get("dst") or ""))
                else:
                    failed_ops += 1
                    err = str(row.get("error") or "rename_failed")
                    apply_errors.append(err)
            con.commit()

    out = {
        "ok": failed_ops == 0 and len(scan_errors) == 0,
        "mode": "apply" if args.apply else "dry_run",
        "plan_path": str(plan_path),
        "plannedCaseMismatchFiles": case_mismatch_files,
        "plannedRenameDirs": len(sorted_ops),
        "alreadyCorrect": already_correct,
        "metadataSkipped": metadata_skipped,
        "scanWarnings": scan_warnings,
        "scanErrors": scan_errors,
        "appliedRenameDirs": applied_ops,
        "failedRenameDirs": failed_ops,
        "dbUpdatedRows": db_updated_rows,
        "apply_log_path": apply_log_path,
        "applyErrors": apply_errors,
    }
    print(safe_json(out))
    return 0 if out["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
