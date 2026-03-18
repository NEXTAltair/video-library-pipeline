#!/usr/bin/env python3
"""Normalize directory-name casing on Windows to match DB-derived destination paths."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from db_helpers import reconstruct_path_metadata
from mediaops_schema import connect_db, create_schema_if_needed, fetchall
from path_placement_rules import build_expected_dest_path, build_routed_dest_path, has_required_db_contract, load_drive_routes
from pathscan_common import (
    as_bool,
    canonicalize_windows_path,
    iter_jsonl,
    now_iso,
    parse_json_arg,
    parse_simple_yaml_lists,
    safe_json,
    split_win,
    ts_compact,
    windows_to_wsl_path,
)
from windows_pwsh_bridge import run_pwsh_json


@dataclass(frozen=True)
class CaseRenameOp:
    src_dir: str
    dst_dir: str


def _path_in_roots(win_path: str, roots_win: list[str]) -> bool:
    """Return True if win_path is under any of roots_win (or roots_win is empty)."""
    if not roots_win:
        return True
    p_lower = win_path.lower()
    for root in roots_win:
        root_lower = root.rstrip("\\").lower()
        if p_lower == root_lower or p_lower.startswith(root_lower + "\\"):
            return True
    return False


def _query_current_paths_with_metadata(con) -> list:
    """Single CTE JOIN query: replaces per-file N+1 DB lookups."""
    return fetchall(
        con,
        """
        WITH latest AS (
            SELECT path_id, MAX(updated_at) AS max_ts
            FROM path_metadata
            WHERE program_title IS NOT NULL AND program_title != ''
            GROUP BY path_id
        )
        SELECT
            p.path_id,
            p.path,
            pm.source,
            pm.program_title,
            pm.air_date,
            pm.needs_review,
            pm.episode_no,
            pm.subtitle,
            pm.broadcaster,
            pm.human_reviewed,
            pm.data_json
        FROM file_paths fp
        JOIN paths p ON p.path_id = fp.path_id
        JOIN latest lm ON lm.path_id = p.path_id
        JOIN path_metadata pm
          ON pm.path_id = lm.path_id AND pm.updated_at = lm.max_ts
        WHERE fp.is_current = 1
        """,
    )


def split_parts(win_path: str) -> tuple[str, list[str]]:
    p = canonicalize_windows_path(win_path)
    drive, _dir, _name, _ext = split_win(p)
    if not drive:
        return "", []
    rest = p[len(drive):]
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
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--plan-path", default="")
    ap.add_argument("--allow-needs-review", default="false")
    ap.add_argument("--allow-unreviewed-metadata", default="false")
    ap.add_argument("--drive-routes", default="", help="Path to drive_routes.yaml for multi-dest routing")
    args = ap.parse_args()

    # --apply requires --plan-path to enforce the dry-run → review → apply workflow
    if args.apply and not (args.plan_path or "").strip():
        raise SystemExit(
            "--plan-path is required when --apply is set. "
            "Run without --apply first to generate a plan, review it, then pass it with --apply."
        )

    db_path = windows_to_wsl_path(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(windows_to_wsl_path(args.windows_ops_root)).resolve()
    move_dir = ops_root / "move"
    scripts_dir = ops_root / "scripts"
    move_dir.mkdir(parents=True, exist_ok=True)

    allow_needs_review = as_bool(args.allow_needs_review, False)
    allow_unreviewed_metadata = as_bool(args.allow_unreviewed_metadata, False)

    plan_path_arg = canonicalize_windows_path(str(args.plan_path or "").strip())
    # When --apply is set, --plan-path is guaranteed non-empty (validated above)
    use_existing_plan = bool(args.apply and plan_path_arg)

    con = connect_db(db_path)
    create_schema_if_needed(con)

    routes = None
    if args.drive_routes and os.path.exists(args.drive_routes):
        routes = load_drive_routes(args.drive_routes)

    plan_path = (
        Path(windows_to_wsl_path(plan_path_arg))
        if use_existing_plan
        else (move_dir / f"case_normalize_plan_{ts_compact()}.jsonl")
    )
    if use_existing_plan and not plan_path.exists():
        raise SystemExit(f"plan file not found: {plan_path_arg}")

    case_ops: dict[tuple[str, str], CaseRenameOp] = {}
    metadata_skipped = 0
    already_correct = 0
    case_mismatch_files = 0
    db_queried_files = 0

    if not use_existing_plan:
        # Resolve roots for filtering (DB-first: no filesystem scan needed)
        roots_win: list[str] = []
        roots_param = parse_json_arg(args.roots_json, None)
        if roots_param is not None:
            if not isinstance(roots_param, list):
                raise SystemExit("--roots-json must be a JSON array")
            roots_win = [canonicalize_windows_path(str(r)) for r in roots_param if str(r).strip()]
        else:
            rfp = str(args.roots_file_path or "").strip()
            if rfp:
                parsed = parse_simple_yaml_lists(Path(windows_to_wsl_path(rfp)))
                roots_yaml = parsed.get("roots", [])
                if not isinstance(roots_yaml, list):
                    raise SystemExit("rootsFilePath must contain list key 'roots'")
                roots_win = [canonicalize_windows_path(str(r)) for r in roots_yaml if str(r).strip()]

        # Single JOIN query replaces per-file N+1 lookups
        db_rows = _query_current_paths_with_metadata(con)

        processed = 0
        for row in db_rows:
            win_path = canonicalize_windows_path(str(row["path"] or ""))
            if not win_path:
                continue
            if not _path_in_roots(win_path, roots_win):
                continue

            db_queried_files += 1

            md = reconstruct_path_metadata(row)
            md_source = str(row["source"]) if row["source"] is not None else None

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
                dst, _genre, dst_err = build_routed_dest_path(routes, win_path, md)
            else:
                dst, dst_err = build_expected_dest_path(args.dest_root, win_path, md)
            if not dst or dst_err:
                continue

            src = win_path
            dst = canonicalize_windows_path(dst)

            if src == dst:
                already_correct += 1
                continue
            if src.lower() != dst.lower():
                continue

            case_mismatch_files += 1
            # Extract the directory portion of src and dst paths
            src_drive, src_parts = split_parts(src)
            dst_drive, dst_parts = split_parts(dst)
            if src_parts and dst_parts:
                src_dir = join_parts(src_drive, src_parts[:-1]) if len(src_parts) > 1 else src_drive
                dst_dir = join_parts(dst_drive, dst_parts[:-1]) if len(dst_parts) > 1 else dst_drive
                for op in derive_case_ops(src_dir, dst_dir):
                    case_ops[(op.src_dir.lower(), op.dst_dir.lower())] = op

            processed += 1
            if args.limit > 0 and processed >= args.limit:
                break

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
                            "roots": roots_win,
                            "dest_root": args.dest_root,
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
        "ok": failed_ops == 0,
        "mode": "apply" if args.apply else "dry_run",
        "plan_path": str(plan_path),
        "dbQueriedFiles": db_queried_files,
        "plannedCaseMismatchFiles": case_mismatch_files,
        "plannedRenameDirs": len(sorted_ops),
        "alreadyCorrect": already_correct,
        "metadataSkipped": metadata_skipped,
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
