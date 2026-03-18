#!/usr/bin/env python3
"""Normalize case-only directory name differences based on DB metadata destination rules."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mediaops_schema import connect_db
from path_placement_rules import build_expected_dest_path, build_routed_dest_path, load_drive_routes
from pathscan_common import canonicalize_windows_path, now_iso, safe_json, split_win, ts_compact, windows_to_wsl_path
from windows_pwsh_bridge import run_pwsh_json


@dataclass
class Candidate:
    src_dir: str
    dst_dir: str
    depth: int


def _split_dirs(win_path: str) -> list[str]:
    return canonicalize_windows_path(win_path).split("\\")


def _starts_with_prefix(path_win: str, prefixes: list[str]) -> bool:
    p = canonicalize_windows_path(path_win).lower()
    for pref in prefixes:
        x = canonicalize_windows_path(pref).rstrip("\\").lower()
        if p == x or p.startswith(x + "\\"):
            return True
    return False


def _iter_case_dir_candidates(src_path: str, dst_path: str) -> list[Candidate]:
    src_parts = _split_dirs(src_path)
    dst_parts = _split_dirs(dst_path)
    if len(src_parts) != len(dst_parts):
        return []
    out: list[Candidate] = []
    # filename is excluded (last segment)
    for i in range(1, len(src_parts) - 1):
        s = src_parts[i]
        d = dst_parts[i]
        if s == d:
            continue
        if s.lower() != d.lower():
            continue
        src_dir = "\\".join(src_parts[: i + 1])
        dst_dir = "\\".join(dst_parts[: i + 1])
        out.append(Candidate(src_dir=src_dir, dst_dir=dst_dir, depth=i + 1))
    return out



def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--dest-root", required=True)
    ap.add_argument("--drive-routes", default="")
    ap.add_argument("--roots-json", default="")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--plan-path", default="")
    args = ap.parse_args()

    ops_root = Path(windows_to_wsl_path(args.windows_ops_root)).resolve()
    move_dir = ops_root / "move"
    move_dir.mkdir(parents=True, exist_ok=True)

    ts = ts_compact()
    plan_path = Path(windows_to_wsl_path(args.plan_path)).resolve() if args.plan_path else (move_dir / f"folder_case_plan_{ts}.jsonl")

    if args.apply:
        if not plan_path.exists():
            raise SystemExit(f"plan not found: {plan_path}")
        apply_ps1 = Path(windows_to_wsl_path(args.windows_ops_root)) / "scripts" / "apply_case_rename_plan.ps1"
        if not apply_ps1.exists():
            raise SystemExit(f"windows script not found: {apply_ps1}")
        applied = run_pwsh_json(
            str(apply_ps1),
            [
                "-PlanJsonl",
                str(plan_path),
                "-OpsRoot",
                str(args.windows_ops_root),
            ],
            normalize_args=True,
        )
        print(
            safe_json(
                {
                    "ok": bool(applied.get("ok", True)),
                    "apply": True,
                    "planPath": str(plan_path),
                    "appliedPath": applied.get("out_jsonl"),
                    "appliedCount": int(applied.get("renamed", 0) or 0),
                    "skippedCount": int(applied.get("skipped", 0) or 0),
                    "errorCount": int(applied.get("errors", 0) or 0),
                    "runId": applied.get("run_id"),
                }
            )
        )
        return 0 if bool(applied.get("ok", True)) else 1

    db_path = windows_to_wsl_path(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {args.db}")

    con = connect_db(db_path)
    rows = con.execute(
        """
        SELECT p.path_id, p.path, pm.program_title, pm.air_date
        FROM file_paths fp
        JOIN paths p ON p.path_id = fp.path_id
        LEFT JOIN path_metadata pm ON pm.path_id = p.path_id
        WHERE fp.is_current = 1
          AND pm.program_title IS NOT NULL
          AND pm.program_title != ''
        """
    ).fetchall()

    roots: list[str] = []
    if args.roots_json:
        try:
            v = json.loads(args.roots_json)
            if isinstance(v, list):
                roots = [str(x) for x in v if str(x).strip()]
        except Exception:
            roots = []
    if not roots:
        roots = [str(args.dest_root)]

    routes = load_drive_routes(args.drive_routes) if args.drive_routes else []

    scanned = 0
    case_path_matches = 0
    dir_candidate_count = 0
    conflicts = 0
    candidate_map: dict[str, Candidate] = {}

    for row in rows:
        scanned += 1
        src = canonicalize_windows_path(str(row["path"] or ""))
        if not src or not _starts_with_prefix(src, roots):
            continue
        md = {
            "program_title": row["program_title"],
            "air_date": row["air_date"],
        }
        if routes:
            dst, _route, err = build_routed_dest_path(routes, src, md)
        else:
            dst, err = build_expected_dest_path(args.dest_root, src, md)
        if err or not dst:
            continue
        dst = canonicalize_windows_path(dst)
        if src == dst or src.lower() != dst.lower():
            continue
        case_path_matches += 1
        for cand in _iter_case_dir_candidates(src, dst):
            dir_candidate_count += 1
            key = cand.src_dir.lower()
            existing = candidate_map.get(key)
            if existing is None:
                candidate_map[key] = cand
                continue
            if canonicalize_windows_path(existing.dst_dir) != canonicalize_windows_path(cand.dst_dir):
                conflicts += 1

    candidates = sorted(candidate_map.values(), key=lambda x: (-x.depth, x.src_dir.lower()))

    with plan_path.open("w", encoding="utf-8") as w:
        w.write(
            safe_json(
                {
                    "_meta": {
                        "kind": "folder_case_plan",
                        "generated_at": now_iso(),
                        "db": db_path,
                        "dest_root": canonicalize_windows_path(args.dest_root),
                        "roots": roots,
                    }
                }
            )
            + "\n"
        )
        for c in candidates:
            drv, parent, _name, _ext = split_win(c.src_dir)
            w.write(
                safe_json(
                    {
                        "src_dir": c.src_dir,
                        "dst_dir": c.dst_dir,
                        "parent_dir": f"{drv}\\{parent}" if drv and parent else parent,
                        "depth": c.depth,
                        "reason": "case_only_directory_normalization",
                    }
                )
                + "\n"
            )

    summary = {
        "ok": True,
        "apply": False,
        "planPath": str(plan_path),
        "scannedFiles": scanned,
        "casePathMatches": case_path_matches,
        "directoryCandidates": dir_candidate_count,
        "plannedRenames": len(candidates),
        "conflictCount": conflicts,
    }
    print(safe_json(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
