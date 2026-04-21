#!/usr/bin/env python3
r"""End-to-end pipeline to analyze and move videos between configured roots.

Runs from WSL/OpenClaw, and performs Windows file operations via pwsh scripts
under <windowsOpsRoot>/scripts using configured sourceRoot/destRoot.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from move_apply_stats import aggregate_move_apply
from windows_pwsh_bridge import canonicalize_windows_path, run_pwsh_json


def run_py(cmd: list[str], env: dict[str, str] | None = None, cwd: str | None = None) -> str:
    run_env = dict(os.environ)
    run_env.setdefault("PYTHONUTF8", "1")
    run_env.setdefault("PYTHONIOENCODING", "utf-8")
    if env:
        run_env.update(env)

    cp = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
        env=run_env,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    # Do not NFKC-normalize subprocess output.  NFKC converts full-width
    # characters that are valid in Windows filenames (e.g. ／ U+FF0F → /,
    # ～ U+FF5E → ~), corrupting paths stored in JSONL/DB.
    # See windows_pwsh_bridge.py for the same rationale.
    out = cp.stdout
    if cp.returncode != 0:
        raise RuntimeError(out.strip() or f"python failed rc={cp.returncode}: {' '.join(cmd)}")
    return out


def run_py_uv(script: Path, args: list[str], cwd: str | None = None) -> str:
    return run_py(["uv", "run", "python", str(script), *args], cwd=cwd)


def parse_last_json_object_line(output: str) -> dict[str, Any] | None:
    if not isinstance(output, str):
        return None
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def string_array(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def aggregate_review_summaries(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "rowsNeedingReview": 0,
        "requiredFieldMissingRows": 0,
        "invalidAirDateRows": 0,
        "needsReviewFlagRows": 0,
        "suspiciousProgramTitleRows": 0,
        "fieldCounts": {},
        "reasonCounts": {},
    }
    for result in results:
        review_summary = result.get("reviewSummary")
        if not isinstance(review_summary, dict):
            continue
        summary["rowsNeedingReview"] += int(review_summary.get("rowsNeedingReview") or 0)
        summary["requiredFieldMissingRows"] += int(review_summary.get("requiredFieldMissingRows") or 0)
        summary["invalidAirDateRows"] += int(review_summary.get("invalidAirDateRows") or 0)
        summary["needsReviewFlagRows"] += int(review_summary.get("needsReviewFlagRows") or 0)
        summary["suspiciousProgramTitleRows"] += int(review_summary.get("suspiciousProgramTitleRows") or 0)
        for key, value in (review_summary.get("fieldCounts") or {}).items():
            if isinstance(key, str):
                summary["fieldCounts"][key] = int(summary["fieldCounts"].get(key, 0)) + int(value or 0)
        for key, value in (review_summary.get("reasonCounts") or {}).items():
            if isinstance(key, str):
                summary["reasonCounts"][key] = int(summary["reasonCounts"].get(key, 0)) + int(value or 0)
    return summary


def export_review_yaml_artifacts(
    output_jsonl_paths: list[str],
    export_script: Path,
    *,
    cwd: str,
    runner: Callable[[Path, list[str], str | None], str] = run_py_uv,
) -> dict[str, Any]:
    artifacts: list[dict[str, Any]] = []
    review_candidates: list[dict[str, Any]] = []
    review_candidates_truncated = False

    for output_jsonl_path in output_jsonl_paths:
        raw = runner(
            export_script,
            [
                "--source-jsonl",
                output_jsonl_path,
                "--only-if-reviewable",
            ],
            cwd,
        )
        parsed = parse_last_json_object_line(raw)
        if not isinstance(parsed, dict) or parsed.get("ok") is not True:
            return {
                "ok": False,
                "error": f"failed to export review YAML for {output_jsonl_path}",
                "sourceJsonlPath": output_jsonl_path,
                "raw": raw,
                "parsed": parsed,
            }
        if not parsed.get("outputPath"):
            continue
        artifacts.append(parsed)
        for candidate in parsed.get("reviewCandidates") or []:
            if len(review_candidates) < 50 and isinstance(candidate, dict):
                review_candidates.append(candidate)
            else:
                review_candidates_truncated = True
        if parsed.get("reviewCandidatesTruncated") is True:
            review_candidates_truncated = True

    review_yaml_paths = [str(result["outputPath"]) for result in artifacts if isinstance(result.get("outputPath"), str)]
    return {
        "ok": True,
        "reviewArtifacts": artifacts,
        "reviewYamlPaths": review_yaml_paths,
        "reviewYamlPath": review_yaml_paths[0] if len(review_yaml_paths) == 1 else None,
        "reviewSummary": aggregate_review_summaries(artifacts),
        "reviewCandidates": review_candidates,
        "reviewCandidatesTruncated": review_candidates_truncated,
    }


def latest(move_dir: Path, glob_pat: str) -> Path:
    files = sorted(move_dir.glob(glob_pat), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No files match: {move_dir}/{glob_pat}")
    return files[0]


def wsl_to_win(path: Path) -> str:
    s = str(path)
    if s.startswith("/mnt/") and len(s) > 6 and s[6] == "/":
        drive = s[5].upper()
        return f"{drive}:\\" + s.split(f"/mnt/{s[5]}/", 1)[1].replace("/", "\\")
    raise ValueError(f"not on /mnt/<drive>: {path}")


def win_to_wsl(path_str: str) -> str:
    s = str(path_str or "").replace("/", "\\")
    if len(s) >= 3 and s[1] == ":" and s[2] == "\\":
        drive = s[0].lower()
        tail = s[3:].replace("\\", "/")
        return f"/mnt/{drive}/{tail}"
    return path_str


def local_path_from_any(path_str: str) -> Path:
    """Accept either /mnt/<drive>/... or X:\\... and return a local WSL Path."""
    return Path(win_to_wsl(path_str)).resolve()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--windows-ops-root", default="")
    ap.add_argument("--source-root", default="")
    ap.add_argument("--dest-root", default="")
    ap.add_argument("--max-files-per-run", type=int, default=20)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--allow-needs-review", action="store_true")
    ap.add_argument("--keep-batches", type=int, default=5)
    ap.add_argument("--ttl-days", type=int, default=30)
    ap.add_argument("--drive-routes", default="", help="Path to drive_routes.yaml for multi-dest routing")
    args = ap.parse_args()

    if not args.windows_ops_root:
        raise SystemExit("windowsOpsRoot is required: pass --windows-ops-root or configure plugin windowsOpsRoot")
    if not args.source_root:
        raise SystemExit("sourceRoot is required: pass --source-root or configure plugin sourceRoot")
    if not args.dest_root:
        raise SystemExit("destRoot is required: pass --dest-root or configure plugin destRoot")

    ops_root = local_path_from_any(args.windows_ops_root)
    db_dir = ops_root / "db"
    move_dir = ops_root / "move"
    llm_dir = ops_root / "llm"
    if args.db:
        args.db = str(local_path_from_any(args.db))
    else:
        args.db = str(db_dir / "mediaops.sqlite")

    source_root_win = canonicalize_windows_path(args.source_root)
    dest_root_win = canonicalize_windows_path(args.dest_root)
    ops_root_win = canonicalize_windows_path(args.windows_ops_root)
    scripts_root_win = canonicalize_windows_path(str(ops_root / "scripts"))
    db_dir.mkdir(parents=True, exist_ok=True)
    move_dir.mkdir(parents=True, exist_ok=True)
    llm_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    here = Path(__file__).resolve().parent
    hints_yaml = here.parent / "rules" / "program_aliases.yaml"
    export_yaml_script = here / "export_program_yaml.py"

    inv_meta = run_pwsh_json(
        scripts_root_win + r"\unwatched_inventory.ps1",
        ["-Root", source_root_win, "-OpsRoot", ops_root_win, "-IncludeHash"],
    )
    inv_out = str(inv_meta.get("out_jsonl") or "")
    inv = Path(win_to_wsl(inv_out)).resolve() if inv_out else latest(move_dir, "inventory_unwatched_*.jsonl")

    run_py_uv(
        here / "ingest_inventory_jsonl.py",
        ["--db", args.db, "--jsonl", str(inv), "--target-root", source_root_win],
        cwd=str(here),
    )

    queue = llm_dir / f"queue_unwatched_batch_{ts}.jsonl"
    run_py_uv(
        here / "make_metadata_queue_from_inventory.py",
        [
            "--db",
            args.db,
            "--inventory",
            str(inv),
            "--source-root",
            source_root_win,
            "--out",
            str(queue),
            "--limit",
            str(args.max_files_per_run),
        ],
        cwd=str(here),
    )

    reextract_raw = run_py_uv(
        here / "run_metadata_batches_promptv1.py",
        [
            "--db",
            args.db,
            "--queue",
            str(queue),
            "--outdir",
            str(llm_dir),
            "--hints",
            str(hints_yaml),
            "--batch-size",
            "50",
            "--start-batch",
            "1",
        ],
        cwd=str(here),
    )
    reextract_parsed = parse_last_json_object_line(reextract_raw) or {}
    reextract_output_jsonl_paths = string_array(reextract_parsed.get("outputJsonlPaths"))
    latest_output_jsonl_path = reextract_parsed.get("latestOutputJsonlPath")
    if not reextract_output_jsonl_paths and isinstance(latest_output_jsonl_path, str) and latest_output_jsonl_path:
        reextract_output_jsonl_paths = [latest_output_jsonl_path]

    plan_cmd = [
        "--db",
        args.db,
        "--inventory",
        str(inv),
        "--source-root",
        source_root_win,
        "--dest-root",
        dest_root_win,
        "--limit",
        str(args.max_files_per_run),
    ]
    if args.drive_routes:
        plan_cmd.extend(["--drive-routes", args.drive_routes])
    if args.allow_needs_review:
        plan_cmd.insert(-2, "--allow-needs-review")
    plan_out_raw = run_py_uv(here / "make_move_plan_from_inventory.py", plan_cmd, cwd=str(here))
    try:
        plan_out = json.loads(plan_out_raw)
    except Exception:
        plan_out = {"raw": plan_out_raw}
    plan_out_path = str(plan_out.get("out") or "") if isinstance(plan_out, dict) else ""
    plan = Path(plan_out_path).resolve() if plan_out_path else latest(move_dir, "move_plan_from_inventory_*.jsonl")

    inv_count = sum(1 for ln in inv.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip())
    plan_stats = plan_out if isinstance(plan_out, dict) else {}
    planned = int(plan_stats.get("planned", 0))

    summary: dict[str, Any] = {
        "ok": True,
        "inventory": str(inv),
        "queue": str(queue),
        "plan": str(plan),
        "plan_stats": plan_out,
        "apply": bool(args.apply),
        "windows_ops_root": str(ops_root),
        "max_files_per_run": int(args.max_files_per_run),
        "reextract": {
            "ok": True,
            "queuePath": str(queue),
            "summary": reextract_parsed,
            "outputJsonlPaths": reextract_output_jsonl_paths,
            "outputJsonlPath": reextract_output_jsonl_paths[-1] if reextract_output_jsonl_paths else None,
            "raw": reextract_raw,
        },
        "workflowState": "relocate_plan_ready",
    }

    if reextract_output_jsonl_paths:
        review_export = export_review_yaml_artifacts(
            reextract_output_jsonl_paths,
            export_yaml_script,
            cwd=str(here),
        )
        if not review_export.get("ok"):
            summary["ok"] = False
            summary["workflowState"] = "review_yaml_export_failed"
            summary["error"] = review_export.get("error") or "failed to export review yaml artifacts"
            summary["reviewExport"] = review_export
            print(json.dumps(summary, ensure_ascii=False))
            return 1
        summary.update(review_export)
        if summary.get("reviewYamlPaths"):
            review_summary = summary.get("reviewSummary") or {}
            summary["workflowState"] = "metadata_review_required"
            summary["nextStep"] = (
                "Human review required for extracted metadata. "
                f"Review reviewYamlPath/reviewYamlPaths and apply corrections before relying on apply=true. "
                f"rowsNeedingReview={int((review_summary or {}).get('rowsNeedingReview') or 0)}."
            )

    if args.apply and planned > 0:
        # Stage 3: 実際にファイルを移動
        apply_args = ["-PlanJsonl", wsl_to_win(plan), "-OpsRoot", ops_root_win]
        apply_meta = run_pwsh_json(scripts_root_win + r"\apply_move_plan.ps1", apply_args)
        applied_out = str(apply_meta.get("out_jsonl") or "")
        applied = Path(win_to_wsl(applied_out)).resolve() if applied_out else latest(move_dir, "move_apply_*.jsonl")

        run_py_uv(
            here / "update_db_paths_from_move_apply.py",
            ["--db", args.db, "--applied", str(applied)],
            cwd=str(here),
        )

        run_py_uv(
            here / "rotate_move_audit_logs.py",
            [
                "--move-dir",
                str(move_dir),
                "--llm-dir",
                str(llm_dir),
                "--keep-batches",
                str(args.keep_batches),
                "--ttl-days",
                str(args.ttl_days),
            ],
            cwd=str(here),
        )

        move_stats = aggregate_move_apply(str(applied))
        summary["applied"] = str(applied)
        summary["moveApplyStats"] = move_stats
        summary["remaining_files"] = max(inv_count - move_stats["succeeded"], 0)
        if summary.get("reviewYamlPaths"):
            summary["nextStep"] = (
                "Moves were applied for ready rows, but reviewable metadata remains. "
                "Review reviewYamlPath/reviewYamlPaths and apply corrections before the next run."
            )
    else:
        # Dry-run: plan のみ。apply は実行しない。
        summary["remaining_files"] = inv_count
        if not summary.get("reviewYamlPaths") and planned > 0:
            summary["nextStep"] = (
                f"Dry-run complete. {planned} files planned for move and no review YAML was needed. "
                "Present the plan before calling this runner with --apply."
            )
        elif not summary.get("reviewYamlPaths") and planned == 0:
            summary["workflowState"] = "no_moves_planned"
            summary["nextStep"] = "Dry-run complete. No files planned for move. Check plan_stats for skip reasons."

    print(
        json.dumps(summary, ensure_ascii=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
