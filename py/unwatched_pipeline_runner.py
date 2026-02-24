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
import unicodedata
from datetime import datetime
from pathlib import Path

from windows_pwsh_bridge import canonicalize_windows_path, run_pwsh_file, run_pwsh_json


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
    out = unicodedata.normalize("NFKC", cp.stdout)
    if cp.returncode != 0:
        raise RuntimeError(out.strip() or f"python failed rc={cp.returncode}: {' '.join(cmd)}")
    return out


def run_py_uv(script: Path, args: list[str], cwd: str | None = None) -> str:
    return run_py(["uv", "run", "python", str(script), *args], cwd=cwd)


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

    normalize_args = ["-Root", source_root_win, "-OpsRoot", ops_root_win]
    if not args.apply:
        normalize_args.append("-DryRun")
    run_pwsh_file(scripts_root_win + r"\normalize_filenames.ps1", normalize_args)

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

    run_py_uv(
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
    if args.allow_needs_review:
        plan_cmd.insert(-2, "--allow-needs-review")
    plan_out_raw = run_py_uv(here / "make_move_plan_from_inventory.py", plan_cmd, cwd=str(here))
    try:
        plan_out = json.loads(plan_out_raw)
    except Exception:
        plan_out = {"raw": plan_out_raw}
    plan_out_path = str(plan_out.get("out") or "") if isinstance(plan_out, dict) else ""
    plan = Path(plan_out_path).resolve() if plan_out_path else latest(move_dir, "move_plan_from_inventory_*.jsonl")

    apply_args = ["-PlanJsonl", wsl_to_win(plan), "-OpsRoot", ops_root_win]
    if not args.apply:
        apply_args.append("-DryRun")
    apply_meta = run_pwsh_json(scripts_root_win + r"\apply_move_plan.ps1", apply_args)
    applied_out = str(apply_meta.get("out_jsonl") or "")
    applied = Path(win_to_wsl(applied_out)).resolve() if applied_out else latest(move_dir, "move_apply_*.jsonl")

    run_py_uv(
        here / "update_db_paths_from_move_apply.py",
        ["--db", args.db, "--applied", str(applied)],
        cwd=str(here),
    )

    remaining_txt = move_dir / f"remaining_unwatched_{ts}.txt"
    out = run_pwsh_file(
        scripts_root_win + r"\list_remaining_unwatched.ps1",
        ["-Root", source_root_win],
    )
    remaining_txt.write_text(out, encoding="utf-8")

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

    lines = [ln for ln in remaining_txt.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    summary = {
        "inventory": str(inv),
        "queue": str(queue),
        "plan": str(plan),
        "plan_stats": plan_out,
        "applied": str(applied),
        "apply": bool(args.apply),
        "remaining_files": len(lines),
        "windows_ops_root": str(ops_root),
        "max_files_per_run": int(args.max_files_per_run),
    }

    print(
        json.dumps(summary, ensure_ascii=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
