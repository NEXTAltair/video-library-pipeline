#!/usr/bin/env python3
r"""End-to-end pipeline to organize B:\未視聴 into B:\VideoLibrary\by_program.

Runs from WSL/OpenClaw, but performs file operations on Windows via pwsh.exe and
scripts stored under B:\_AI_WORK\scripts.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

WIN_PWSH = "/mnt/c/Program Files/PowerShell/7/pwsh.exe"
MOVE_DIR = Path("/mnt/b/_AI_WORK/move")
LLM_DIR = Path("/mnt/b/_AI_WORK/llm")
SCRIPTS_ROOT_WIN = r"B:\_AI_WORK\scripts"
PYTHON = sys.executable or "python3"


def run_pwsh_file(file_win: str, args: list[str]) -> str:
    cp = subprocess.run(
        [WIN_PWSH, "-NoProfile", "-File", file_win, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = unicodedata.normalize("NFKC", cp.stdout)
    if cp.returncode != 0:
        raise RuntimeError(out.strip() or f"pwsh failed rc={cp.returncode}")
    return out


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


def latest(glob_pat: str) -> Path:
    files = sorted(MOVE_DIR.glob(glob_pat), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No files match: {MOVE_DIR}/{glob_pat}")
    return files[0]


def wsl_to_win_b(path: Path) -> str:
    s = str(path)
    if s.startswith("/mnt/b/"):
        return "B:\\" + s.split("/mnt/b/", 1)[1].replace("/", "\\")
    raise ValueError(f"not on /mnt/b: {path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/mnt/b/_AI_WORK/db/mediaops.sqlite")
    ap.add_argument("--source-root", default=r"B:\未視聴")
    ap.add_argument("--dest-root", default=r"B:\VideoLibrary\by_program")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--allow-needs-review", action="store_true")
    ap.add_argument("--keep-batches", type=int, default=5)
    args = ap.parse_args()

    MOVE_DIR.mkdir(parents=True, exist_ok=True)
    LLM_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    here = Path(__file__).resolve().parent

    tsfix_args = ["-Root", args.source_root]
    if not args.apply:
        tsfix_args.append("-DryRun")
    run_pwsh_file(SCRIPTS_ROOT_WIN + r"\fix_prefix_timestamp_names.ps1", tsfix_args)

    norm_args = ["-Root", args.source_root]
    if not args.apply:
        norm_args.append("-DryRun")
    run_pwsh_file(SCRIPTS_ROOT_WIN + r"\normalize_unwatched_names.ps1", norm_args)

    run_pwsh_file(
        SCRIPTS_ROOT_WIN + r"\unwatched_inventory.ps1",
        ["-Root", args.source_root, "-IncludeHash"],
    )
    inv = latest("inventory_unwatched_*.jsonl")

    run_py([PYTHON, str(here / "ingest_inventory_jsonl.py"), "--jsonl", str(inv), "--target-root", args.source_root])

    queue = LLM_DIR / f"queue_unwatched_batch_{ts}.jsonl"
    run_py(
        [
            PYTHON,
            str(here / "make_metadata_queue_from_inventory.py"),
            "--db",
            args.db,
            "--inventory",
            str(inv),
            "--source-root",
            args.source_root,
            "--out",
            str(queue),
            "--limit",
            str(args.limit),
        ]
    )

    run_py(
        [
            PYTHON,
            str(here / "run_metadata_batches_promptv1.py"),
            "--queue",
            str(queue),
            "--outdir",
            str(LLM_DIR),
            "--batch-size",
            "50",
            "--start-batch",
            "1",
        ]
    )

    plan_cmd = [
        PYTHON,
        str(here / "make_move_plan_from_inventory.py"),
        "--db",
        args.db,
        "--inventory",
        str(inv),
        "--source-root",
        args.source_root,
        "--dest-root",
        args.dest_root,
        "--limit",
        str(args.limit),
    ]
    if args.allow_needs_review:
        plan_cmd.insert(-2, "--allow-needs-review")
    plan_out_raw = run_py(plan_cmd)
    try:
        plan_out = json.loads(plan_out_raw)
    except Exception:
        plan_out = {"raw": plan_out_raw}
    plan = latest("move_plan_from_inventory_*.jsonl")

    apply_args = ["-PlanJsonl", wsl_to_win_b(plan)]
    if not args.apply:
        apply_args.append("-DryRun")
    run_pwsh_file(SCRIPTS_ROOT_WIN + r"\apply_move_plan.ps1", apply_args)
    applied = latest("move_apply_*.jsonl")

    run_py([PYTHON, str(here / "update_db_paths_from_move_apply.py"), "--db", args.db, "--applied", str(applied)])

    remaining_txt = MOVE_DIR / f"remaining_unwatched_{ts}.txt"
    out = run_pwsh_file(
        SCRIPTS_ROOT_WIN + r"\list_remaining_unwatched.ps1",
        ["-Root", args.source_root],
    )
    remaining_txt.write_text(out, encoding="utf-8")

    run_py(
        [
            PYTHON,
            str(here / "rotate_move_audit_logs.py"),
            "--move-dir",
            str(MOVE_DIR),
            "--keep-batches",
            str(args.keep_batches),
        ]
    )

    lines = [ln for ln in remaining_txt.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    print(
        json.dumps(
            {
                "inventory": str(inv),
                "queue": str(queue),
                "plan": str(plan),
                "plan_stats": plan_out,
                "applied": str(applied),
                "apply": bool(args.apply),
                "remaining_files": len(lines),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
