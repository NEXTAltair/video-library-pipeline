#!/usr/bin/env python3
r"""Rotate move/llm audit logs to keep the workspace readable."""

from __future__ import annotations

import argparse
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path


def _gzip_file(src: Path) -> Path:
    dst = src.with_suffix(src.suffix + ".gz")
    if dst.exists():
        return dst
    with src.open("rb") as f_in, gzip.open(dst, "wb", compresslevel=9) as f_out:
        shutil.copyfileobj(f_in, f_out)
    src.unlink(missing_ok=True)
    return dst


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--move-dir", required=True)
    ap.add_argument("--llm-dir", required=True)
    ap.add_argument("--keep-batches", type=int, default=5)
    ap.add_argument("--keep-listings", type=int, default=3)
    ap.add_argument("--ttl-days", type=int, default=30)
    args = ap.parse_args()

    move_dir = Path(args.move_dir)
    arc = move_dir / "archive"
    arc.mkdir(parents=True, exist_ok=True)
    llm_dir = Path(args.llm_dir)
    llm_arc = llm_dir / "archive"
    llm_arc.mkdir(parents=True, exist_ok=True)

    remaining = sorted(move_dir.glob("remaining_unwatched_*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in remaining[1:]:
        shutil.move(str(p), str(arc / p.name))

    move_logs_legacy = sorted(move_dir.glob("move_to_videolibrary_by_program_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    move_apply_logs = sorted(move_dir.glob("move_apply_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    move_plan_logs = sorted(move_dir.glob("move_plan_from_queue_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    inv_logs = sorted(move_dir.glob("inventory_unwatched_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    plan_inv_logs = sorted(move_dir.glob("move_plan_from_inventory_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)

    keep_moves = set(p.name for p in move_logs_legacy[: max(args.keep_batches, 0)])
    keep_moves |= set(p.name for p in move_apply_logs[: max(args.keep_batches, 0)])
    keep_moves |= set(p.name for p in move_plan_logs[: max(args.keep_batches, 0)])
    if inv_logs:
        keep_moves.add(inv_logs[0].name)
    if plan_inv_logs:
        keep_moves.add(plan_inv_logs[0].name)

    keep_listings = set()
    for pat in ["unwatched_pwsh_errors_*.jsonl", "unwatched_ls_*.jsonl", "unwatched_gci_*.jsonl"]:
        files = sorted(move_dir.glob(pat), key=lambda p: p.stat().st_mtime, reverse=True)
        for p in files[: max(args.keep_listings, 0)]:
            keep_listings.add(p.name)

    for p in move_dir.glob("*.jsonl"):
        if p.name in keep_moves or p.name in keep_listings:
            continue
        dest = arc / p.name
        if not dest.exists():
            shutil.move(str(p), str(dest))
        if dest.suffix == ".jsonl":
            _gzip_file(dest)

    # LLM artifact retention:
    # keep the latest N files for each active prefix, archive+gzip older files.
    llm_prefixes = [
        "queue_unwatched_batch_",
        "llm_filename_extract_input_",
        "llm_filename_extract_output_",
    ]
    keep_llm = set()
    for prefix in llm_prefixes:
        files = sorted(llm_dir.glob(f"{prefix}*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
        for p in files[: max(args.keep_batches, 0)]:
            keep_llm.add(p.name)
    for p in llm_dir.glob("*.jsonl"):
        if p.name in keep_llm:
            continue
        dest = llm_arc / p.name
        if not dest.exists():
            shutil.move(str(p), str(dest))
        if dest.suffix == ".jsonl":
            _gzip_file(dest)

    # TTL cleanup on archived artifacts.
    ttl_cutoff = datetime.now().astimezone() - timedelta(days=max(args.ttl_days, 0))
    for arc_dir in [arc, llm_arc]:
        for p in arc_dir.glob("*.gz"):
            if datetime.fromtimestamp(p.stat().st_mtime).astimezone() < ttl_cutoff:
                p.unlink(missing_ok=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
