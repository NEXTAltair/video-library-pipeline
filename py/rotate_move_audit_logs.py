#!/usr/bin/env python3
"""Legacy audit archival: preview by default, verified gzip, no TTL deletion."""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from video_pipeline.platform.artifact_retention import safe_path, digest, sync_dir


def _gzip_digest(path):
    value = hashlib.sha256()
    with gzip.open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _gzip_file(src: Path, dst: Path | None = None) -> Path:
    """Lossless replacement; a differing/partial old archive cannot erase source."""
    src = safe_path(src)
    dst = safe_path(dst or src.with_suffix(src.suffix + ".gz"))
    expected = digest(src)
    before = src.stat()
    if not dst.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=dst.parent, suffix=".gz", delete=False) as stream:
            temporary = Path(stream.name)
        try:
            with src.open("rb") as f_in, gzip.open(temporary, "wb", compresslevel=9) as f_out:
                shutil.copyfileobj(f_in, f_out)
            if _gzip_digest(temporary) != expected:
                raise ValueError("archive verification failed")
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            # Atomic, exclusive publication, never replace an existing archive.
            os.link(temporary, dst)
            sync_dir(dst.parent)
        finally:
            temporary.unlink(missing_ok=True)
    if _gzip_digest(dst) != expected:
        raise ValueError("existing archive differs; source retained")
    after = src.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns) or digest(src) != expected:
        raise ValueError("source changed during archival; source retained")
    src.unlink()
    sync_dir(src.parent)
    return dst


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--move-dir", required=True)
    ap.add_argument("--llm-dir", required=True)
    ap.add_argument("--keep-batches", type=int, default=5)
    ap.add_argument("--keep-listings", type=int, default=3)
    ap.add_argument("--ttl-days", type=int, default=30, help="Deprecated; archives are never deleted by age")
    ap.add_argument("--apply", action="store_true", help="Archive known legacy logs; no permanent deletion")
    args = ap.parse_args(argv)

    move_dir = safe_path(args.move_dir)
    arc = move_dir / "archive"
    llm_dir = safe_path(args.llm_dir)
    llm_arc = llm_dir / "archive"
    if not move_dir.is_dir() or not llm_dir.is_dir():
        raise ValueError("audit directories must already exist")

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

    candidates = []
    known_move_files = set(move_logs_legacy + move_apply_logs + move_plan_logs + inv_logs + plan_inv_logs)
    for pattern in ("unwatched_pwsh_errors_*.jsonl", "unwatched_ls_*.jsonl", "unwatched_gci_*.jsonl"):
        known_move_files.update(move_dir.glob(pattern))
    for p in sorted(known_move_files):
        if p.name in keep_moves or p.name in keep_listings:
            continue
        candidates.append((p, arc / (p.name + ".gz")))

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
    known_llm_files = set()
    for prefix in llm_prefixes:
        known_llm_files.update(llm_dir.glob(prefix + "*.jsonl"))
    for p in sorted(known_llm_files):
        if p.name in keep_llm:
            continue
        candidates.append((p, llm_arc / (p.name + ".gz")))

    archived = []
    if args.apply:
        # Serialize writers. All collisions are checked before transforming a log.
        with safe_path(move_dir / ".audit-archive.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            for source, target in candidates:
                safe_path(source); safe_path(target)
                if target.exists() and _gzip_digest(target) != digest(source):
                    raise ValueError("archive collision; sources retained")
            for source, target in candidates:
                archived.append(str(_gzip_file(source, target)))
    print(json.dumps({"ok": True, "preview": not args.apply, "candidates": [str(source) for source, _ in candidates],
                      "archived": archived, "ttlDeletionDisabled": True, "permanentlyDeleted": 0}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
