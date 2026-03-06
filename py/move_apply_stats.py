"""Shared aggregation for apply_move_plan.ps1 JSONL output.

Reads the JSONL produced by apply_move_plan.ps1 and returns a structured
breakdown of successes, failures, and error types.  Used by
unwatched_pipeline_runner, relocate_existing_files, and dedup_recordings.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MAX_FAILED_ITEMS = 20


def aggregate_move_apply(jsonl_path: str | Path) -> dict[str, Any]:
    """Aggregate apply_move_plan.ps1 output JSONL into a stats dict.

    Returns
    -------
    dict with keys:
        totalOps, succeeded, failed, errorBreakdown,
        failedItems (capped at MAX_FAILED_ITEMS), failedItemsTruncated
    """
    p = Path(jsonl_path)
    if not p.exists():
        return {
            "totalOps": 0,
            "succeeded": 0,
            "failed": 0,
            "errorBreakdown": {},
            "failedItems": [],
            "failedItemsTruncated": False,
        }

    total = 0
    succeeded = 0
    failed = 0
    error_breakdown: dict[str, int] = {}
    failed_items: list[dict[str, Any]] = []

    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(rec, dict):
            continue
        # Skip _meta header rows
        if "_meta" in rec:
            continue
        if rec.get("op") != "move":
            continue

        total += 1
        if rec.get("ok"):
            succeeded += 1
        else:
            failed += 1
            err = str(rec.get("error") or "unknown")
            # Normalize compound errors like "mkdir_failed: ..." to base key
            err_key = err.split(":")[0].strip() if ":" in err else err
            error_breakdown[err_key] = error_breakdown.get(err_key, 0) + 1
            if len(failed_items) < MAX_FAILED_ITEMS:
                failed_items.append({
                    "path_id": rec.get("path_id"),
                    "src": rec.get("src"),
                    "dst": rec.get("dst"),
                    "error": err,
                })

    return {
        "totalOps": total,
        "succeeded": succeeded,
        "failed": failed,
        "errorBreakdown": error_breakdown,
        "failedItems": failed_items,
        "failedItemsTruncated": failed > MAX_FAILED_ITEMS,
    }
