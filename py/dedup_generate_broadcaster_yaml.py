"""Generate broadcaster-assign review YAML from dedup plan JSONL.

Reads a dedup_plan_*.jsonl, extracts unknown_bucket_mixed items,
and writes a YAML file for human review.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import yaml

from mediaops_schema import connect_db, fetchall
from pathscan_common import iter_jsonl, now_iso, safe_json, ts_compact


def _load_plan_manual_review_items(plan_jsonl: str) -> list[dict]:
    """Extract manual_review_required rows with reason=unknown_bucket_mixed."""
    items: list[dict] = []
    for row in iter_jsonl(plan_jsonl):
        if row.get("_meta"):
            continue
        if row.get("decision") == "manual_review_required" and row.get("reason") == "unknown_bucket_mixed":
            items.append(row)
    return items


def generate(plan_jsonl: str, db_path: str, output: str) -> dict:
    items = _load_plan_manual_review_items(plan_jsonl)
    if not items:
        return {"ok": True, "itemCount": 0, "outputPath": None, "message": "No unknown_bucket_mixed items found"}

    con = connect_db(db_path)
    path_ids = [row["path_id"] for row in items]
    placeholders = ",".join("?" * len(path_ids))
    db_rows = fetchall(
        con,
        f"""
        SELECT pm.path_id, p.path, pm.broadcaster
        FROM path_metadata pm
        JOIN paths p ON p.path_id = pm.path_id
        WHERE pm.path_id IN ({placeholders})
        """,
        path_ids,
    )
    db_map = {r["path_id"]: r for r in db_rows}

    yaml_items = []
    for row in items:
        pid = row["path_id"]
        db_row = db_map.get(pid, {})
        yaml_items.append({
            "path_id": pid,
            "path": db_row.get("path") or row.get("path", ""),
            "broadcaster": db_row.get("broadcaster") or None,
            "bucket": None,
        })

    doc = {
        "version": 1,
        "kind": "dedup_broadcaster_assign",
        "generated_at": now_iso(),
        "source_plan_jsonl": plan_jsonl,
        "items": yaml_items,
    }

    output_path = output or str(
        Path(plan_jsonl).parent / f"dedup_broadcaster_review_{ts_compact()}.yaml"
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    con.close()
    return {"ok": True, "itemCount": len(yaml_items), "outputPath": output_path}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan-jsonl", required=True, help="Path to dedup_plan_*.jsonl")
    ap.add_argument("--db", required=True)
    ap.add_argument("--output", default="", help="Output YAML path (auto-generated if empty)")
    args = ap.parse_args()

    if not os.path.exists(args.plan_jsonl):
        print(safe_json({"ok": False, "error": f"Plan JSONL not found: {args.plan_jsonl}"}))
        return 1
    if not os.path.exists(args.db):
        print(safe_json({"ok": False, "error": f"DB not found: {args.db}"}))
        return 1

    result = generate(args.plan_jsonl, args.db, args.output)
    print(safe_json(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
