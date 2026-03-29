"""Generate drop-review YAML from dedup plan JSONL.

Reads a dedup_plan_*.jsonl, extracts keep/drop groups,
and writes a YAML file for human review of drop decisions.
"""

from __future__ import annotations

import argparse
import os
from collections import OrderedDict
from pathlib import Path

import yaml

from mediaops_schema import connect_db, fetchall
from pathscan_common import iter_jsonl, now_iso, safe_json, ts_compact


def _load_keep_drop_groups(plan_jsonl: str) -> dict[str, list[dict]]:
    """Extract keep/drop rows grouped by group_key (excluding manual_review_required)."""
    groups: dict[str, list[dict]] = OrderedDict()
    for row in iter_jsonl(plan_jsonl):
        if row.get("_meta"):
            continue
        decision = row.get("decision", "")
        if decision in ("keep", "drop"):
            gk = row.get("group_key", "")
            groups.setdefault(gk, []).append(row)
    return groups


def generate(plan_jsonl: str, db_path: str, output: str) -> dict:
    groups = _load_keep_drop_groups(plan_jsonl)
    if not groups:
        return {"ok": True, "groupCount": 0, "outputPath": None, "message": "No keep/drop groups found"}

    # get size_bytes and broadcaster from DB
    all_path_ids = []
    for rows in groups.values():
        for row in rows:
            all_path_ids.append(row["path_id"])

    con = connect_db(db_path)
    placeholders = ",".join("?" * len(all_path_ids))
    db_rows = fetchall(
        con,
        f"""
        SELECT pm.path_id, pm.broadcaster, f.size_bytes
        FROM path_metadata pm
        JOIN paths p ON p.path_id = pm.path_id
        LEFT JOIN file_paths fp ON fp.path_id = pm.path_id AND fp.is_current = 1
        LEFT JOIN files f ON f.file_id = fp.file_id
        WHERE pm.path_id IN ({placeholders})
        """,
        all_path_ids,
    )
    db_map = {r["path_id"]: r for r in db_rows}
    con.close()

    yaml_groups = []
    for gk, rows in groups.items():
        candidates = []
        for row in rows:
            pid = row["path_id"]
            db_row = db_map.get(pid, {})
            candidates.append({
                "path_id": pid,
                "path": row.get("path", ""),
                "broadcaster": db_row.get("broadcaster") or None,
                "size_bytes": db_row.get("size_bytes") or 0,
                "decision": row["decision"],
            })
        yaml_groups.append({
            "group_key": gk,
            "candidates": candidates,
        })

    doc = {
        "version": 1,
        "kind": "dedup_drop_review",
        "generated_at": now_iso(),
        "source_plan_jsonl": plan_jsonl,
        "groups": yaml_groups,
    }

    output_path = output or str(
        Path(plan_jsonl).parent / f"dedup_drop_review_{ts_compact()}.yaml"
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    return {"ok": True, "groupCount": len(yaml_groups), "outputPath": output_path}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan-jsonl", required=True, help="Path to dedup_plan_*.jsonl")
    ap.add_argument("--db", required=True)
    ap.add_argument("--output", default="", help="Output YAML path (optional, auto-generated if empty)")
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
