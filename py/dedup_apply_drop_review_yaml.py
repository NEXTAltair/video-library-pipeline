"""Apply human-edited drop-review YAML to quarantine drop candidates.

Reads the operator-edited YAML, collects drop items, and moves them
to quarantine using PowerShell apply_move_plan.ps1.
"""

from __future__ import annotations

import argparse
import os
import uuid
from pathlib import Path, PureWindowsPath
from typing import Any

import yaml

from dedup_common import safe_group_key
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from pathscan_common import (
    canonicalize_windows_path,
    iter_jsonl,
    now_iso,
    safe_json,
    split_win,
    ts_compact,
    wsl_to_windows_path,
    windows_to_wsl_path,
)
from windows_pwsh_bridge import run_pwsh_json


def apply_yaml(yaml_path: str, db_path: str, windows_ops_root: str) -> dict:
    with open(yaml_path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    if doc.get("kind") != "dedup_drop_review":
        return {"ok": False, "error": f"Unexpected YAML kind: {doc.get('kind')}"}

    groups = doc.get("groups") or []
    if not groups:
        return {"ok": True, "groupsProcessed": 0, "filesMoved": 0, "filesSkipped": 0, "errors": []}

    # collect drop items, respecting skip
    rows_drop: list[dict[str, Any]] = []
    groups_processed = 0
    groups_skipped = 0
    for group in groups:
        gk = group.get("group_key", "")
        candidates = group.get("candidates") or []
        decisions = [c.get("decision", "") for c in candidates]

        # if any candidate has decision=skip, skip entire group
        if "skip" in decisions:
            groups_skipped += 1
            continue

        groups_processed += 1
        for c in candidates:
            if c.get("decision") == "drop":
                rows_drop.append({
                    "path_id": c["path_id"],
                    "path": c["path"],
                    "group_key": gk,
                })

    if not rows_drop:
        return {
            "ok": True,
            "groupsProcessed": groups_processed,
            "groupsSkipped": groups_skipped,
            "filesMoved": 0,
            "filesSkipped": 0,
            "errors": [],
        }

    # setup paths
    ops_root = Path(windows_ops_root).resolve()
    move_dir = ops_root / "move"
    quarantine_root = ops_root / "duplicates" / "quarantine"
    move_dir.mkdir(parents=True, exist_ok=True)
    quarantine_root.mkdir(parents=True, exist_ok=True)
    scripts_root = ops_root / "scripts"
    apply_move_script = scripts_root / "apply_move_plan.ps1"

    errors: list[str] = []
    if not apply_move_script.exists():
        return {"ok": False, "error": f"apply_move_plan.ps1 not found: {apply_move_script}"}

    ops_root_win = wsl_to_windows_path(str(ops_root))
    quarantine_root_win = wsl_to_windows_path(str(quarantine_root))
    ts = ts_compact()

    # write internal move plan
    internal_move_plan = move_dir / f"dedup_drop_review_move_plan_{ts}.jsonl"
    drop_by_path_id = {str(r["path_id"]): r for r in rows_drop}
    with internal_move_plan.open("w", encoding="utf-8") as w:
        w.write(safe_json({"_meta": {"kind": "dedup_drop_review_move_plan", "generated_at": now_iso(), "rows": len(rows_drop)}}) + "\n")
        for row in rows_drop:
            src_win = canonicalize_windows_path(str(row["path"]))
            group_dir_win = canonicalize_windows_path(
                quarantine_root_win + "\\" + safe_group_key(str(row["group_key"]))
            )
            base_name = PureWindowsPath(src_win).name
            dst_win = canonicalize_windows_path(group_dir_win + "\\" + base_name)
            w.write(safe_json({"op": "move", "path_id": row["path_id"], "src": src_win, "dst": dst_win}) + "\n")

    # run PowerShell move
    move_apply_file = None
    try:
        apply_meta = run_pwsh_json(
            str(apply_move_script),
            ["-PlanJsonl", wsl_to_windows_path(str(internal_move_plan)), "-OpsRoot", ops_root_win, "-OnDstExists", "rename_suffix"],
        )
        out_jsonl = str(apply_meta.get("out_jsonl") or "").strip()
        if not out_jsonl:
            raise RuntimeError("apply_move_plan.ps1 did not return out_jsonl")
        move_apply_file = Path(windows_to_wsl_path(out_jsonl))
        if not move_apply_file.exists():
            raise RuntimeError(f"move apply JSONL not found: {move_apply_file}")
    except Exception as e:
        errors.append(f"dedup drop review move engine failed: {e}")

    # record in DB
    files_moved = 0
    con = connect_db(db_path)
    create_schema_if_needed(con)

    if move_apply_file is not None:
        run_id = str(uuid.uuid4())
        try:
            begin_immediate(con)
            con.execute(
                """
                INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, "dedup_drop_review", str(quarantine_root), now_iso(), None,
                 "dedup_apply_drop_review_yaml.py", f"groups={groups_processed} drops={len(rows_drop)}"),
            )

            apply_rows: list[dict[str, Any]] = []
            for rec in iter_jsonl(str(move_apply_file)):
                if rec.get("op") != "move":
                    continue
                pid = str(rec.get("path_id") or "")
                src_win = canonicalize_windows_path(str(rec.get("src") or ""))
                dst_win_val = canonicalize_windows_path(str(rec.get("dst") or ""))
                ok = bool(rec.get("ok"))
                err_text = None if ok else str(rec.get("error") or "")
                src_row = drop_by_path_id.get(pid, {})
                group_key = src_row.get("group_key")

                if ok and pid and dst_win_val:
                    drive, dir_, name, ext = split_win(dst_win_val)
                    con.execute(
                        "UPDATE paths SET path=?, drive=?, dir=?, name=?, ext=?, updated_at=? WHERE path_id=?",
                        (dst_win_val, drive, dir_, name, ext, now_iso(), pid),
                    )
                    con.execute(
                        """
                        INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (run_id, now_iso(), "dedup_move", pid, None,
                         safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}), 1, None),
                    )
                    files_moved += 1
                else:
                    if src_win or dst_win_val or pid:
                        errors.append(f"move failed: {src_win or '(empty)'} -> {dst_win_val or '(empty)'} :: {err_text or 'unknown_error'}")
                    if pid:
                        con.execute(
                            """
                            INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (run_id, now_iso(), "dedup_move", pid, None,
                             safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}), 0, err_text or "move_failed"),
                        )

                apply_rows.append({"group_key": group_key, "path_id": pid or None, "src": src_win, "dst": dst_win_val, "ok": ok, "error": err_text, "ts": str(rec.get("ts") or now_iso())})

            con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

        # write apply JSONL
        apply_path = move_dir / f"dedup_drop_review_apply_{ts}.jsonl"
        with apply_path.open("w", encoding="utf-8") as w:
            w.write(safe_json({"_meta": {"kind": "dedup_drop_review_apply", "generated_at": now_iso(), "run_id": run_id, "rows": len(apply_rows)}}) + "\n")
            for row in apply_rows:
                w.write(safe_json(row) + "\n")
    else:
        con.close()

    return {
        "ok": len(errors) == 0,
        "groupsProcessed": groups_processed,
        "groupsSkipped": groups_skipped,
        "filesMoved": files_moved,
        "filesDropped": len(rows_drop),
        "errors": errors,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yaml", required=True, help="Path to edited drop-review YAML")
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    args = ap.parse_args()

    if not os.path.exists(args.yaml):
        print(safe_json({"ok": False, "error": f"YAML not found: {args.yaml}"}))
        return 1
    if not os.path.exists(args.db):
        print(safe_json({"ok": False, "error": f"DB not found: {args.db}"}))
        return 1

    result = apply_yaml(args.yaml, args.db, args.windows_ops_root)
    print(safe_json(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
