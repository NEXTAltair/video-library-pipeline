#!/usr/bin/env python3
"""Update program_title for specified records with proper npk sync.

Accepts a JSON array of update instructions via --updates or stdin:
  [
    {"path_pattern": "%ドキュメント72時間 としまえん%", "new_title": "ドキュメント72時間"},
    {"path_id": "fe5cbee5-...", "new_title": "岸辺露伴ルーヴルへ行く"}
  ]

Each entry must have "new_title" and one of:
  - "path_pattern": SQL LIKE pattern matched against paths.path
  - "path_id": exact path_id

Usage:
  python update_program_titles.py --db mediaops.sqlite --updates '[...]' --dry-run
  echo '[...]' | python update_program_titles.py --db mediaops.sqlite
  python update_program_titles.py --db mediaops.sqlite --updates '[...]'
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import PureWindowsPath

from epg_common import normalize_program_key, program_id_for
from mediaops_schema import begin_immediate, connect_db
from path_placement_rules import safe_dir_name
from pathscan_common import now_iso


def _infer_library_root_from_layout(path: str) -> str | None:
    """<root>\\<prog>\\<YYYY>\\<MM>\\<file> 構造を検出してルートを返す。"""
    p = PureWindowsPath(str(path or "").replace("/", "\\"))
    month_dir = p.parent           # <MM>
    year_dir = month_dir.parent    # <YYYY>
    program_dir = year_dir.parent  # <prog>
    year, month = year_dir.name, month_dir.name
    if not (len(year) == 4 and year.isdigit() and len(month) == 2 and month.isdigit()):
        return None
    if not (1 <= int(month) <= 12):
        return None
    root_s = str(program_dir.parent).rstrip("\\")
    return root_s or None


def _infer_root_by_old_title(path: str, old_title: str) -> str | None:
    """old_title (および safe_dir_name 変換後) でセグメントマッチしてルートを返す。"""
    title = str(old_title or "").strip()
    if not title:
        return None
    p = PureWindowsPath(str(path or "").replace("/", "\\"))
    parts = p.parts
    candidates = {title, safe_dir_name(title)}
    for i in range(1, len(parts)):  # アンカー (drive) はスキップ
        if parts[i] in candidates:
            return str(PureWindowsPath(*parts[:i]))
    return None


def infer_affected_root(path: str, old_title: str) -> str | None:
    """優先順位: (1) layout検出 → (2) old_titleマッチ → (3) ドライブルート"""
    p = PureWindowsPath(str(path or "").replace("/", "\\"))
    return (
        _infer_library_root_from_layout(path)
        or _infer_root_by_old_title(path, old_title)
        or ((p.drive + "\\") if p.drive else None)
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--updates", default="",
                    help="JSON array of updates (or read from stdin)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    raw = args.updates.strip() if args.updates.strip() else sys.stdin.read().strip()
    if not raw:
        print(json.dumps({"ok": False, "error": "no updates provided"}))
        return 1

    try:
        instructions = json.loads(raw)
    except json.JSONDecodeError as e:
        print(json.dumps({"ok": False, "error": f"invalid JSON: {e}"}))
        return 1

    if not isinstance(instructions, list):
        print(json.dumps({"ok": False, "error": "updates must be a JSON array"}))
        return 1

    con = connect_db(args.db)

    updates: list[tuple[str, str]] = []  # (new_title, path_id)
    errors: list[dict] = []

    for i, instr in enumerate(instructions):
        if not isinstance(instr, dict):
            errors.append({"index": i, "error": "not an object"})
            continue

        new_title = str(instr.get("new_title") or "").strip()
        if not new_title:
            errors.append({"index": i, "error": "missing new_title"})
            continue

        path_id = str(instr.get("path_id") or "").strip()
        path_pattern = str(instr.get("path_pattern") or "").strip()

        if path_id:
            row = con.execute(
                "SELECT path_id FROM path_metadata WHERE path_id = ?",
                (path_id,),
            ).fetchone()
            if not row:
                errors.append({"index": i, "error": f"path_id not found: {path_id[:16]}..."})
                continue
            updates.append((new_title, path_id))

        elif path_pattern:
            rows = con.execute(
                """SELECT pm.path_id
                   FROM path_metadata pm
                   JOIN paths p ON p.path_id = pm.path_id
                   WHERE p.path LIKE ?""",
                (path_pattern,),
            ).fetchall()
            if not rows:
                errors.append({"index": i, "error": f"no matches for pattern: {path_pattern}"})
                continue
            for r in rows:
                updates.append((new_title, r["path_id"]))

        else:
            errors.append({"index": i, "error": "need path_id or path_pattern"})

    # Capture old_title BEFORE any DB mutation for affectedRoots inference (Issue #86).
    # Querying pm.program_title after the UPDATE would yield the new title,
    # breaking old_title segment matching in infer_affected_root.
    old_title_map: dict[str, str] = {}
    if updates:
        pids_for_old = sorted({pid for _, pid in updates})
        ph_old = ",".join("?" for _ in pids_for_old)
        for row in con.execute(
            f"SELECT path_id, program_title FROM path_metadata WHERE path_id IN ({ph_old})",
            pids_for_old,
        ).fetchall():
            old_title_map[str(row["path_id"])] = str(row["program_title"] or "")

    result = {
        "ok": True,
        "matched": len(updates),
        "errors": errors,
        "dry_run": args.dry_run,
    }

    # Composable output: expose affected path ids / roots for downstream chaining
    updated_path_ids = sorted({pid for _, pid in updates})
    result["updatedPathIds"] = updated_path_ids

    affected_roots: list[str] = []
    if updated_path_ids:
        placeholders = ",".join("?" for _ in updated_path_ids)
        rows = con.execute(
            f"SELECT path_id, path FROM paths WHERE path_id IN ({placeholders})",
            updated_path_ids,
        ).fetchall()
        roots: set[str] = set()
        for row in rows:
            pid = str(row["path_id"])
            old_title = old_title_map.get(pid, "")
            root = infer_affected_root(str(row["path"] or ""), old_title)
            if root:
                roots.add(root)
        affected_roots = sorted(roots)
    result["affectedRoots"] = affected_roots

    if args.dry_run:
        # Show what would change
        preview: list[dict] = []
        seen: set[str] = set()
        for new_title, pid in updates:
            if pid in seen:
                continue
            seen.add(pid)
            row = con.execute(
                "SELECT path FROM paths WHERE path_id = ?",
                (pid,),
            ).fetchone()
            if row:
                preview.append({
                    "path_id": pid[:16],
                    "old_title": old_title_map.get(pid, ""),
                    "new_title": new_title,
                    "path": row["path"][-80:] if row["path"] else "",
                })
        result["preview"] = preview[:50]
        if len(preview) > 50:
            result["preview_truncated"] = len(preview)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    begin_immediate(con)
    con.executemany(
        """UPDATE path_metadata
           SET program_title = ?,
               human_reviewed = 1,
               needs_review = 0,
               source = 'human_reviewed',
               updated_at = datetime('now')
           WHERE path_id = ?""",
        updates,
    )

    # Feedback: ensure corrected titles exist in programs table
    # so that future dictionary-match extraction picks them up.
    registered_titles: list[str] = []
    seen_titles: set[str] = set()
    for new_title, _ in updates:
        if new_title in seen_titles:
            continue
        seen_titles.add(new_title)
        pkey = normalize_program_key(new_title)
        pid = program_id_for(pkey)
        try:
            con.execute(
                """INSERT INTO programs (program_id, program_key, canonical_title, created_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(program_id) DO UPDATE SET
                     canonical_title = excluded.canonical_title""",
                (pid, pkey, new_title, now_iso()),
            )
            registered_titles.append(new_title)
        except sqlite3.OperationalError:
            pass  # programs table may not exist yet

    con.commit()
    con.close()

    result["updated"] = len(updates)
    result["programs_registered"] = registered_titles
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
