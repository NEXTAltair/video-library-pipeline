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

from epg_common import normalize_program_key, program_id_for
from mediaops_schema import begin_immediate, connect_db
from pathscan_common import now_iso


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
        # Collect old program_titles (current folder names) for path-based root detection
        placeholders = ",".join("?" for _ in updated_path_ids)
        title_rows = con.execute(
            f"SELECT DISTINCT program_title FROM path_metadata WHERE path_id IN ({placeholders})",
            updated_path_ids,
        ).fetchall()
        old_titles = {str(r["program_title"]).strip() for r in title_rows if r["program_title"]}

        path_rows = con.execute(
            f"SELECT DISTINCT path FROM paths WHERE path_id IN ({placeholders})",
            updated_path_ids,
        ).fetchall()
        roots: set[str] = set()
        for row in path_rows:
            path = str(row["path"] or "")
            # Find dest_root: parent directory of the program_title folder segment
            # e.g. B:\VideoLibrary\番組名\2026\03\file.ts → B:\VideoLibrary
            parts = path.replace("/", "\\").split("\\")
            for i, seg in enumerate(parts):
                if seg in old_titles and i > 0:
                    roots.add("\\".join(parts[:i]))
                    break
            else:
                # Fallback: drive letter root
                if len(path) >= 3 and path[1:3] == ":\\":
                    roots.add(path[:3].upper())
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
                """SELECT pm.program_title, p.path
                   FROM path_metadata pm
                   JOIN paths p ON p.path_id = pm.path_id
                   WHERE pm.path_id = ?""",
                (pid,),
            ).fetchone()
            if row:
                preview.append({
                    "path_id": pid[:16],
                    "old_title": row["program_title"],
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
