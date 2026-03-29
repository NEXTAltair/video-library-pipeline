"""Apply human-edited broadcaster-assign YAML to DB and broadcast_buckets.yaml.

Reads the operator-edited YAML, updates path_metadata.broadcaster,
and optionally adds unknown broadcasters to broadcast_buckets.yaml.
"""

from __future__ import annotations

import argparse
import os
import re
import uuid
from pathlib import Path
from typing import Any

import yaml

from dedup_common import classify_broadcast_bucket, load_bucket_rules
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall
from pathscan_common import now_iso, safe_json


def _add_keyword_to_bucket_yaml(yaml_path: Path, keyword: str, bucket: str) -> None:
    """Append a broadcaster keyword to the correct section of broadcast_buckets.yaml."""
    section_key = "terrestrial_keywords" if bucket == "terrestrial" else "bs_cs_keywords"
    lines = yaml_path.read_text(encoding="utf-8").splitlines(keepends=True)
    insert_idx: int | None = None
    in_section = False
    for i, line in enumerate(lines):
        stripped = line.rstrip()
        if re.match(rf"^\s*{section_key}\s*:\s*$", stripped):
            in_section = True
            continue
        if in_section:
            if re.match(r"^\s*-\s+", stripped):
                insert_idx = i + 1  # keep tracking last item in section
            elif stripped and not stripped.lstrip().startswith("#"):
                # next section or scalar key
                break
    if insert_idx is not None:
        new_line = f"  - \"{keyword}\"\n"
        lines.insert(insert_idx, new_line)
    else:
        # section not found or empty; append to end
        lines.append(f"\n{section_key}:\n  - \"{keyword}\"\n")

    yaml_path.write_text("".join(lines), encoding="utf-8")


def apply_yaml(yaml_path: str, db_path: str, bucket_rules_path: str) -> dict:
    with open(yaml_path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    if doc.get("kind") != "dedup_broadcaster_assign":
        return {"ok": False, "error": f"Unexpected YAML kind: {doc.get('kind')}"}

    items = doc.get("items") or []
    if not items:
        return {"ok": True, "updated": 0, "skipped": 0, "bucketsAdded": 0, "errors": []}

    bucket_rules_file = Path(bucket_rules_path)
    bucket_rules = load_bucket_rules(bucket_rules_file)

    con = connect_db(db_path)
    create_schema_if_needed(con)

    run_id = str(uuid.uuid4())
    errors: list[str] = []
    updated = 0
    skipped = 0
    buckets_added = 0

    with begin_immediate(con):
        # register run
        con.execute(
            """
            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, "broadcaster_assign", "", now_iso(), None, "dedup_apply_broadcaster_yaml.py",
             f"items={len(items)}"),
        )

        for item in items:
            path_id = item.get("path_id")
            new_broadcaster = item.get("broadcaster")
            bucket_hint = item.get("bucket")

            if not path_id:
                errors.append("item missing path_id")
                continue
            if not new_broadcaster:
                skipped += 1
                continue

            # check if broadcaster is known
            fake_row: dict[str, Any] = {"broadcaster": new_broadcaster}
            detected_bucket, _reason = classify_broadcast_bucket(fake_row, bucket_rules)

            if detected_bucket == "unknown":
                if not bucket_hint or bucket_hint not in ("terrestrial", "bs_cs"):
                    errors.append(
                        f"Unknown broadcaster '{new_broadcaster}' for path_id={path_id} "
                        f"requires bucket field (terrestrial/bs_cs)"
                    )
                    continue
                # add to broadcast_buckets.yaml
                _add_keyword_to_bucket_yaml(bucket_rules_file, new_broadcaster, bucket_hint)
                bucket_rules = load_bucket_rules(bucket_rules_file)  # reload
                buckets_added += 1

            # get old value for audit
            old_rows = fetchall(con, "SELECT broadcaster FROM path_metadata WHERE path_id=?", (path_id,))
            old_broadcaster = old_rows[0]["broadcaster"] if old_rows else None

            # update DB
            con.execute(
                "UPDATE path_metadata SET broadcaster=?, updated_at=? WHERE path_id=?",
                (new_broadcaster, now_iso(), path_id),
            )

            # record event
            con.execute(
                """
                INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, now_iso(), "broadcaster_assign", path_id, None,
                 safe_json({"old_broadcaster": old_broadcaster, "new_broadcaster": new_broadcaster,
                            "bucket_hint": bucket_hint}),
                 1, None),
            )
            updated += 1

        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))

    con.close()

    return {
        "ok": len(errors) == 0,
        "runId": run_id,
        "updated": updated,
        "skipped": skipped,
        "bucketsAdded": buckets_added,
        "errors": errors,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yaml", required=True, help="Path to edited broadcaster-assign YAML")
    ap.add_argument("--db", required=True)
    ap.add_argument("--bucket-rules-path", default="", help="Path to broadcast_buckets.yaml")
    args = ap.parse_args()

    if not os.path.exists(args.yaml):
        print(safe_json({"ok": False, "error": f"YAML not found: {args.yaml}"}))
        return 1
    if not os.path.exists(args.db):
        print(safe_json({"ok": False, "error": f"DB not found: {args.db}"}))
        return 1

    bucket_rules_path = (
        args.bucket_rules_path
        if args.bucket_rules_path.strip()
        else str(Path(__file__).resolve().parent.parent / "rules" / "broadcast_buckets.yaml")
    )

    result = apply_yaml(args.yaml, args.db, bucket_rules_path)
    print(safe_json(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
