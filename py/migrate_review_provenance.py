#!/usr/bin/env python3
"""Evidence-scoped correction of legacy agent reviews mislabeled as human.

No default database, no schema migration, no inference from model/filename or
reviewed_by. Dry-run is default. Supply exact path_ids AND a JSON evidence file:
{"rows": {"path-id": {"evidence": "original agent transcript/artifact reference",
                      "expected_sha256": "digest from --inspect"}}}
--inspect only reports the explicitly requested rows and their snapshot hashes;
it does not attest that those rows were reviewed by an agent.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3

from backup_mediaops_db import backup_db
from pathscan_common import now_iso


def snapshot_digest(row):
    return hashlib.sha256(json.dumps(dict(row), sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def migrate(db, path_ids, evidence=None, *, apply=False, inspect=False):
    if not path_ids or len(set(path_ids)) != len(path_ids) or any(not p.strip() for p in path_ids):
        raise ValueError("explicit unique path_ids are required")
    if apply and inspect:
        raise ValueError("inspect cannot apply")
    db = Path(db).resolve(strict=True)
    uri = db.as_uri() + ("?mode=rw" if apply else "?mode=ro")
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    try:
        con.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        rows = []
        for pid in path_ids:
            row = con.execute("SELECT * FROM path_metadata WHERE path_id=?", (pid,)).fetchone()
            if row is None:
                raise ValueError(f"metadata not found: {pid}")
            rows.append(row)
        if inspect:
            return {"ok": True, "applied": False, "rows": [
                {"path_id": row["path_id"], "source": row["source"],
                 "human_reviewed": row["human_reviewed"], "expected_sha256": snapshot_digest(row)}
                for row in rows]}
        manifest = evidence.get("rows") if isinstance(evidence, dict) else None
        if not isinstance(manifest, dict) or set(manifest) != set(path_ids):
            raise ValueError("evidence rows must exactly match explicit path_ids")
        updates = []
        for row in rows:
            pid = row["path_id"]
            proof = manifest[pid]
            digest = snapshot_digest(row)
            if not isinstance(proof, dict) or not isinstance(proof.get("evidence"), str) or not proof["evidence"].strip():
                raise ValueError(f"original agent-review evidence is required: {pid}")
            if proof.get("expected_sha256") != digest:
                raise ValueError(f"metadata changed or evidence snapshot mismatch: {pid}")
            if row["source"] != "human_reviewed" or row["human_reviewed"] != 1:
                raise ValueError(f"not a legacy human-labeled row: {pid}")
            data = json.loads(row["data_json"] or "{}")
            if not isinstance(data, dict):
                raise ValueError(f"invalid metadata JSON: {pid}")
            if data.get("review_acceptance") == "human_confirmed" or data.get("human_confirmation"):
                raise ValueError(f"explicit human confirmation must not be downgraded: {pid}")
            old = {key: data.pop(key) for key in ("human_reviewed", "human_reviewed_at", "human_reviewed_by") if key in data}
            history = data.setdefault("source_history", [])
            if not isinstance(history, list):
                raise ValueError(f"invalid source history: {pid}")
            history.append({"source": "provenance_migration", "at": now_iso(),
                            "fields": ["source", "human_reviewed", "review_acceptance"],
                            "previous_source": row["source"], "previous_human_reviewed": row["human_reviewed"],
                            "previous_snapshot_sha256": digest, "previous_provenance": old,
                            "evidence": proof["evidence"]})
            data["review_acceptance"] = "agent_accepted"
            data["provenance_migration_evidence"] = proof["evidence"]
            updates.append((json.dumps(data, ensure_ascii=False), now_iso(), pid))
        result = {"ok": True, "applied": apply, "path_ids": path_ids, "rows": len(updates)}
        if apply:
            ok, backup_path, error = backup_db(str(db), "pre_migrate_review_provenance")
            if not ok:
                raise ValueError(f"metadata backup failed: {error}")
            con.executemany("UPDATE path_metadata SET source='agent_accepted',human_reviewed=0,data_json=?,updated_at=? WHERE path_id=?", updates)
            con.commit()
            result["backupPath"] = backup_path
        return result
    finally:
        con.rollback()
        con.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--path-id", action="append", required=True, dest="path_ids")
    ap.add_argument("--evidence-json", help="Path to evidence manifest (not inline JSON)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--inspect", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="Default; opens DB read-only")
    args = ap.parse_args()
    try:
        evidence = json.loads(Path(args.evidence_json).read_text()) if args.evidence_json else None
        result = migrate(args.db, args.path_ids, evidence, apply=args.apply, inspect=args.inspect)
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
