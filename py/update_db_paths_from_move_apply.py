#!/usr/bin/env python3
"""Commit successful move receipts without rewriting locations or historical rows.

Replaying the same durable log is idempotent. Dry-run receipts never update DB.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import uuid
import sqlite3
from pathlib import Path
from mediaops_schema import connect_db, create_schema_if_needed, begin_immediate
from pathscan_common import iter_jsonl, now_iso
from video_pipeline.db.file_identity import ensure_identity_schema, record_move


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', required=True)
    ap.add_argument('--applied', required=True)
    ap.add_argument('--notes')
    ap.add_argument('--run-kind', default='apply')
    ap.add_argument('--event-kind', default='move')
    ap.add_argument('--detail-source', default='apply_move_plan.ps1')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    if not os.path.isfile(args.db):
        raise SystemExit('DB not found: ' + args.db)
    lines = Path(args.applied).read_text(encoding="utf-8-sig").splitlines()
    records = []
    for i,line in enumerate(lines):
        if not line.strip(): continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            if i != len(lines)-1: raise
            # A crash can truncate the last write. Recover previous full receipts.
            break
    if any(r.get('_meta', {}).get('dry_run') for r in records):
        print(json.dumps({'updated': 0, 'events': 0, 'dry_run_log': True}))
        return 0
    rows = [(i,r) for i,r in enumerate(records) if r.get('op') == 'move' and r.get('ok') is True and not r.get('dry_run')]
    con = connect_db(args.db)
    if args.dry_run:
        clone = connect_db(":memory:")
        con.backup(clone); con.close(); con = clone
    create_schema_if_needed(con)
    ensure_identity_schema(con)
    con.commit()
    run_id = uuid.uuid4().hex
    changed = 0
    try:
        begin_immediate(con)
        con.execute('INSERT INTO runs(run_id,kind,started_at,notes) VALUES(?,?,?,?)', (run_id,args.run_kind,now_iso(),args.notes))
        for index,row in rows:
            # Stable across retries and log copies; no filename-only inference.
            receipt = hashlib.sha256(json.dumps(row,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
            changed += int(record_move(con,row,receipt,run_id))
        con.execute('UPDATE runs SET finished_at=? WHERE run_id=?', (now_iso(),run_id))
        if args.dry_run:
            con.rollback()
        else:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    print(json.dumps({'updated':changed,'events':changed,'already_applied':len(rows)-changed,'merged_conflicts':0,'missing_src_path_rows':0,'run_id':run_id,'dry_run':args.dry_run}))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
