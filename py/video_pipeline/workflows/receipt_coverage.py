"""Immutable successful-parent snapshots; coverage is committed only after DB success.

A native mover can outlive its caller and append receipts after an earlier flush.
Never acknowledge a mutable log wholesale. Bundle members are covered only with a
successful, complete parent; standalone member intents are not DB authority.
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile

from video_pipeline.domain.companion_bundle import parent_move_succeeded


def canonical(row):
    # Same receipt identity used by update_db_paths_from_move_apply.py.
    return json.dumps(row, sort_keys=True, ensure_ascii=False)


def digest(row):
    return hashlib.sha256(canonical(row).encode()).hexdigest()


def successful_receipts(log: Path):
    if not log.exists():
        return {}
    lines = log.read_text(encoding='utf-8-sig').splitlines()
    records = []
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            if index == len(lines)-1:
                break  # only a torn final write is recoverable
            raise
        if not isinstance(row, dict):
            raise ValueError('invalid receipt object')
        records.append(row)
    if any((r.get('_meta') or {}).get('dry_run') for r in records):
        return {}
    return {digest(r): r for r in records if r.get('op') == 'move'
            and parent_move_succeeded(r) and not r.get('dry_run')}


def coverage_entry(row):
    members = row.get('members', [])
    if not isinstance(members, list) or any(not isinstance(m, dict) or not isinstance(m.get('kind'), str) for m in members):
        raise ValueError('invalid receipt member coverage')
    indexed = {m['kind']: digest(m) for m in members}
    if len(indexed) != len(members):
        raise ValueError('duplicate receipt member coverage')
    return {'src': row['src'], 'dst': row['dst'], 'path_id': row.get('path_id'),
            'members': indexed}


def snapshot(run_dir: Path, rows):
    payload = ('\n'.join(canonical(r) for r in rows) + '\n').encode()
    key = hashlib.sha256(payload).hexdigest()
    directory = run_dir/'apply'/'db_flush'
    directory.mkdir(parents=True, exist_ok=True)
    target = directory/(key+'.jsonl')
    if target.exists():
        if target.read_bytes() != payload:
            raise ValueError('receipt snapshot content changed')
        return target
    with tempfile.NamedTemporaryFile(dir=directory, delete=False) as f:
        f.write(payload); f.flush(); os.fsync(f.fileno()); temporary=f.name
    os.replace(temporary, target)
    return target


def needs_flush(state):
    """Read-only status recovery: expose reconcile, never silently move on."""
    try:
        for chunk in state['chunks']:
            current = successful_receipts(Path(chunk['localLog']))
            covered = chunk.get('dbReceiptCoverage', {})
            if set(current) != set(covered):
                return True
            if any(covered[key] != coverage_entry(row) for key, row in current.items()):
                return True
    except (OSError, ValueError, TypeError, KeyError):
        return True  # reconciliation reports the exact corruption; no new moves
    return False
