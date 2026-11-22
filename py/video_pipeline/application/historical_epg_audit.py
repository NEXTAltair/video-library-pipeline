"""Conservative historical EPG triage, never a classification or write operation.

Shared genre arrays are not proof of contamination. Accepted documentary rows
with EPG evidence are deliberately retained as a broad fallback: old acceptance
is not independent evidence that the inherited EPG belonged to this program.
"""
from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from db_helpers import reconstruct_path_metadata
from video_pipeline.domain.program_genre_policy import alias_key, genre_key


def audit_rows(rows):
    rows = list(rows)
    signatures = defaultdict(set)
    prepared = []
    for row in rows:
        epg = row.get('epg_genres')
        signature = json.dumps(epg, ensure_ascii=False, sort_keys=True) if isinstance(epg, list) and epg else None
        title = alias_key(str(row.get('program_title') or ''))
        if signature and title:
            signatures[signature].add(title)
        prepared.append((row, signature))
    candidates = []
    for row, signature in prepared:
        reasons = []
        peers = sorted(signatures[signature]) if signature else []
        if len(peers) > 1:
            reasons.append('shared_epg_signature')
        # Intentionally broad, low-confidence fallback includes isolated historical
        # cross-program evidence that cannot be identified from genres alone.
        if signature and genre_key(row.get('genre')) == 'documentary':
            reasons.append('documentary_epg_requires_independent_verification')
        if reasons:
            candidates.append({'path_id': str(row['path_id']), 'program_title': row.get('program_title'),
                               'genre': row.get('genre'), 'epg_genres': row.get('epg_genres'),
                               'source': row.get('source'), 'human_reviewed': bool(row.get('human_reviewed')),
                               'reasons': reasons, 'signature_programs': peers,
                               'confidence': 'candidate_only'})
    return {'schema': 'historical_epg_audit/v1', 'scanned_rows': len(rows), 'candidates': candidates,
            'row_overrides': {}, 'db_writes': 0, 'physical_moves': 0,
            'warning': 'Shared genres and documentary acceptance are triage signals, not proof. '
                       'Independently verify each program before editing a run-owned review.'}


def audit_database(db):
    """Read existing path metadata without schema setup, PRAGMA writes or backups."""
    uri = Path(db).resolve().as_uri() + '?mode=ro'
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    try:
        con.execute('PRAGMA query_only=ON')
        rows = []
        for row in con.execute('SELECT * FROM path_metadata ORDER BY path_id'):
            metadata = reconstruct_path_metadata(row)
            metadata.update(path_id=row['path_id'], source=row['source'])
            rows.append(metadata)
        return audit_rows(rows)
    finally:
        con.close()
