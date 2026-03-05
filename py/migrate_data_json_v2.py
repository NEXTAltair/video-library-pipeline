#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from franchise_resolver import resolve_franchise
from genre_resolver import resolve_genre
from mediaops_schema import begin_immediate, connect_db
from source_history import make_entry


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', required=True)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--drive-routes', default='')
    ap.add_argument('--franchise-rules', default='')
    args = ap.parse_args()

    con = connect_db(args.db)
    rows = con.execute('SELECT pm.path_id, pm.source, pm.data_json, p.path FROM path_metadata pm LEFT JOIN paths p ON p.path_id=pm.path_id').fetchall()
    updates = []
    for r in rows:
        try:
            data = json.loads(str(r['data_json']))
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        data.setdefault('path_id', r['path_id'])
        data.setdefault('path', r['path'])
        genre = resolve_genre(data, args.drive_routes or None)
        franchise = resolve_franchise(data, args.franchise_rules or None)
        if genre is not None:
            data['genre'] = genre
        if franchise is not None:
            data['franchise'] = franchise
        hist = data.get('source_history') if isinstance(data.get('source_history'), list) else []
        if not hist:
            keys = [k for k, v in data.items() if v is not None and k not in {'path_id', 'path', 'source_history'}]
            hist = [make_entry(str(r['source'] or 'unknown'), keys)]
        data['source_history'] = hist
        updates.append((json.dumps(data, ensure_ascii=False), r['path_id']))

    if args.dry_run:
        print(json.dumps({'ok': True, 'rows': len(updates), 'dryRun': True}, ensure_ascii=False))
        con.close()
        return 0

    begin_immediate(con)
    con.executemany('UPDATE path_metadata SET data_json=? WHERE path_id=?', updates)
    con.commit()
    con.close()
    print(json.dumps({'ok': True, 'rows': len(updates), 'dryRun': False}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
