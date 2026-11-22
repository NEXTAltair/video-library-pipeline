import json
import sqlite3

from video_pipeline.application.historical_epg_audit import audit_rows, audit_database


def test_shared_evidence_and_accepted_documentary_are_candidates_not_corrections():
    epg = [{'category': '情報/ワイドショー', 'subcategory': 'グルメ'},
           {'category': '趣味/教育', 'subcategory': '料理'}]
    rows = [{'path_id': str(i), 'program_title': title, 'epg_genres': epg,
             'genre': 'ドキュメンタリー・情報', 'source': 'human_reviewed'}
            for i, title in enumerate(['Cooking A', 'Cooking B', 'Drama A', 'Drama B', 'Animation A'])]
    rows.append({'path_id': '5', 'program_title': 'Heroes', 'genre': 'ドキュメンタリー・情報',
                 'source': 'rule_based', 'epg_genres': [{'category': 'ドキュメンタリー', 'subcategory': '歴史'}]})
    rows.append({'path_id': '6', 'program_title': 'Clean', 'genre': 'アニメ', 'epg_genres': []})
    before = json.dumps(rows)
    report = audit_rows(rows)
    assert {c['path_id'] for c in report['candidates']} == set(map(str, range(6)))
    assert 'shared_epg_signature' in report['candidates'][2]['reasons']
    assert report['row_overrides'] == {} and report['db_writes'] == 0
    assert json.dumps(rows) == before


def test_db_audit_is_readonly_and_missing_db_not_created(tmp_path):
    db = tmp_path / 'copy.sqlite'
    with sqlite3.connect(db) as con:
        con.execute('CREATE TABLE path_metadata(path_id TEXT, program_title TEXT, source TEXT, data_json TEXT)')
        con.execute('INSERT INTO path_metadata VALUES (?,?,?,?)', ('p', 'Synthetic', 'human_reviewed', json.dumps({
            'genre': 'ドキュメンタリー・情報', 'epg_genres': [{'category': 'ドキュメンタリー'}]})))
    before = db.read_bytes()
    assert audit_database(db)['candidates'][0]['path_id'] == 'p'
    assert db.read_bytes() == before
    import pytest
    with pytest.raises(sqlite3.OperationalError):
        audit_database(tmp_path / 'missing.sqlite')
    assert not (tmp_path / 'missing.sqlite').exists()
