"""Provenance checks use isolated SQLite databases only."""
from contextlib import closing
from dataclasses import replace
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from db_helpers import is_human_reviewed_metadata, latest_path_metadata
from migrate_review_provenance import migrate
from tests.test_reviewed_metadata_application import fixture, db_state
from upsert_path_metadata_jsonl import upsert_records
from video_pipeline.application.reviewed_metadata import apply_reviewed_metadata, review_rows
from video_pipeline.domain.review_provenance import is_agent_accepted_metadata
from title_resolution import load_canonical_title_sources, suggest_canonical_title


def sample():
    return {'path_id': 'p0', 'program_title': 'Example Show', 'air_date': '2026-11-01', 'needs_review': False,
            'human_reviewed': True, 'human_reviewed_by': 'old', 'human_reviewed_at': 'old',
            'human_confirmation': {'confirmed_by': 'old', 'evidence': 'old'}}


def test_agent_acceptance_does_not_inherit_human_claims():
    old = sample()
    row = review_rows([old], [])[0]
    assert not row['human_reviewed']
    assert row['review_acceptance'] == 'agent_accepted'
    assert not row['human_confirmation']
    assert 'human_reviewed_at' not in row and 'human_reviewed_by' not in row
    assert old['human_reviewed'] is True
    assert is_agent_accepted_metadata('agent_accepted', row)
    assert not is_human_reviewed_metadata('agent_accepted', old)
    assert not is_human_reviewed_metadata('human_reviewed', row)


@pytest.mark.parametrize('confirmation', [None, {}, {'confirmed_by': 'Val'}, {'confirmed_by': '', 'evidence': 'message'}])
def test_human_confirmation_requires_named_original_evidence(confirmation):
    with pytest.raises(ValueError, match='confirmed_by and evidence'):
        review_rows([sample()], [], acceptance='human_confirmed', human_confirmation=confirmation)


def test_actual_human_confirmation_roundtrips_and_cannot_replay_as_agent(tmp_path):
    store, db, cfg = fixture(tmp_path)
    proof = {'confirmed_by': 'owner', 'evidence': 'explicit original message 123'}
    cfg = replace(cfg, acceptance='human_confirmed', human_confirmation=proof)
    result = apply_reviewed_metadata(store, cfg)
    assert result['reviewAcceptance'] == 'human_confirmed'
    assert apply_reviewed_metadata(store, cfg) == result
    with closing(sqlite3.connect(db)) as con:
        assert con.execute('SELECT source,human_reviewed FROM path_metadata').fetchall() == [('human_reviewed', 1)] * 2
    before = db_state(db)
    with pytest.raises(ValueError, match='different review provenance'):
        apply_reviewed_metadata(store, replace(cfg, acceptance='agent_accepted', human_confirmation=None))
    assert db_state(db) == before


def legacy(tmp_path):
    store, db, cfg = fixture(tmp_path)
    apply_reviewed_metadata(store, cfg)
    with closing(sqlite3.connect(db)) as con:
        con.execute("UPDATE path_metadata SET source='human_reviewed',human_reviewed=1")
        con.commit()
    inspected = migrate(db, ['p0'], inspect=True)['rows'][0]
    manifest = {'rows': {'p0': {'expected_sha256': inspected['expected_sha256'],
                              'evidence': 'original agent-only run/artifact reference'}}}
    return store, db, cfg, manifest


def test_scoped_migration_dryrun_apply_preserves_unknown_legacy_and_metadata(tmp_path):
    store, db, cfg, manifest = legacy(tmp_path)
    before = db_state(db)
    assert not migrate(db, ['p0'], manifest)['applied']
    assert db_state(db) == before
    result = migrate(db, ['p0'], manifest, apply=True)
    assert result['applied'] and Path(result['backupPath']).is_file()
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        row, source = latest_path_metadata(con, 'p0')
        assert source == 'agent_accepted' and not row['human_reviewed']
        assert row['program_title'] == 'After' and row['air_date'] == '2026-10-10'
        assert row['source_history'][-1]['evidence'] == manifest['rows']['p0']['evidence']
        assert con.execute("SELECT source,human_reviewed FROM path_metadata WHERE path_id='p1'").fetchone()[:] == ('human_reviewed', 1)


@pytest.mark.parametrize('case', ['no_evidence', 'wrong_scope', 'stale', 'confirmed_human'])
def test_migration_rejects_unsupported_evidence_atomically(tmp_path, case):
    _, db, _, manifest = legacy(tmp_path)
    if case == 'no_evidence': manifest['rows']['p0']['evidence'] = ''
    if case == 'wrong_scope': manifest['rows']['unknown'] = manifest['rows']['p0']
    if case == 'stale': manifest['rows']['p0']['expected_sha256'] = '0' * 64
    if case == 'confirmed_human':
        with closing(sqlite3.connect(db)) as con:
            raw, = con.execute("SELECT data_json FROM path_metadata WHERE path_id='p0'").fetchone()
            data = json.loads(raw); data['review_acceptance'] = 'human_confirmed'
            con.execute("UPDATE path_metadata SET data_json=? WHERE path_id='p0'", (json.dumps(data),)); con.commit()
        manifest['rows']['p0']['expected_sha256'] = migrate(db, ['p0'], inspect=True)['rows'][0]['expected_sha256']
    before = db_state(db)
    with pytest.raises(ValueError):
        migrate(db, ['p0'], manifest, apply=True)
    assert db_state(db) == before
    assert not list(db.parent.glob('*.bak_pre_migrate_review_provenance*'))


def test_agent_cannot_overwrite_unverified_legacy_human_rows(tmp_path):
    _, db, _, _ = legacy(tmp_path)
    before = db_state(db)
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN IMMEDIATE')
        with pytest.raises(ValueError, match='cannot overwrite human-reviewed'):
            upsert_records(con, [review_rows([sample()], [])[0]], source='agent_accepted', strict=True)
    assert db_state(db) == before


def test_legacy_commit_replay_reports_unknown_without_rewriting(tmp_path):
    store, db, cfg = fixture(tmp_path)
    summary = apply_reviewed_metadata(store, cfg)
    summary.pop('reviewAcceptance'); summary.pop('humanConfirmation')
    with closing(sqlite3.connect(db)) as con:
        con.execute('UPDATE workflow_metadata_commits SET summary_json=?', (json.dumps(summary),)); con.commit()
    before = db_state(db)
    replay = apply_reviewed_metadata(store, cfg)
    assert replay['reviewAcceptance'] == 'legacy_unverified' and replay['provenanceMigrationRequired']
    assert db_state(db) == before


def test_accepted_titles_are_canonical_without_human_label(tmp_path):
    store, db, cfg = fixture(tmp_path)
    apply_reviewed_metadata(store, cfg)
    with closing(sqlite3.connect(db)) as con:
        sources = load_canonical_title_sources(con)
    assert 'After' not in sources.human_reviewed
    assert 'After' in sources.agent_accepted
    assert suggest_canonical_title('After', sources, min_extra_chars=3) == (None, 'exact_agent_accepted')
    assert suggest_canonical_title('After' + 'a' * 20, sources, min_extra_chars=3) == ('After', 'agent_accepted')


def test_migration_cli_requires_explicit_scope_and_dryrun_is_readonly(tmp_path):
    _, db, _, manifest = legacy(tmp_path)
    proof = tmp_path / 'proof.json'; proof.write_text(json.dumps(manifest))
    cli = Path(__file__).resolve().parents[1] / 'migrate_review_provenance.py'
    before = db_state(db)
    result = subprocess.run([sys.executable, str(cli), '--db', str(db), '--path-id', 'p0',
                             '--evidence-json', str(proof), '--dry-run'], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['applied'] is False
    assert db_state(db) == before


def test_provenance_snapshot_tamper_is_not_accepted_on_replay(tmp_path):
    store, db, cfg = fixture(tmp_path)
    result = apply_reviewed_metadata(store, cfg)
    (Path(result['acceptedDirectory']) / 'provenance.json').write_text('{}')
    before = db_state(db)
    with pytest.raises(ValueError, match='snapshot was modified'):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == before


def test_agent_metadata_is_usable_for_relocation_without_relaxing_holds(tmp_path, monkeypatch, capsys):
    from tests.test_routing_inventory import Preview
    preview = Preview(tmp_path, monkeypatch)
    preview.add(r'B:\Library\Show accepted.mp4')
    preview.add(r'B:\Library\Show held.mp4', hold=True)
    preview.con.execute("UPDATE path_metadata SET source='agent_accepted',human_reviewed=0,data_json=?",
                        (json.dumps({'review_acceptance': 'agent_accepted'}),))
    summary, rows, _ = preview.run(monkeypatch, capsys)
    assert summary['plannedMoves'] == 1 and summary['heldSkipped'] == 1
    assert {row['reason'] for row in rows} == {'recompute_destination', 'recording_hold'}


def test_extraction_lock_preserves_acceptance_without_human_labels():
    from run_metadata_batches_promptv1 import _build_locked_row
    row = review_rows([sample()], [])[0]
    locked = _build_locked_row(row, {'path_id': 'p0'}, 'model', 'version')
    assert not locked['human_reviewed']
    assert locked['review_acceptance'] == 'agent_accepted'
    assert locked['title_extraction_path'] == 'accepted_review_lock'


def test_unreviewed_upsert_cannot_clobber_an_accepted_decision(tmp_path):
    store, db, cfg = fixture(tmp_path)
    apply_reviewed_metadata(store, cfg)
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN IMMEDIATE')
        upsert_records(con, [dict(sample(), program_title='Unreviewed guess', human_reviewed=False)], source='llm')
        con.commit()
        row, source = latest_path_metadata(con, 'p0')
        assert source == 'agent_accepted' and row['program_title'] == 'After'
        assert row['human_reviewed'] is False


def test_output_only_preservation_counts_agent_separately(tmp_path, monkeypatch, capsys):
    import run_metadata_batches_promptv1 as mod
    store, db, cfg = fixture(tmp_path)
    apply_reviewed_metadata(store, cfg)
    queue = tmp_path / 'queue.jsonl'
    queue.write_text(json.dumps({'path_id': 'p0', 'path': r'B:\Unwatched\show0.mp4', 'name': 'show0.mp4'}) + '\n')
    before = db_state(db)
    monkeypatch.setattr(sys, 'argv', ['extract', '--db', str(db), '--queue', str(queue),
                                    '--outdir', str(tmp_path / 'out'), '--output-only'])
    assert mod.main() == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary['preservedHumanReviewed'] == 0
    assert summary['preservedAgentAccepted'] == 1
    row = json.loads(Path(summary['latestOutputJsonlPath']).read_text().splitlines()[0])
    assert row['human_reviewed'] is False and row['review_acceptance'] == 'agent_accepted'
    assert db_state(db) == before


def test_old_frozen_snapshot_without_commit_is_accepted_as_agent_not_human(tmp_path):
    import hashlib
    from video_pipeline.application.reviewed_metadata import preflight
    store, db, cfg = fixture(tmp_path, count=1)
    source = store.run_dir(cfg.run_id) / 'metadata/0.jsonl'
    raw = json.loads(source.read_text())
    raw.update(human_reviewed=True, human_reviewed_by='legacy agent', human_reviewed_at='old')
    source.write_text(json.dumps(raw) + '\n')
    run = store.read_run(cfg.run_id)
    run.artifacts['extract_0'].sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    store.write_run(run)
    _, _, _, prepared = preflight(store, cfg)
    fingerprint = hashlib.sha256(json.dumps([[p['artifact'].id, p['digest'], p['sourceDigest']] for p in prepared], sort_keys=True).encode()).hexdigest()
    accepted = store.run_dir(cfg.run_id) / 'review' / 'accepted' / fingerprint
    accepted.mkdir(parents=True)
    (accepted / '0000.yaml').write_bytes(prepared[0]['draft'])
    (accepted / '0000.jsonl').write_bytes(prepared[0]['source'])
    result = apply_reviewed_metadata(store, cfg)
    assert result['reviewAcceptance'] == 'agent_accepted'
    with closing(sqlite3.connect(db)) as con:
        assert con.execute('SELECT source,human_reviewed FROM path_metadata').fetchone() == ('agent_accepted', 0)
    assert json.loads((accepted / 'provenance.json').read_text())['human_reviewed'] is False
    assert b'legacy agent' in (accepted / '0000.jsonl').read_bytes()  # Immutable raw input, not acceptance evidence.


def test_shared_writer_rejects_unsupported_human_source(tmp_path):
    _, db, _ = fixture(tmp_path)
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN IMMEDIATE')
        with pytest.raises(ValueError,match='human confirmation requires'):
            incoming = sample()
            incoming.pop('human_confirmation')
            upsert_records(con,[incoming],source='human_reviewed')
        assert con.execute('SELECT count(*) FROM path_metadata').fetchone()[0] == 0


def test_shared_writer_requires_marker_and_proof_for_human_source(tmp_path):
    _, db, _ = fixture(tmp_path)
    row = dict(sample(),human_confirmation={'confirmed_by':'owner','evidence':'original message'})
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN IMMEDIATE')
        with pytest.raises(ValueError,match='explicit human_confirmed provenance'):
            upsert_records(con,[row],source='human_reviewed')
        row['review_acceptance'] = 'human_confirmed'
        upsert_records(con,[row],source='human_reviewed')
        assert con.execute('SELECT source,human_reviewed FROM path_metadata').fetchone()[:] == ('human_reviewed',1)


def test_new_run_review_supersedes_legacy_claim_without_retroactive_migration(tmp_path):
    store, db, cfg = fixture(tmp_path)
    with closing(sqlite3.connect(db)) as con:
        for i in range(2):
            con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,needs_review,human_reviewed) VALUES(?,'human_reviewed',?,'old','Old','2026-10-10',0,1)",
                (f'p{i}',json.dumps({'human_reviewed_at':'old','human_confirmation':{'confirmed_by':'old-person','evidence':'old-proof'}})))
        con.commit()
    result = apply_reviewed_metadata(store,cfg)
    assert result['reviewAcceptance'] == 'agent_accepted'
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        row, source = latest_path_metadata(con,'p0')
        assert source == 'agent_accepted' and not row['human_reviewed']
        history = [h for h in row['source_history'] if h.get('source')=='review_superseded']
        assert len(history) == 1 and history[0]['previous_human_reviewed']
        assert history[0]['previous_human_confirmation']['evidence'] == 'old-proof'
        assert history[0]['review_context']['run_id'] == cfg.run_id
        assert 'provenance_migration_evidence' not in row


def test_normal_extraction_preserves_locked_human_and_processes_remaining_rows(tmp_path):
    _, db, _ = fixture(tmp_path)
    with closing(sqlite3.connect(db)) as con:
        con.row_factory = sqlite3.Row
        con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,needs_review,human_reviewed) VALUES('p0','human_reviewed','{}','old','Locked','2026-11-01',0,1)")
        con.commit()
        con.execute('BEGIN IMMEDIATE')
        locked = dict(sample(),program_title='Locked')
        fresh = {'path_id':'p1','program_title':'New','air_date':'2026-11-01','needs_review':False}
        result = upsert_records(con,[locked,fresh],source='rule_based')
        con.commit()
        assert result['rows'] == 1
        assert con.execute("SELECT source,program_title,human_reviewed FROM path_metadata WHERE path_id='p0'").fetchone()[:] == ('human_reviewed','Locked',1)
        assert con.execute("SELECT source,program_title,human_reviewed FROM path_metadata WHERE path_id='p1'").fetchone()[:] == ('rule_based','New',0)
