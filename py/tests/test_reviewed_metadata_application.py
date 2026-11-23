from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest
import yaml

from mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.application.reviewed_metadata import apply_reviewed_metadata
from video_pipeline.workflows import WorkflowStore, SourceRootApplyConfig, SourceRootWorkflowService, RelocateApplyConfig, RelocateWorkflowService


def fixture(tmp_path, *, flow="source_root", count=2):
    ops = tmp_path / "ops"
    ops.mkdir()
    db = ops / "mediaops.sqlite"
    with closing(connect_db(str(db))) as con:
        create_schema_if_needed(con)
        for i in range(count):
            con.execute("INSERT INTO paths(path_id,path,created_at,updated_at) VALUES (?,?,?,?)",
                        (f"p{i}", rf"B:\Unwatched\show{i}.mp4", "before", "before"))
        con.commit()
    store = WorkflowStore(ops)
    run = store.init_run(flow, run_id="run_review", config_snapshot={"db": str(db)})
    ids, paths = [], []
    for i in range(count):
        source = store.run_dir(run.run_id) / "metadata" / f"{i}.jsonl"
        row = {"path_id": f"p{i}", "path": rf"B:\Unwatched\show{i}.mp4", "program_title": "Before",
               "air_date": "2026-10-10", "needs_review": True, "needs_review_reason": "needs_review_flagged"}
        source.write_text(json.dumps(row) + "\n")
        extraction = store.register_artifact(run.run_id, artifact_type="metadata_extract_output", path=source,
                                            producer="fixture", artifact_id=f"extract_{i}")
        draft = store.run_dir(run.run_id) / "review" / f"{i}.yaml"
        draft.write_text(yaml.safe_dump({"source_jsonl": str(source), "hints": [{"canonical_title": "After", "aliases": ["Before"]}]}))
        review = store.register_artifact(run.run_id, artifact_type="metadata_review_yaml", path=draft,
                                        producer="fixture", artifact_id=f"review_{i}", input_artifact_ids=[extraction.id])
        ids.append(review.id)
        paths.append(str(draft))
    store.create_review_gate(run.run_id, gate_type="metadata_review", gate_id="metadata_review", artifact_ids=ids)
    run = store.read_run(run.run_id)
    run.phase = "review_required"
    store.write_run(run)
    config_class = SourceRootApplyConfig if flow == "source_root" else RelocateApplyConfig
    config = config_class(windows_ops_root=str(ops), run_id=run.run_id, db=str(db), gate_id="metadata_review",
                          artifact_ids=ids, review_yaml_paths=paths)
    return store, db, config


def db_state(db):
    with closing(sqlite3.connect(db)) as con:
        return "\n".join(con.iterdump())


@pytest.mark.parametrize("case", ["missing_run", "wrong_phase", "unsupported_flow", "closed_gate", "wrong_gate",
                                 "wrong_artifacts", "wrong_draft", "wrong_db", "stale_revision", "wrong_digest",
                                 "source_changed", "yaml_redirect", "bad_json", "invalid_second_batch", "overlap", "non_title_risk"])
def test_preflight_rejects_without_changing_db_manifest_or_drafts(tmp_path, case):
    store, db, cfg = fixture(tmp_path)
    if case == "missing_run": cfg = replace(cfg, run_id="missing")
    if case in {"wrong_phase", "unsupported_flow", "closed_gate"}:
        run = store.read_run(cfg.run_id)
        if case == "wrong_phase": run.phase = "complete"
        if case == "unsupported_flow": run.flow = "dedup"
        if case == "closed_gate": run.review_gates[cfg.gate_id].status = "approved"
        store.write_run(run)
    if case == "wrong_gate": cfg = replace(cfg, gate_id="other_gate")
    if case == "wrong_artifacts": cfg = replace(cfg, artifact_ids=["foreign_artifact"])
    if case == "wrong_draft": cfg = replace(cfg, review_yaml_paths=[str(tmp_path / "foreign.yaml")])
    if case == "wrong_db": cfg = replace(cfg, db=str(tmp_path / "foreign.sqlite"))
    if case == "stale_revision": cfg = replace(cfg, run_revision=0)
    if case == "wrong_digest": cfg = replace(cfg, review_digests={"review_0": "0" * 64, "review_1": "0" * 64})
    draft = Path(cfg.review_yaml_paths[-1]) if case != "wrong_draft" else Path(store.run_dir("run_review") / "review/1.yaml")
    source = store.run_dir("run_review") / "metadata/1.jsonl"
    if case == "source_changed": source.write_text(source.read_text() + "\n")
    if case == "yaml_redirect":
        body = yaml.safe_load(draft.read_text()); body["source_jsonl"] = str(tmp_path / "other.jsonl")
        draft.write_text(yaml.safe_dump(body))
    if case == "invalid_second_batch": draft.write_text("hints: not-a-list\n")
    if case in {"bad_json", "overlap", "non_title_risk"}:
        row = json.loads(source.read_text())
        if case == "overlap": row["path_id"] = "p0"
        if case == "non_title_risk": row["needs_review_reason"] = "short_recording"
        source.write_text("not json\n" if case == "bad_json" else json.dumps(row) + "\n")
        run = store.read_run("run_review")
        run.artifacts["extract_1"].sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
        store.write_run(run)
    original_manifest = store.manifest_path("run_review").read_bytes()
    drafts = {p: p.read_bytes() for p in store.run_dir("run_review").glob("review/*.yaml")}
    original_db = db_state(db)
    with pytest.raises((ValueError, FileNotFoundError)):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == original_db
    assert store.manifest_path("run_review").read_bytes() == original_manifest
    assert all(p.read_bytes() == body for p, body in drafts.items())
    assert not list(db.parent.glob("*.bak_*"))
    assert not (store.run_dir("run_review") / "review/accepted").exists()


def test_second_batch_sql_failure_rolls_back_first_batch_and_receipt(tmp_path):
    store, db, cfg = fixture(tmp_path)
    with closing(sqlite3.connect(db)) as con:
        con.execute("CREATE TRIGGER fail_later BEFORE INSERT ON path_metadata WHEN NEW.path_id='p1' BEGIN SELECT RAISE(ABORT,'injected failure'); END")
        con.commit()
    before = db_state(db)
    with pytest.raises(sqlite3.IntegrityError, match="injected failure"):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == before
    assert store.read_run(cfg.run_id).review_gates[cfg.gate_id].status == "open"
    assert store.read_run(cfg.run_id).phase == "review_required"


def test_explicit_row_review_can_correct_date_and_genre_without_editing_extraction(tmp_path):
    store, db, cfg = fixture(tmp_path)
    source = store.run_dir(cfg.run_id) / 'metadata/0.jsonl'
    before = source.read_bytes()
    draft = Path(cfg.review_yaml_paths[0])
    body = yaml.safe_load(draft.read_text())
    body['row_overrides'] = {'p0': {'program_title': 'VIVANT', 'air_date': '2026-09-10', 'genre': 'drama',
                                  'epg_genres': None, 'broadcaster': None, 'needs_review': False,
                                  'needs_review_reason': '', 'review_note': 'Recording title/date verified; foreign EPG discarded.'}}
    draft.write_text(yaml.safe_dump(body))
    result = apply_reviewed_metadata(store, cfg)
    assert result['rows'] == 2
    assert source.read_bytes() == before
    with closing(sqlite3.connect(db)) as con:
        title, air, raw = con.execute("SELECT program_title,air_date,data_json FROM path_metadata WHERE path_id='p0'").fetchone()
        assert (title, air) == ('VIVANT', '2026-09-10')
        stored = json.loads(raw)
        assert stored['genre'] == 'drama'
        assert stored['review_note'].startswith('Recording title/date verified')


@pytest.mark.parametrize('case', ['foreign_id', 'path', 'path_id', 'file_id', 'source', 'human_reviewed',
                                  'dest_root', 'no_note', 'invalid_date', 'invalid_bool', 'invalid_genre', 'invalid_episode'])
def test_bad_row_override_does_not_change_database_or_acceptance_state(tmp_path, case):
    store, db, cfg = fixture(tmp_path)
    draft = Path(cfg.review_yaml_paths[0])
    body = yaml.safe_load(draft.read_text())
    fields = {'review_note': 'Reviewed exact source evidence.'}
    if case in ['path', 'path_id', 'file_id', 'source', 'human_reviewed', 'dest_root']:
        fields[case] = 'not-permitted'
    if case == 'no_note': fields = {'needs_review': False}
    if case == 'invalid_date': fields['air_date'] = '2026-02-30'
    if case == 'invalid_bool': fields['needs_review'] = 'false'
    if case == 'invalid_genre': fields['epg_genres'] = [{'category': 123}]
    if case == 'invalid_episode': fields['episode_no'] = {'episode': 2}
    body['row_overrides'] = {'foreign' if case == 'foreign_id' else 'p0': fields}
    draft.write_text(yaml.safe_dump(body))
    before = db_state(db)
    manifest = store.manifest_path(cfg.run_id).read_bytes()
    with pytest.raises(ValueError):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == before
    assert store.manifest_path(cfg.run_id).read_bytes() == manifest
    assert not list(db.parent.glob('*.bak_*'))


def test_atomic_apply_freezes_drafts_keeps_references_and_replays_committed_result(tmp_path):
    store, db, cfg = fixture(tmp_path)
    drafts = {p: Path(p).read_bytes() for p in cfg.review_yaml_paths}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: apply_reviewed_metadata(store, cfg), range(2)))
    assert results[0] == results[1]
    assert results[0]["rows"] == 2
    assert all(Path(p).read_bytes() == body for p, body in drafts.items())
    accepted = Path(results[0]["acceptedDirectory"])
    assert len(list(accepted.glob("*.yaml"))) == 2
    assert len(list(db.parent.glob("*.bak_*"))) == 1
    with closing(sqlite3.connect(db)) as con:
        assert con.execute("SELECT program_title,human_reviewed,source FROM path_metadata").fetchall() == [("After", 0, "agent_accepted")] * 2
        assert con.execute("SELECT COUNT(*) FROM workflow_metadata_commits").fetchone()[0] == 1
    # An interrupted caller cannot silently apply newly edited bytes to a
    # gate whose earlier exact acceptance was already committed.
    draft = Path(cfg.review_yaml_paths[0])
    draft.write_text(draft.read_text().replace("After", "Different"))
    before = db_state(db)
    with pytest.raises(ValueError, match="different accepted review"):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == before


@pytest.mark.parametrize("flow", ["source_root", "relocate"])
def test_service_rejection_does_not_close_or_block_gate_or_run(tmp_path, flow):
    store, db, cfg = fixture(tmp_path, flow=flow)
    service = SourceRootWorkflowService() if flow == "source_root" else RelocateWorkflowService()
    before_manifest, before_db = store.manifest_path(cfg.run_id).read_bytes(), db_state(db)
    rejected = service.resume(replace(cfg, gate_id="wrong_gate"), action="apply_reviewed_metadata")
    assert not rejected.ok
    assert store.manifest_path(cfg.run_id).read_bytes() == before_manifest
    assert db_state(db) == before_db


def test_cli_invalid_run_has_nonzero_exit_and_one_json_response(tmp_path):
    store, db, cfg = fixture(tmp_path)
    cli = Path(__file__).resolve().parents[1] / "workflow_cli.py"
    result = subprocess.run([sys.executable, str(cli), "resume", "--windows-ops-root", cfg.windows_ops_root,
                             "--run-id", "missing", "--db", str(db), "--action", "apply_reviewed_metadata"],
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert json.loads(result.stdout)["ok"] is False
    assert db_state(db).count("INSERT INTO \"path_metadata\"") == 0


def test_process_exit_after_commit_can_replay_without_a_second_database_apply(tmp_path):
    store, db, cfg = fixture(tmp_path)
    py_root = Path(__file__).resolve().parents[1]
    code = """
import os, sys
sys.path.insert(0, sys.argv[1])
from video_pipeline.workflows import WorkflowStore, SourceRootApplyConfig
from video_pipeline.application.reviewed_metadata import apply_reviewed_metadata
apply_reviewed_metadata(WorkflowStore(sys.argv[2]), SourceRootApplyConfig(windows_ops_root=sys.argv[2], run_id='run_review'))
os._exit(23)
"""
    result = subprocess.run([sys.executable, "-c", code, str(py_root), cfg.windows_ops_root], capture_output=True, text=True)
    assert result.returncode == 23, result.stderr
    before = db_state(db)
    replay = apply_reviewed_metadata(store, cfg)
    assert replay["ok"] and replay["rows"] == 2
    assert db_state(db) == before
    assert len(list(db.parent.glob("*.bak_*"))) == 1


def test_simultaneous_resume_has_one_side_effect_owner(tmp_path, monkeypatch):
    from video_pipeline.workflows import WorkflowResult
    store, db, cfg = fixture(tmp_path)
    service = SourceRootWorkflowService()
    def plan_after(store, run_id):
        store.transition_run(run_id, "plan_ready")
        return WorkflowResult(ok=True, run_id=run_id, flow="source_root", phase="plan_ready", outcome="fixture_planned")
    monkeypatch.setattr(service, "_plan_after_metadata_accepted", plan_after)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: service.resume(cfg, action="apply_reviewed_metadata"), range(2)))
    assert sorted(result.ok for result in results) == [False, True]
    run = store.read_run(cfg.run_id)
    assert run.phase == "plan_ready" and run.review_gates[cfg.gate_id].status == "approved"
    with closing(sqlite3.connect(db)) as con:
        assert con.execute("SELECT COUNT(*) FROM workflow_metadata_commits").fetchone()[0] == 1
    assert len(list(db.parent.glob("*.bak_*"))) == 1


@pytest.mark.parametrize("flow", ["source_root", "relocate"])
def test_planning_failure_reports_committed_metadata_and_does_not_offer_reapplication(tmp_path, monkeypatch, flow):
    store, db, cfg = fixture(tmp_path, flow=flow)
    service = SourceRootWorkflowService() if flow == "source_root" else RelocateWorkflowService()
    def fail(*args):
        raise RuntimeError("injected planning failure after commit")
    if flow == "source_root":
        monkeypatch.setattr(service, "_plan_after_metadata_accepted", fail)
    else:
        monkeypatch.setattr(service, "_dry_run_existing", fail)
    result = service.resume(cfg, action="apply_reviewed_metadata")
    assert not result.ok and result.phase == "blocked"
    assert result.outcome.endswith("plan_after_review_failed")
    assert result.diagnostics[-1].details["metadataCommitted"] is True
    assert store.read_run(cfg.run_id).review_gates[cfg.gate_id].status == "approved"
    assert not result.next_actions
    with closing(sqlite3.connect(db)) as con:
        assert con.execute("SELECT COUNT(*) FROM path_metadata WHERE source='agent_accepted'").fetchone()[0] == 2
    before = db_state(db)
    retry = service.resume(cfg, action="apply_reviewed_metadata")
    assert not retry.ok
    assert db_state(db) == before


def test_cli_routes_helper_logs_to_stderr_and_returns_a_failed_payload_exit(tmp_path, monkeypatch, capsys):
    import workflow_cli
    def helper(args):
        print("diagnostic from helper")
        return {"ok": False, "error": "injected failure"}
    monkeypatch.setattr(workflow_cli, "_cmd_status", helper)
    monkeypatch.setattr(sys, "argv", ["workflow_cli.py", "status", "--windows-ops-root", str(tmp_path)])
    assert workflow_cli.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"ok": False, "error": "injected failure"}
    assert "diagnostic from helper" in captured.err


@pytest.mark.parametrize('franchise', ['Reviewed Franchise', None])
def test_reviewed_overrides_clear_existing_fields_and_preserve_franchise(tmp_path, franchise):
    store, db, cfg = fixture(tmp_path)
    with closing(connect_db(str(db))) as con:
        con.execute("INSERT INTO path_metadata(path_id,source,data_json,broadcaster,updated_at) VALUES(?,?,?,?,?)",
                    ('p0', 'rule_based', json.dumps({'epg_genres': [{'category': 'ニュース'}],
                     'broadcaster': 'Wrong station', 'franchise': 'Wrong franchise'}), 'Wrong station', 'before'))
        con.commit()
    draft = Path(cfg.review_yaml_paths[0])
    body = yaml.safe_load(draft.read_text())
    body['row_overrides'] = {'p0': {'epg_genres': None, 'broadcaster': None, 'franchise': franchise,
                                   'review_note': 'Verified individual recording and discarded foreign EPG.'}}
    draft.write_text(yaml.safe_dump(body))
    apply_reviewed_metadata(store, cfg)
    with closing(sqlite3.connect(db)) as con:
        raw, station = con.execute("SELECT data_json,broadcaster FROM path_metadata WHERE path_id='p0'").fetchone()
        stored = json.loads(raw)
        assert station is None and stored.get('epg_genres') is None
        assert stored.get('franchise') == franchise
        assert any('epg_genres' in h['fields'] and 'broadcaster' in h['fields'] for h in stored['source_history'])


def test_explicit_title_review_does_not_trust_contaminated_old_folder():
    from video_pipeline.application.reviewed_metadata import review_rows
    row = {'path_id': 'a-studio', 'path': r'B:\VideoLibrary\A-Studio+ゲスト対談\show.mp4',
           'program_title': 'A-Studio+ゲスト対談', 'air_date': '2026-11-13', 'needs_review': False}
    original = dict(row)
    overrides = {'a-studio': {'program_title': 'A-Studio+', 'review_note': '先頭番組名/EPGトークバラエティを確認'}}
    reviewed = review_rows([row], [], overrides)
    assert reviewed[0]['program_title'] == 'A-Studio+'
    assert reviewed[0]['human_reviewed'] is False
    assert reviewed[0]['review_acceptance'] == 'agent_accepted'
    assert row == original


@pytest.mark.parametrize('case', ['no_note', 'contaminated_title', 'needs_review', 'no_title_override'])
def test_explicit_title_review_keeps_other_risk_guards(case):
    from video_pipeline.application.reviewed_metadata import review_rows
    row = {'path_id': 'a-studio', 'path': r'B:\VideoLibrary\A-Studio+ゲスト対談\show.mp4',
           'program_title': 'A-Studio+', 'air_date': '2026-11-13', 'needs_review': False}
    fields = {'program_title': 'A-Studio+', 'review_note': '先頭番組名を確認'}
    if case == 'no_note': fields.pop('review_note')
    if case == 'contaminated_title': fields['program_title'] = 'A-Studio+▽ゲスト対談'
    if case == 'needs_review': fields['needs_review'] = True
    if case == 'no_title_override': fields.pop('program_title')
    with pytest.raises(ValueError):
        review_rows([row], [], {'a-studio': fields})


def test_mixed_artifacts_preserve_program_genre_identity(tmp_path):
    store, db, cfg = fixture(tmp_path)
    source = store.run_dir(cfg.run_id) / 'metadata/0.jsonl'
    row = json.loads(source.read_text())
    row.update(needs_review=False, needs_review_reason='', human_reviewed=True)
    source.write_text(json.dumps(row) + '\n')
    run = store.read_run(cfg.run_id)
    run.artifacts['extract_0'].sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    store.write_run(run)
    draft = Path(cfg.review_yaml_paths[0])
    body = yaml.safe_load(draft.read_text())
    body['hints'] = []
    body['program_genre_overrides'] = {'Before': {'genre': 'news', 'review_note': 'category checked'}}
    draft.write_text(yaml.safe_dump(body))
    with closing(connect_db(str(db))) as con:
        con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,human_reviewed) VALUES (?,?,?,?,?,?,?)",
                    ('p0', 'human_reviewed', json.dumps({'human_reviewed_at': 'original', 'review_note': 'current DB note'}), 'before', 'Current Identity', '2026-09-09', 1))
        con.commit()
    result = apply_reviewed_metadata(store, cfg)
    assert result['rows'] == 2
    with closing(sqlite3.connect(db)) as con:
        first = con.execute("SELECT source,program_title,air_date,human_reviewed FROM path_metadata WHERE path_id='p0'").fetchone()
        assert first == ('human_reviewed', 'Current Identity', '2026-09-09', 1)
        assert con.execute("SELECT program_title FROM path_metadata WHERE path_id='p1'").fetchone()[0] == 'After'

    with closing(sqlite3.connect(db)) as con:
        data = json.loads(con.execute("SELECT data_json FROM path_metadata WHERE path_id='p0'").fetchone()[0])
        assert data['review_note'] == 'current DB note; category checked'
        assert 'franchise' not in data


def test_partial_program_selection_across_artifacts_rejects_genre_split(tmp_path):
    store, db, cfg = fixture(tmp_path)
    run = store.read_run(cfg.run_id)
    for i, path in enumerate(cfg.review_yaml_paths):
        source = store.run_dir(cfg.run_id) / f'metadata/{i}.jsonl'
        row = json.loads(source.read_text())
        row.update(needs_review=False, needs_review_reason='', genre='ニュース・報道')
        source.write_text(json.dumps(row) + '\n')
        run.artifacts[f'extract_{i}'].sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
        draft = Path(path)
        body = yaml.safe_load(draft.read_text())
        body['hints'] = []
        if i == 0:
            body['program_genre_overrides'] = {'Before': {'genre': 'movie', 'review_note': 'checked'}}
        draft.write_text(yaml.safe_dump(body))
    store.write_run(run)
    before = db_state(db)
    with pytest.raises(ValueError, match='genre split'):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == before


def test_reviewed_null_genre_survives_epg_inference_and_requires_preparation(tmp_path):
    from relocate_existing_files import genre_unresolved_default_fallback
    from video_pipeline.domain.path_placement_rules import DriveRoute, DriveRoutes

    store, db, cfg = fixture(tmp_path, count=1)
    with closing(connect_db(str(db))) as con:
        con.execute(
            "INSERT INTO path_metadata(path_id,source,data_json,updated_at) VALUES(?,?,?,?)",
            ('p0', 'rule_based', json.dumps({'genre': 'news', 'epg_genres': [
                {'category': 'ニュース'}]}), 'before'),
        )
        con.commit()
    draft = Path(cfg.review_yaml_paths[0])
    body = yaml.safe_load(draft.read_text())
    body['row_overrides'] = {'p0': {
        'genre': None, 'review_note': 'Reviewed genre remains unknown; retain EPG as evidence.',
    }}
    draft.write_text(yaml.safe_dump(body))

    assert apply_reviewed_metadata(store, cfg)['rows'] == 1
    with closing(sqlite3.connect(db)) as con:
        raw = con.execute("SELECT data_json FROM path_metadata WHERE path_id='p0'").fetchone()[0]
    stored = json.loads(raw)
    assert 'genre' in stored and stored['genre'] is None
    assert stored['epg_genres'] == [{'category': 'ニュース'}]
    routes = DriveRoutes(r'B:\Library', 'by_title', [DriveRoute({
        'genre': 'news', 'dest_root': r'N:\News', 'layout': 'by_title',
    })])
    route, _ = routes.resolve(stored)
    assert route is None
    assert genre_unresolved_default_fallback(routes, stored, 'default')
@pytest.mark.parametrize('source,human', [('human_reviewed', 1), ('agent_accepted', 0), ('rule_based', 0)])
@pytest.mark.parametrize('replacement', [None, [], [{'category': 'ドラマ', 'subcategory': '国内ドラマ'}]])
def test_explicit_run_review_corrects_accepted_genre_and_epg(tmp_path, source, human, replacement):
    store, db, cfg = fixture(tmp_path, count=1)
    with sqlite3.connect(db) as con:
        con.execute('INSERT INTO path_metadata(path_id,source,data_json,updated_at,human_reviewed) VALUES(?,?,?,?,?)',
                    ('p0', source, json.dumps({'genre': 'ドキュメンタリー・情報',
                     'epg_genres': [{'category': '情報', 'subcategory': 'グルメ'}]}), 'before', human))
    draft = Path(cfg.review_yaml_paths[0])
    body = yaml.safe_load(draft.read_text())
    body['row_overrides'] = {'p0': {'genre': 'ドラマ', 'epg_genres': replacement,
                                   'needs_review': False, 'needs_review_reason': '',
                                   'review_note': 'Independent synthetic program evidence checked; foreign EPG discarded.'}}
    before = db_state(db)
    draft.write_text(yaml.safe_dump(body))
    assert db_state(db) == before  # drafting is not acceptance
    apply_reviewed_metadata(store, cfg)
    with closing(sqlite3.connect(db)) as con:
        data = json.loads(con.execute("SELECT data_json FROM path_metadata WHERE path_id='p0'").fetchone()[0])
        assert data['genre'] == 'ドラマ'
        assert data['epg_genres'] == replacement
        assert any('epg_genres' in h.get('fields', []) for h in data['source_history'])


def _partition_fixture(tmp_path):
    store, db, cfg = fixture(tmp_path, flow="relocate")
    root = store.run_dir(cfg.run_id)
    rows = [json.loads((root / f"metadata/{i}.jsonl").read_text()) for i in range(2)]
    queue_path = root / "metadata/queue.jsonl"
    queue_path.write_text("\n".join(map(json.dumps, rows)))
    queue = store.register_artifact(cfg.run_id, artifact_type="relocate_metadata_queue", path=queue_path,
                                    producer="fixture", artifact_id="queue")
    run = store.read_run(cfg.run_id)
    for i, row in enumerate(rows):
        source = run.artifacts[f"extract_{i}"]
        source.input_artifact_ids = [queue.id]
        draft = Path(cfg.review_yaml_paths[i])
        body = yaml.safe_load(draft.read_bytes())
        body.update(metadata_partition={"selected_path_ids": [row["path_id"]] if not i else [],
                                       "held_path_ids": [row["path_id"]] if i else []},
                    cohort={"source_jsonl_sha256": source.sha256, "path_id_count": 1,
                            "path_id_set_hash": "metadata_partition/v1:" + hashlib.sha256(row["path_id"].encode()).hexdigest()[:16]})
        draft.write_text(yaml.safe_dump(body))
    store.write_run(run)
    return store, db, replace(cfg, run_revision=store.read_run(cfg.run_id).revision,
                              review_digests=_digests(cfg))


def _digests(cfg):
    return {aid: hashlib.sha256(Path(p).read_bytes()).hexdigest()
            for aid, p in zip(cfg.artifact_ids, cfg.review_yaml_paths)}


def test_partition_acceptance_is_replayable_and_held_rows_are_untouched(tmp_path):
    store, db, cfg = _partition_fixture(tmp_path)
    receipt = apply_reviewed_metadata(store, cfg)
    assert receipt["metadataPartition"] == {"selectedPathIds": ["p0"], "heldPathIds": ["p1"],
        "queueArtifactId": "queue", "queueSha256": store.read_run(cfg.run_id).artifacts["queue"].sha256}
    assert receipt["rows"] == 1 and apply_reviewed_metadata(store, cfg) == receipt
    with closing(sqlite3.connect(db)) as con:
        assert con.execute("SELECT path_id,program_title FROM path_metadata").fetchall() == [("p0", "After")]
        assert con.execute("SELECT COUNT(*) FROM paths WHERE updated_at='before'").fetchone() == (2,)


@pytest.mark.parametrize("case", ["foreign", "overlap", "duplicate", "missing", "mixed", "held_edit", "risk",
                                  "queue_changed", "queue_incomplete", "wrong_path", "missing_digest",
                                  "missing_revision", "physical_args", "all_held", "all_chosen", "unversioned"])
def test_bad_metadata_partition_fails_without_db_or_gate_change(tmp_path, case):
    store, db, cfg = _partition_fixture(tmp_path)
    p = Path(cfg.review_yaml_paths[int(case not in {"risk", "all_held"})])
    body = yaml.safe_load(p.read_bytes())
    part = body["metadata_partition"]
    if case == "foreign": part["held_path_ids"] = ["other"]
    if case == "overlap": part["selected_path_ids"] = ["p1"]
    if case == "duplicate": part["held_path_ids"] *= 2
    if case == "missing": part["held_path_ids"] = []
    if case == "mixed": del body["metadata_partition"]
    if case == "unversioned": body["cohort"]["path_id_set_hash"] = body["cohort"]["path_id_set_hash"].split(":")[1]
    if case == "held_edit": body["row_overrides"] = {"p1": {"review_note": "held"}}
    if case == "risk": body["hints"] = []
    if case == "all_held": part.update(selected_path_ids=[], held_path_ids=["p0"])
    if case == "all_chosen": part.update(selected_path_ids=["p1"], held_path_ids=[])
    p.write_text(yaml.safe_dump(body))
    cfg = replace(cfg, review_digests=_digests(cfg))
    if case.startswith("queue_") or case == "wrong_path":
        run = store.read_run(cfg.run_id)
        queue = Path(run.artifacts["queue"].path)
        rows = [json.loads(line) for line in queue.read_text().splitlines()]
        if case == "wrong_path": rows[1]["path"] += ".other"
        else: rows.pop()
        queue.write_text("\n".join(map(json.dumps, rows)))
        if case != "queue_changed":
            run.artifacts["queue"].sha256 = hashlib.sha256(queue.read_bytes()).hexdigest()
            store.write_run(run)
            cfg = replace(cfg, run_revision=store.read_run(cfg.run_id).revision)
    if case == "missing_digest": cfg = replace(cfg, review_digests={})
    if case == "missing_revision": cfg = replace(cfg, run_revision=None)
    if case == "physical_args": cfg = replace(cfg, selected_path_ids=["p0"])
    before, manifest = db_state(db), store.manifest_path(cfg.run_id).read_bytes()
    with pytest.raises(ValueError):
        apply_reviewed_metadata(store, cfg)
    assert db_state(db) == before and store.manifest_path(cfg.run_id).read_bytes() == manifest
    assert not (store.run_dir(cfg.run_id) / "review/accepted").exists()


def test_partition_after_registration_does_not_reserve_own_earlier_unregistered_sources(tmp_path):
    from video_pipeline.platform.pathscan_common import path_id_for
    from video_pipeline.workflows.relocate_metadata_scope import resolve_scope, check_output
    from tests.test_relocate_metadata_scope import install_gate
    store = WorkflowStore(tmp_path)
    rid = store.init_run("relocate", run_id="run_registered_partition").run_id
    root = store.run_dir(rid)
    src, held = r"E:\Old\Show.mp4", r"E:\Old\Other.mp4"
    pid, hid = path_id_for(src), path_id_for(held)
    initial = root / "plan/before.jsonl"
    initial.write_text(json.dumps({"src": src, "status": "skipped", "reason": "unregistered_path"}))
    store.register_artifact(rid, artifact_type="relocate_plan", path=initial, producer="fixture")
    qp = root / "metadata/registered.jsonl"
    qp.write_text("\n".join(json.dumps({"path_id": i, "path": p}) for i, p in ((pid, src), (hid, held))))
    q = store.register_artifact(rid, artifact_type="relocate_metadata_queue", path=qp, producer="fixture")
    install_gate(store, rid, dict(queueArtifactId=q.id, queueSha256=q.sha256,
        selectedPathIds=[pid], heldPathIds=[hid]), q)
    scope = resolve_scope(store.read_run(rid), root)
    proposed = {"path_id": pid, "src": src, "dst": r"E:\New\Show.mp4", "status": "planned"}
    check_output(scope, [proposed])
    with pytest.raises(ValueError, match="overlaps"):
        check_output(scope, [dict(proposed, dst=held)])
