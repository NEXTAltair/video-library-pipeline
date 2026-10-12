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
        assert con.execute("SELECT program_title,human_reviewed,source FROM path_metadata").fetchall() == [("After", 1, "human_reviewed")] * 2
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
        assert con.execute("SELECT COUNT(*) FROM path_metadata WHERE source='human_reviewed'").fetchone()[0] == 2
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
