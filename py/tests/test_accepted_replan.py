import hashlib
import json
import signal
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from tests.test_relocate_workflow import RelocateWorkflowHarness
from tests.test_relocate_metadata_scope import install_gate
from video_pipeline.workflows import RelocateApplyConfig, WorkflowPhase, WorkflowStore
from video_pipeline.workflows import accepted_replan
from workflow_cli import _run_to_status


@pytest.fixture
def interrupted(tmp_path):
    h = RelocateWorkflowHarness(tmp_path, run_id="interrupted")
    h.dry_run()
    store = WorkflowStore(h.cfg.windows_ops_root)
    rid = h.cfg.run_id
    root = store.run_dir(rid)
    store.transition_run(rid, WorkflowPhase.REVIEW_REQUIRED)
    path = root / "metadata/q.jsonl"
    path.write_text('\n'.join(json.dumps({"path_id": pid, "path": value}) for pid, value in [
        ("p1", r"B:\VideoLibrary\Old\show.mp4"), ("held", r"B:\VideoLibrary\held.mp4")]))
    q = store.register_artifact(rid, artifact_type="relocate_metadata_queue", artifact_id="q", producer="fixture", path=path)
    install_gate(store, rid, dict(queueArtifactId=q.id, queueSha256=q.sha256, selectedPathIds=["p1"],
                                 heldPathIds=["held"]), q)
    run = store.read_run(rid)
    gate = run.review_gates["review"]
    summary = gate.resolution["metadataApply"]
    summary.update(ok=True, rows=1, missing=0, acceptedDigest="f"*64, artifactIds=gate.artifact_ids)
    store.write_run(run)
    db = Path(h.cfg.db)
    db.unlink()
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE workflow_metadata_commits (operation_id TEXT PRIMARY KEY, digest TEXT, summary_json TEXT)")
        op = hashlib.sha256(json.dumps([str(root.resolve()), [gate.id]]).encode()).hexdigest()
        con.execute("INSERT INTO workflow_metadata_commits VALUES (?,?,?)", (op, summary["acceptedDigest"], json.dumps(summary)))
    def status():
        return _run_to_status(store.read_run(rid), include_artifacts=False, run_dir=root)
    def config():
        (item,) = status()["nextActions"]
        assert item["action"] == item["params"]["resumeAction"] == accepted_replan.ACTION
        p = item["params"]
        return RelocateApplyConfig(run_id=p["runId"], windows_ops_root=h.cfg.windows_ops_root,
                                   gate_id=p["gateId"], run_revision=p["runRevision"], artifact_ids=p["artifactIds"])
    h.calls.clear()
    return h, status, config


def test_replan_only_accepted_once(interrupted):
    h, status, config = interrupted
    service = h.service()
    cfg = config()
    assert status()["acceptedReplanDiagnostic"]["code"] == "accepted_replan_no_record"
    db_bytes = Path(h.cfg.db).read_bytes()
    def runner(script, args, cwd):
        assert script.name == 'relocate_existing_files.py'
        assert not {'--apply', '--register-unregistered-only'} & set(args)
        scope = json.loads(Path(args[args.index('--metadata-scope-file')+1]).read_bytes())
        assert set(scope['selected']) == {'p1'}
        return h.python_runner(script, args, cwd)
    service.python_runner = runner
    for stale in (replace(cfg, run_revision=0), replace(cfg, gate_id='bogus'), replace(cfg, artifact_ids=[]),
                  replace(cfg, selected_path_ids=['held'])):
        assert not service.resume(stale, action=accepted_replan.ACTION).ok
    for action in ('register_unregistered', 'prepare_relocate_metadata', 'apply_reviewed_metadata', 'apply_relocate_move_plan'):
        assert not service.resume(cfg, action=action).ok
    assert status()['revision'] == cfg.run_revision
    result = service.resume(cfg, action=accepted_replan.ACTION)
    assert result.ok and result.phase == 'plan_ready'
    assert status()['acceptedReplanDiagnostic']['code'] == 'accepted_replan_planned'
    assert not service.resume(cfg, action=accepted_replan.ACTION).ok
    assert len(h.calls) == 1 and Path(h.cfg.db).read_bytes() == db_bytes


def test_receipt_tamper_rejected_before_planner(interrupted):
    h, status, config = interrupted
    cfg = config()
    with sqlite3.connect(h.cfg.db) as con:
        con.execute("UPDATE workflow_metadata_commits SET summary_json='{}'")
    assert not h.service().resume(cfg, action=accepted_replan.ACTION).ok
    assert not h.calls and status()['revision'] == cfg.run_revision


@pytest.mark.parametrize('terminated', [False, True])
def test_failure_visible_and_resumable(interrupted, terminated):
    h, status, config = interrupted
    service, cfg = h.service(), config()
    def fail(*_):
        assert status()['acceptedReplanDiagnostic']['code'] == 'accepted_replan_started'
        if terminated:
            signal.raise_signal(signal.SIGTERM)
        raise RuntimeError('native probe failed')
    service.python_runner = fail
    if terminated:
        with pytest.raises(accepted_replan.PlannerTerminated):
            service.resume(cfg, action=accepted_replan.ACTION)
    else:
        assert not service.resume(cfg, action=accepted_replan.ACTION).ok
    assert status()['acceptedReplanDiagnostic']['code'] == 'accepted_replan_' + ('terminated' if terminated else 'failed')
    assert config().run_revision > cfg.run_revision


def test_process_group_termination_leaves_checkpoint(interrupted):
    import os
    import subprocess
    import sys
    import time

    if not sys.platform.startswith("linux"):
        pytest.skip("exercise the Linux/WSL process group")
    h, status, config = interrupted
    cfg = config()
    # Real detached group, fake waiting planner; no move or external target.
    code = """
import json, subprocess, sys
from pathlib import Path
from video_pipeline.workflows import RelocateWorkflowService, RelocateApplyConfig
def wait(script, args, cwd):
    subprocess.run([sys.executable, '-c', 'import time; time.sleep(60)'], check=True)
RelocateWorkflowService(python_runner=wait).resume(RelocateApplyConfig(**json.loads(sys.argv[1])),action='replan_accepted_metadata')
"""
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])}
    proc = subprocess.Popen([sys.executable, "-c", code, json.dumps(vars(cfg))],
                            env=env, start_new_session=True)
    try:
        deadline = time.monotonic() + 10
        while status().get("acceptedReplanDiagnostic", {}).get("code") != "accepted_replan_started":
            assert proc.poll() is None and time.monotonic() < deadline
            time.sleep(.03)
        # The checkpoint precedes the planner call. Wait for its child so this
        # signal tests interruption of a real subprocess wait.
        children = Path(f"/proc/{proc.pid}/task/{proc.pid}/children")
        while not children.read_text().strip():
            assert proc.poll() is None and time.monotonic() < deadline
            time.sleep(.03)
        os.killpg(proc.pid, signal.SIGTERM)
        assert proc.wait(timeout=2) == 143
        assert status()["acceptedReplanDiagnostic"]["code"] == "accepted_replan_terminated"
        assert config().run_revision > cfg.run_revision
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
