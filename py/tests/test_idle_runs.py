"""#140: bulk cleanup of long-idle active runs and bounded status output."""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from video_pipeline.workflows import WorkflowFlow, WorkflowStore
from video_pipeline.workflows import idle_runs
from workflow_cli import _cmd_resume, _cmd_status, build_parser

DAY = 86400


def _make(ops_root, run_id, *, age_days, flow=WorkflowFlow.RELOCATE, gate=True, phase=None):
    """A run whose manifest looks untouched for ``age_days`` (both updatedAt and file mtime)."""
    store = WorkflowStore(ops_root)
    store.init_run(flow, run_id=run_id, config_snapshot={})
    if gate:
        store.create_review_gate(run_id, gate_type="metadata_review", artifact_ids=[], gate_id="g")
    path = store.manifest_path(run_id)
    data = json.loads(path.read_text(encoding="utf-8"))
    then = datetime.now(timezone.utc) - timedelta(days=age_days)
    data["createdAt"] = data["updatedAt"] = then.isoformat()
    if phase:
        data["phase"] = phase
    path.write_text(json.dumps(data), encoding="utf-8")
    stamp = time.time() - age_days * DAY
    os.utime(path, (stamp, stamp))
    return store


def _resume(ops_root, run_id, **extra):
    argv = ["resume", "--windows-ops-root", str(ops_root), "--run-id", run_id, "--action", "supersede_idle_runs"]
    for key, value in extra.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    return _cmd_resume(build_parser().parse_args(argv))


def _status(ops_root, **kw):
    base = dict(windows_ops_root=str(ops_root), run_id="", limit=10, include_artifacts=False, hints_path="", idle_days=0)
    base.update(kw)
    return _cmd_status(argparse.Namespace(**base))


def test_status_open_gates_are_capped_and_counted(tmp_path):
    ops = tmp_path / "ops"
    for n in range(40):
        _make(ops, f"run_many_{n:02d}", age_days=0)

    payload = _status(ops, limit=5)

    assert len(payload["openGates"]) == 5
    assert payload["openGatesTotal"] == 40
    assert len(json.dumps(payload)) < 20_000


def test_status_idle_listing_is_opt_in_and_read_only(tmp_path):
    ops = tmp_path / "ops"
    _make(ops, "run_old", age_days=40)
    _make(ops, "run_recent", age_days=2)
    before = WorkflowStore(ops).manifest_path("run_old").read_bytes()

    assert "idleActiveRuns" not in _status(ops)
    listing = _status(ops, idle_days=30)["idleActiveRuns"]

    assert [r["runId"] for r in listing["runs"]] == ["run_old"]
    assert listing["total"] == 1 and listing["supersedable"] == 1
    assert WorkflowStore(ops).manifest_path("run_old").read_bytes() == before


def test_supersede_idle_runs_supersedes_only_safe_runs_of_the_successor_flow(tmp_path):
    ops = tmp_path / "ops"
    _make(ops, "run_idle_a", age_days=90)
    _make(ops, "run_idle_b", age_days=45)
    _make(ops, "run_recent", age_days=3)
    _make(ops, "run_other_flow", age_days=60, flow=WorkflowFlow.SOURCE_ROOT)
    _make(ops, "run_applying", age_days=60, phase="applying")
    _make(ops, "run_apply_state", age_days=60)
    state = ops / "runs" / "run_apply_state" / "apply" / "relocate_apply_state.json"
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text("{}", encoding="utf-8")
    store = _make(ops, "run_keep", age_days=0)
    keep_before = store.manifest_path("run_keep").read_bytes()

    payload = _resume(ops, "run_keep", idle_days=30, reason="abandoned reviews")

    details = payload["diagnostics"][0]["details"]
    assert payload["ok"] and payload["outcome"] == "idle_runs_superseded"
    assert sorted(details["superseded"]) == ["run_idle_a", "run_idle_b"]
    assert {s["runId"]: s["reason"] for s in details["skipped"]} == {
        "run_applying": "apply_in_progress",
        "run_apply_state": "apply_state_present",
    }
    old = store.read_run("run_idle_a")
    assert old.status == "complete" and old.resolution["supersededByRunId"] == "run_keep"
    assert old.review_gates["g"].status == "superseded"
    for untouched in ("run_recent", "run_other_flow", "run_applying", "run_apply_state"):
        assert store.read_run(untouched).status in {"active", "created"}, untouched
    assert store.read_run("run_other_flow").review_gates["g"].status == "open"
    assert store.manifest_path("run_keep").read_bytes() == keep_before


def test_supersede_idle_runs_skips_runs_not_older_than_the_successor(tmp_path):
    ops = tmp_path / "ops"
    _make(ops, "run_idle_newer", age_days=40)
    store = _make(ops, "run_keep", age_days=50)  # idle itself, but the chosen successor

    details = _resume(ops, "run_keep", idle_days=30, reason="r")["diagnostics"][0]["details"]

    assert details["superseded"] == []
    assert details["skipped"] == [{"runId": "run_idle_newer", "reason": "not_older_than_successor"}]
    assert store.read_run("run_keep").status != "complete"


def test_supersede_idle_runs_requires_reason_idle_days_and_an_active_successor(tmp_path):
    ops = tmp_path / "ops"
    _make(ops, "run_old", age_days=60)
    store = _make(ops, "run_keep", age_days=0)

    with pytest.raises(ValueError, match="reason is required"):
        _resume(ops, "run_keep", idle_days=30, reason="  ")
    with pytest.raises(ValueError, match="idleDays"):
        _resume(ops, "run_keep", idle_days=0, reason="r")
    run = store.read_run("run_keep")
    run.phase, run.status = "failed", "failed"
    store.write_run(run)
    with pytest.raises(ValueError, match="successor must be an active run"):
        _resume(ops, "run_keep", idle_days=30, reason="r")
    assert store.read_run("run_old").status != "complete"


def test_recently_written_manifest_is_never_listed_even_if_updated_at_is_old(tmp_path):
    # mtime is the cheap pre-filter: a file somebody just wrote is not idle.
    ops = tmp_path / "ops"
    store = _make(ops, "run_touched", age_days=60)
    os.utime(store.manifest_path("run_touched"), None)

    assert idle_runs.idle_active_runs(store, 30) == []


def test_idle_listing_stays_fast_with_hundreds_of_runs(tmp_path):
    ops = tmp_path / "ops"
    for n in range(300):
        _make(ops, f"run_bulk_{n:03d}", age_days=100 + n % 7)
    _make(ops, "run_keep", age_days=0)

    started = time.monotonic()
    payload = _status(ops, limit=10, idle_days=30)
    elapsed = time.monotonic() - started

    assert payload["idleActiveRuns"]["total"] == 300
    assert len(payload["idleActiveRuns"]["runs"]) == 10
    assert len(payload["openGates"]) <= 10
    assert elapsed < 5
    result = _resume(ops, "run_keep", idle_days=30, reason="bulk cleanup")
    assert len(result["diagnostics"][0]["details"]["superseded"]) == 300
    assert _status(ops)["openGatesTotal"] == 1
