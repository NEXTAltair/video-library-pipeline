"""Partitioned metadata previews use only accepted identities, never authorize a move."""
import json
from pathlib import Path

import pytest

from tests.test_routing_inventory import Preview
from tests.test_relocate_workflow import RelocateWorkflowHarness
from video_pipeline.platform.pathscan_common import normalize_win_for_id, path_id_for
from video_pipeline.workflows import WorkflowStore, ReviewGateStatus
from video_pipeline.workflows.relocate_metadata_scope import physical_paths, resolve_scope
from video_pipeline.workflows.store import sha256_file


def scope_file(root, selected, held, reserved=()):
    sources = {path_id_for(p): normalize_win_for_id(p) for p in (*selected, *held)}
    path = root / "scope.json"
    path.write_text(json.dumps({"sources": sources, "selected": {path_id_for(p): sources[path_id_for(p)] for p in selected},
        "reserved": sorted(set().union(*(physical_paths(p) for p in (*held, *reserved))))}))
    return str(path)


def test_only_selected_reach_move_and_metadata_queues(tmp_path, monkeypatch, capsys):
    preview = Preview(tmp_path, monkeypatch)
    held = preview.add(r"B:\Library\Show held.mp4", title=None).win_path
    preview.add(r"B:\Library\Show unrelated.mp4", title=None)
    move = preview.add(r"B:\Library\Show selected.mp4").win_path
    gap = preview.add(r"B:\Library\Show gap.mp4", title=None).win_path
    blocked = preview.add(r"B:\Library\Show existing hold.mp4", hold=True).win_path
    scope = scope_file(tmp_path, [move, gap, blocked], [held])
    summary, plan, inventory = preview.run(monkeypatch, capsys, "--metadata-scope-file", scope)
    assert {p["src"]: p["reason"] for p in plan} == {
        move: "recompute_destination", gap: "missing_metadata", blocked: "recording_hold"}
    assert summary["plannedMoves"] == summary["metadataQueuePlannedCount"] == 1
    assert inventory["selectedFiles"] == 3
    queue = [json.loads(line) for line in Path(summary["metadataQueuePath"]).read_text().splitlines()]
    assert [r["path"] for r in queue if "_meta" not in r] == [gap]
    assert not summary["apply"] and summary["movedFiles"] == 0


@pytest.mark.parametrize("failure", ["source", "destination", "db_path", "missing", "mtime", "not_dry"])
def test_scope_rejects_identity_drift_collisions_and_mutating_modes(tmp_path, monkeypatch, capsys, failure):
    preview = Preview(tmp_path, monkeypatch)
    move = preview.add(r"B:\Library\Show selected.mp4").win_path
    held = preview.add(move[:-3] + "mkv" if failure == "source" else r"B:\Library\Show held.mp4").win_path
    scope = scope_file(tmp_path, [move], [held], [r"N:\Show\Show selected.mkv"] if failure == "destination" else [])
    if failure == "db_path":
        preview.con.execute("UPDATE paths SET path=? WHERE path_id=?", (held + ".renamed", path_id_for(held)))
    if failure == "mtime":
        binding = json.loads(Path(scope).read_text())
        binding["observedMtimes"] = {path_id_for(move): ["2020-01-01T00:00:00Z"]}
        Path(scope).write_text(json.dumps(binding))
    if failure == "missing":
        preview.files.pop(0)
    with pytest.raises(SystemExit if failure == "not_dry" else ValueError):
        preview.run(monkeypatch, capsys, "--metadata-scope-file", scope,
                    *(["--register-unregistered-only"] if failure == "not_dry" else []))


def install_gate(store, run_id, decision, queue, suffix=""):
    run_dir = store.run_dir(run_id)
    for kind, aid, inputs in (("metadata_extract_output", "ex" + suffix, [queue.id]),
                              ("metadata_review_yaml", "draft" + suffix, ["ex" + suffix])):
        path = run_dir / aid
        path.write_text("{}")
        store.register_artifact(run_id, artifact_type=kind, artifact_id=aid, producer="fixture",
                                path=path, input_artifact_ids=inputs)
    store.create_review_gate(run_id, gate_type="metadata_review", gate_id="review" + suffix, artifact_ids=["draft" + suffix])
    store.update_review_gate(run_id, "review" + suffix, status=ReviewGateStatus.APPROVED,
        resolution={"action": "apply_reviewed_metadata", "metadataApply": {"metadataPartition": decision}})


def test_old_plan_stale_gate_and_unscoped_planner_are_not_authority(tmp_path):
    h = RelocateWorkflowHarness(tmp_path, run_id="partition")
    h.dry_run()
    service = h.service()
    store = WorkflowStore(Path(h.cfg.windows_ops_root))
    run_dir = store.run_dir("partition")
    previous = store.read_run("partition")
    old_plan = previous.artifacts["relocate_plan"]
    held_dst = r"E:\Held\future.mkv"
    with Path(old_plan.path).open("a") as stream:
        stream.write(json.dumps(dict(path_id="p2", src=r"B:\VideoLibrary\held.mp4",
                                     dst=held_dst, status="planned")) + "\n")
    old_plan.sha256 = sha256_file(Path(old_plan.path))
    store.write_run(previous)
    queue_path = run_dir / "metadata/q.jsonl"
    src = r"B:\VideoLibrary\Old\show.mp4"
    queue_path.write_text("\n".join(json.dumps({"path_id": pid, "path": path}) for pid, path in
                                    [("p1", src), ("p2", r"B:\VideoLibrary\held.mp4")]))
    q = store.register_artifact("partition", artifact_id="q", artifact_type="relocate_metadata_queue",
                                producer="fixture", path=queue_path)
    decision = dict(queueArtifactId=q.id, queueSha256=q.sha256, selectedPathIds=["p1"], heldPathIds=["p2"])
    install_gate(store, "partition", decision, q)
    stale = store.read_run("partition")
    stale.artifact_ids.remove("relocate_plan")
    stale.artifact_ids.append("relocate_plan")
    with pytest.raises(ValueError, match="stale planning generation"):
        resolve_scope(stale, run_dir)
    with pytest.raises(ValueError, match="predates"):
        service._validate_owned_plan_artifact(store, "partition", "relocate_plan")
    def planner(script, args, cwd):
        scope = json.loads(Path(args[args.index("--metadata-scope-file") + 1]).read_text())
        assert scope["selected"] == {"p1": normalize_win_for_id(src)}
        assert physical_paths(held_dst) <= set(scope["reserved"])
        return h.python_runner(script, args, cwd)
    service.python_runner = planner
    for _ in range(2):
        result = service._dry_run_existing("partition", h.cfg, store, h.cfg.db)
        assert result.ok
        assert result.next_actions[0].requires_human_input
        service._validate_owned_plan_artifact(store, "partition", result.next_actions[0].params["artifactId"])
    def ignoring(script, args, cwd):
        summary = json.loads(planner(script, args, cwd))
        with Path(summary["planPath"]).open("a") as f:
            f.write(json.dumps({"path_id": "new", "src": "unrelated.mp4", "status": "planned"}) + "\n")
        return json.dumps(summary)
    service.python_runner = ignoring
    with pytest.raises(ValueError, match="differs from approved"):
        service._dry_run_existing("partition", h.cfg, store, h.cfg.db)
    run = store.read_run("partition")
    run.review_gates["review"].resolution = {}
    with pytest.raises(ValueError, match="disappeared"):
        resolve_scope(run, run_dir)
    queue_path.write_text("tampered")
    with pytest.raises(ValueError, match="checksum"):
        resolve_scope(store.read_run("partition"), run_dir)


def test_repeated_partition_may_narrow_but_not_resurrect_a_hold(tmp_path):
    store = WorkflowStore(tmp_path)
    rid = store.init_run("relocate", run_id="chain").run_id
    run_dir = store.run_dir(rid)
    def gate(suffix, yes, no):
        qid = "q" + suffix
        path = run_dir / "metadata" / (qid + ".jsonl")
        path.write_text("\n".join(json.dumps({"path_id": pid, "path": rf"B:\Shows\{pid}.mp4"}) for pid in yes + no))
        queue = store.register_artifact(rid, artifact_id=qid, artifact_type="relocate_metadata_queue",
                                        producer="test", path=path)
        install_gate(store, rid, dict(queueArtifactId=qid, queueSha256=queue.sha256,
                                      selectedPathIds=yes, heldPathIds=no), queue, suffix)
    gate("1", ["a", "b"], ["c"])
    def record_plan(aid):
        history = resolve_scope(store.read_run(rid), run_dir)["history"]
        path = run_dir / "plan" / aid
        path.write_text("")
        store.register_artifact(rid, artifact_id=aid, artifact_type="relocate_plan", producer="fixture",
                                path=path, metadata={"metadataScopeHistory": history})
    record_plan("plan1")
    gate("2", ["a"], ["b"])
    scope = resolve_scope(store.read_run(rid), run_dir)
    assert set(scope["selected"]) == {"a"}
    assert physical_paths(r"B:\Shows\b.mp4") | physical_paths(r"B:\Shows\c.mp4") <= set(scope["reserved"])
    record_plan("plan2")
    gate("3", ["c"], ["a"])
    with pytest.raises(ValueError, match="exceeds retained scope"):
        resolve_scope(store.read_run(rid), run_dir)
