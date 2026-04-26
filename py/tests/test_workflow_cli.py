import argparse

from video_pipeline.workflows import WorkflowFlow, WorkflowPhase, WorkflowStore
from workflow_cli import _cmd_inspect_artifact, _cmd_status


def test_workflow_cli_status_lists_recent_runs(tmp_path):
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_source")
    store.transition_run("run_source", WorkflowPhase.INVENTORY_READY)
    store.create_review_gate(
        "run_source",
        gate_type="metadata_review",
        artifact_ids=["metadata_review_yaml_0001"],
        gate_id="metadata_review",
    )

    payload = _cmd_status(
        argparse.Namespace(
            windows_ops_root=str(ops_root),
            run_id="",
            limit=10,
            include_artifacts=False,
        )
    )

    assert payload["ok"] is True
    assert payload["runs"][0]["runId"] == "run_source"
    assert payload["runs"][0]["phase"] == "inventory_ready"
    assert "artifacts" not in payload["runs"][0]
    assert payload["openGates"][0]["runId"] == "run_source"
    assert payload["openGates"][0]["id"] == "metadata_review"


def test_workflow_cli_inspect_artifact_returns_related_gates_and_preview(tmp_path):
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_relocate")
    artifact_path = ops_root / "runs" / "run_relocate" / "plan" / "relocate_plan.jsonl"
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")
    store.register_artifact(
        "run_relocate",
        artifact_type="relocate_plan",
        path=artifact_path,
        producer="test",
        artifact_id="relocate_plan",
    )
    store.create_review_gate(
        "run_relocate",
        gate_type="relocate_plan_review",
        artifact_ids=["relocate_plan"],
        gate_id="relocate_plan_review",
    )

    payload = _cmd_inspect_artifact(
        argparse.Namespace(
            windows_ops_root=str(ops_root),
            run_id="run_relocate",
            artifact_id="relocate_plan",
            include_content_preview=True,
            preview_bytes=20,
        )
    )

    assert payload["ok"] is True
    assert payload["artifact"]["id"] == "relocate_plan"
    assert payload["exists"] is True
    assert payload["relatedGates"][0]["id"] == "relocate_plan_review"
    assert payload["contentPreview"] == '{"ok": true}\n'
