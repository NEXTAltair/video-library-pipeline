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
    assert payload["runs"][0]["nextActions"] == []
    assert "artifacts" not in payload["runs"][0]
    assert payload["openGates"][0]["runId"] == "run_source"
    assert payload["openGates"][0]["id"] == "metadata_review"


def test_workflow_cli_status_reconstructs_source_root_review_action(tmp_path):
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_source_review")
    review_path = ops_root / "runs" / "run_source_review" / "review" / "metadata_review_0001.yaml"
    review_path.write_text("hints: []\n", encoding="utf-8")
    store.register_artifact(
        "run_source_review",
        artifact_type="metadata_review_yaml",
        path=review_path,
        producer="test",
        artifact_id="metadata_review_yaml_0001",
    )
    store.create_review_gate(
        "run_source_review",
        gate_type="metadata_review",
        artifact_ids=["metadata_review_yaml_0001"],
        gate_id="metadata_review",
    )
    store.transition_run("run_source_review", WorkflowPhase.INVENTORY_READY)
    store.transition_run("run_source_review", WorkflowPhase.METADATA_EXTRACTED)
    store.transition_run("run_source_review", WorkflowPhase.REVIEW_REQUIRED)

    payload = _cmd_status(
        argparse.Namespace(
            windows_ops_root=str(ops_root),
            run_id="run_source_review",
            limit=10,
            include_artifacts=True,
        )
    )

    assert payload["ok"] is True
    assert payload["run"]["nextActions"] == [
        {
            "action": "review_metadata",
            "label": "Review extracted metadata YAML",
            "tool": "video_pipeline_resume",
            "params": {
                "runId": "run_source_review",
                "gateId": "metadata_review",
                "artifactIds": ["metadata_review_yaml_0001"],
                "reviewYamlPaths": [str(review_path)],
                "resumeAction": "apply_reviewed_metadata",
            },
            "requiresHumanInput": True,
        }
    ]


def test_workflow_cli_status_reconstructs_latest_relocate_plan_action(tmp_path):
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_relocate_plan")
    old_plan_path = ops_root / "runs" / "run_relocate_plan" / "plan" / "relocate_plan.jsonl"
    old_plan_path.write_text("{}\n", encoding="utf-8")
    store.register_artifact(
        "run_relocate_plan",
        artifact_type="relocate_plan",
        path=old_plan_path,
        producer="test",
        artifact_id="relocate_plan",
    )
    latest_plan_path = ops_root / "runs" / "run_relocate_plan" / "plan" / "relocate_plan_0002.jsonl"
    latest_plan_path.write_text('{"ok": true}\n', encoding="utf-8")
    store.register_artifact(
        "run_relocate_plan",
        artifact_type="relocate_plan",
        path=latest_plan_path,
        producer="test",
        artifact_id="relocate_plan_0002",
    )
    store.transition_run("run_relocate_plan", WorkflowPhase.PLAN_READY)

    payload = _cmd_status(
        argparse.Namespace(
            windows_ops_root=str(ops_root),
            run_id="run_relocate_plan",
            limit=10,
            include_artifacts=False,
        )
    )

    assert payload["ok"] is True
    assert payload["run"]["nextActions"][0]["params"] == {
        "runId": "run_relocate_plan",
        "artifactId": "relocate_plan_0002",
        "resumeAction": "apply_relocate_move_plan",
    }


def test_workflow_cli_status_prefers_open_relocate_review_gate_over_stale_queue(tmp_path):
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_relocate_review")
    queue_path = ops_root / "runs" / "run_relocate_review" / "metadata" / "relocate_metadata_queue.jsonl"
    queue_path.write_text("{}\n", encoding="utf-8")
    store.register_artifact(
        "run_relocate_review",
        artifact_type="relocate_metadata_queue",
        path=queue_path,
        producer="test",
        artifact_id="relocate_metadata_queue",
    )
    diagnostics_path = ops_root / "runs" / "run_relocate_review" / "logs" / "relocate_summary.json"
    diagnostics_path.write_text("{}\n", encoding="utf-8")
    store.register_artifact(
        "run_relocate_review",
        artifact_type="relocate_diagnostics",
        path=diagnostics_path,
        producer="test",
        artifact_id="relocate_diagnostics",
    )
    store.create_review_gate(
        "run_relocate_review",
        gate_type="relocate_metadata_review",
        artifact_ids=["relocate_diagnostics"],
        gate_id="relocate_metadata_review",
    )
    store.transition_run("run_relocate_review", WorkflowPhase.METADATA_EXTRACTED)
    store.transition_run("run_relocate_review", WorkflowPhase.REVIEW_REQUIRED)

    payload = _cmd_status(
        argparse.Namespace(
            windows_ops_root=str(ops_root),
            run_id="run_relocate_review",
            limit=10,
            include_artifacts=False,
        )
    )

    assert payload["ok"] is True
    assert payload["run"]["nextActions"][0] == {
        "action": "review_relocate_metadata",
        "label": "Review blocked relocate metadata",
        "tool": "video_pipeline_resume",
        "params": {
            "runId": "run_relocate_review",
            "gateId": "relocate_metadata_review",
            "artifactIds": ["relocate_diagnostics"],
        },
        "requiresHumanInput": True,
    }


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
