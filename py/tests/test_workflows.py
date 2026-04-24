import hashlib
import json

import pytest

from video_pipeline.workflows import (
    Diagnostic,
    DiagnosticSeverity,
    InvalidTransitionError,
    NextAction,
    ReviewGateStatus,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowResult,
    WorkflowStore,
    WorkflowStatus,
    validate_transition,
)
from video_pipeline.workflows.state_machine import VALID_TRANSITIONS


def test_valid_adr_transitions_are_accepted() -> None:
    for source, targets in VALID_TRANSITIONS.items():
        for target in targets:
            validate_transition(source, target)


def test_invalid_transition_reports_structured_diagnostic() -> None:
    with pytest.raises(InvalidTransitionError) as excinfo:
        validate_transition(WorkflowPhase.CREATED, WorkflowPhase.PLAN_READY)

    diagnostic = excinfo.value.diagnostic
    assert diagnostic.code == "workflow_invalid_phase_transition"
    assert diagnostic.severity == DiagnosticSeverity.ERROR
    assert diagnostic.details["fromPhase"] == "created"
    assert diagnostic.details["toPhase"] == "plan_ready"
    assert diagnostic.details["allowedNextPhases"] == [
        "blocked",
        "failed",
        "inventory_ready",
        "metadata_extracted",
    ]


def test_terminal_transition_is_rejected() -> None:
    with pytest.raises(InvalidTransitionError) as excinfo:
        validate_transition(WorkflowPhase.COMPLETE, WorkflowPhase.FAILED)

    assert excinfo.value.diagnostic.code == "workflow_terminal_phase_transition"


def test_init_run_creates_manifest_and_standard_directories(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    run = store.init_run(
        WorkflowFlow.SOURCE_ROOT,
        run_id="run_test",
        config_snapshot={"windowsOpsRoot": str(tmp_path)},
    )

    run_dir = tmp_path / "runs" / "run_test"
    assert run.run_id == "run_test"
    assert run.phase == "created"
    assert run.status == "active"
    assert (run_dir / "run.json").exists()
    for name in ("inventory", "metadata", "review", "plan", "apply", "logs"):
        assert (run_dir / name).is_dir()

    loaded = store.read_run("run_test")
    assert loaded.to_dict()["runId"] == "run_test"
    assert loaded.to_dict()["configSnapshot"] == {"windowsOpsRoot": str(tmp_path)}


def test_run_id_rejects_path_traversal(tmp_path) -> None:
    store = WorkflowStore(tmp_path)

    for run_id in ("../escape", r"..\escape", "/tmp/escape", "run/escape", r"run\escape", "run..escape"):
        with pytest.raises(ValueError):
            store.run_dir(run_id)

    assert not (tmp_path / "escape").exists()


def test_init_run_fails_for_existing_run_id_without_clobbering_manifest(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_existing")
    run_path = tmp_path / "runs" / "run_existing" / "run.json"
    before = json.loads(run_path.read_text(encoding="utf-8"))

    with pytest.raises(FileExistsError):
        store.init_run(WorkflowFlow.RELOCATE, run_id="run_existing")

    after = json.loads(run_path.read_text(encoding="utf-8"))
    assert after == before
    assert after["flow"] == "source_root"


def test_transition_run_updates_phase_status_and_manifest(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_transition")

    run = store.transition_run("run_transition", WorkflowPhase.METADATA_EXTRACTED)

    assert run.phase == "metadata_extracted"
    assert run.status == WorkflowStatus.ACTIVE
    assert store.read_run("run_transition").phase == "metadata_extracted"


def test_register_artifact_records_checksum_and_provenance(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_artifact")
    artifact_file = tmp_path / "runs" / "run_artifact" / "inventory" / "input.jsonl"
    artifact_file.write_text('{"path":"x"}\n', encoding="utf-8")

    artifact = store.register_artifact(
        "run_artifact",
        artifact_type="inventory",
        path=artifact_file,
        producer="test_runner",
        artifact_id="artifact_inventory",
        input_artifact_ids=["seed"],
        metadata={"rows": 1},
    )

    expected_hash = hashlib.sha256(artifact_file.read_bytes()).hexdigest()
    assert artifact.sha256 == expected_hash
    assert artifact.input_artifact_ids == ["seed"]
    assert artifact.metadata == {"rows": 1}

    loaded = store.read_run("run_artifact")
    assert loaded.artifact_ids == ["artifact_inventory"]
    assert loaded.artifacts["artifact_inventory"].producer == "test_runner"


def test_register_artifact_rejects_duplicate_artifact_id_without_clobbering(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_artifact_duplicate")
    first_file = tmp_path / "runs" / "run_artifact_duplicate" / "inventory" / "first.jsonl"
    second_file = tmp_path / "runs" / "run_artifact_duplicate" / "inventory" / "second.jsonl"
    first_file.write_text("first\n", encoding="utf-8")
    second_file.write_text("second\n", encoding="utf-8")
    first = store.register_artifact(
        "run_artifact_duplicate",
        artifact_type="inventory",
        path=first_file,
        producer="first_step",
        artifact_id="artifact_inventory",
    )

    with pytest.raises(FileExistsError):
        store.register_artifact(
            "run_artifact_duplicate",
            artifact_type="inventory",
            path=second_file,
            producer="second_step",
            artifact_id="artifact_inventory",
        )

    loaded = store.read_run("run_artifact_duplicate")
    assert loaded.artifact_ids == ["artifact_inventory"]
    assert loaded.artifacts["artifact_inventory"].path == str(first_file)
    assert loaded.artifacts["artifact_inventory"].sha256 == first.sha256
    assert loaded.artifacts["artifact_inventory"].producer == "first_step"


def test_register_missing_artifact_adds_diagnostic(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_missing")

    with pytest.raises(FileNotFoundError):
        store.register_artifact(
            "run_missing",
            artifact_type="inventory",
            path=tmp_path / "missing.jsonl",
            producer="test_runner",
        )

    loaded = store.read_run("run_missing")
    assert loaded.diagnostics[-1].code == "workflow_artifact_missing"


def test_review_gate_create_and_update_round_trip(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_gate")
    gate = store.create_review_gate(
        "run_gate",
        gate_type="metadata_review",
        artifact_ids=["artifact_metadata"],
        gate_id="gate_metadata",
    )

    assert gate.status == ReviewGateStatus.OPEN
    assert gate.resolved_at is None

    updated = store.update_review_gate(
        "run_gate",
        "gate_metadata",
        status=ReviewGateStatus.APPROVED,
        resolution={"approvedBy": "tester"},
    )

    assert updated.status == "approved"
    assert updated.resolved_at is not None
    assert updated.resolution == {"approvedBy": "tester"}
    assert store.read_run("run_gate").review_gate_ids == ["gate_metadata"]


def test_create_review_gate_rejects_duplicate_gate_id_without_clobbering(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_gate_duplicate")
    store.create_review_gate(
        "run_gate_duplicate",
        gate_type="metadata_review",
        artifact_ids=["artifact_first"],
        gate_id="gate_metadata",
    )
    approved = store.update_review_gate(
        "run_gate_duplicate",
        "gate_metadata",
        status=ReviewGateStatus.APPROVED,
        resolution={"approvedBy": "tester"},
    )

    with pytest.raises(FileExistsError):
        store.create_review_gate(
            "run_gate_duplicate",
            gate_type="metadata_review",
            artifact_ids=["artifact_second"],
            gate_id="gate_metadata",
        )

    loaded = store.read_run("run_gate_duplicate")
    gate = loaded.review_gates["gate_metadata"]
    assert loaded.review_gate_ids == ["gate_metadata"]
    assert gate.status == "approved"
    assert gate.artifact_ids == ["artifact_first"]
    assert gate.resolved_at == approved.resolved_at
    assert gate.resolution == {"approvedBy": "tester"}


def test_workflow_result_serializes_for_typescript_consumption(tmp_path) -> None:
    store = WorkflowStore(tmp_path)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_result")
    artifact_file = tmp_path / "runs" / "run_result" / "metadata" / "output.jsonl"
    artifact_file.write_text("{}", encoding="utf-8")
    artifact = store.register_artifact(
        "run_result",
        artifact_type="metadata",
        path=artifact_file,
        producer="metadata_step",
        artifact_id="artifact_metadata",
    )
    gate = store.create_review_gate(
        "run_result",
        gate_type="metadata_review",
        artifact_ids=["artifact_metadata"],
        gate_id="gate_metadata",
    )

    result = WorkflowResult(
        ok=False,
        run_id="run_result",
        flow=WorkflowFlow.SOURCE_ROOT,
        phase=WorkflowPhase.REVIEW_REQUIRED,
        outcome="review_required",
        artifacts=[artifact],
        gates=[gate],
        next_actions=[
            NextAction(
                action="review_metadata",
                label="Review metadata",
                tool="video_pipeline_resume",
                params={"runId": "run_result"},
                requires_human_input=True,
            )
        ],
        diagnostics=[
            Diagnostic(
                code="review_gate_open",
                severity=DiagnosticSeverity.INFO,
                message="metadata review is required",
            )
        ],
    )

    payload = result.to_dict()
    json.dumps(payload, ensure_ascii=False)
    assert payload["runId"] == "run_result"
    assert payload["phase"] == "review_required"
    assert payload["artifacts"][0]["inputArtifactIds"] == []
    assert payload["gates"][0]["requiresHumanReview"] is True
    assert payload["nextActions"][0]["requiresHumanInput"] is True
