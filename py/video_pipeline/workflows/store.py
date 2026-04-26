"""Run-scoped manifest and artifact store for V2 workflows."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

from .models import (
    ArtifactRef,
    ArtifactStatus,
    Diagnostic,
    DiagnosticSeverity,
    ReviewGate,
    ReviewGateStatus,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowRun,
    now_iso,
    phase_status,
)
from .state_machine import validate_transition

RUN_SUBDIRS = ("inventory", "metadata", "review", "plan", "apply", "logs")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def generate_run_id() -> str:
    compact = now_iso().replace("-", "").replace(":", "").split(".")[0]
    compact = compact.replace("+0000", "Z")
    return f"run_{compact}_{uuid.uuid4().hex[:8]}"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_run_id(run_id: str) -> str:
    rid = str(run_id or "")
    if not RUN_ID_PATTERN.fullmatch(rid) or ".." in rid:
        raise ValueError(f"invalid workflow run_id: {run_id!r}")
    return rid


class WorkflowStore:
    def __init__(self, windows_ops_root: str | Path) -> None:
        self.windows_ops_root = Path(windows_ops_root)
        self.runs_root = self.windows_ops_root / "runs"

    def run_dir(self, run_id: str) -> Path:
        return self.runs_root / validate_run_id(run_id)

    def manifest_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "run.json"

    def init_run(
        self,
        flow: WorkflowFlow | str,
        *,
        run_id: str | None = None,
        config_snapshot: dict[str, Any] | None = None,
    ) -> WorkflowRun:
        rid = validate_run_id(run_id or generate_run_id())
        flow_value = WorkflowFlow(flow).value
        run_path = self.run_dir(rid)
        if run_path.exists():
            raise FileExistsError(f"workflow run already exists: {rid}")
        run_path.mkdir(parents=True)
        for subdir in RUN_SUBDIRS:
            (run_path / subdir).mkdir(exist_ok=True)
        now = now_iso()
        run = WorkflowRun(
            run_id=rid,
            flow=flow_value,
            phase=WorkflowPhase.CREATED.value,
            status=phase_status(WorkflowPhase.CREATED).value,
            created_at=now,
            updated_at=now,
            config_snapshot=dict(config_snapshot or {}),
        )
        self.write_run(run)
        return run

    def read_run(self, run_id: str) -> WorkflowRun:
        expected_run_id = validate_run_id(run_id)
        with self.manifest_path(run_id).open("r", encoding="utf-8") as f:
            run = WorkflowRun.from_dict(json.load(f))
        if run.run_id != expected_run_id:
            raise ValueError(
                f"workflow manifest runId mismatch: expected {expected_run_id!r}, got {run.run_id!r}"
            )
        return run

    def write_run(self, run: WorkflowRun) -> None:
        path = self.manifest_path(run.run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(run.to_dict(), f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")

    def transition_run(self, run_id: str, to_phase: WorkflowPhase | str) -> WorkflowRun:
        run = self.read_run(run_id)
        validate_transition(run.phase, to_phase)
        now = now_iso()
        run.phase = WorkflowPhase(to_phase).value
        run.status = phase_status(to_phase).value
        run.updated_at = now
        self.write_run(run)
        return run

    def register_artifact(
        self,
        run_id: str,
        *,
        artifact_type: str,
        path: str | Path,
        producer: str,
        artifact_id: str | None = None,
        status: ArtifactStatus | str = ArtifactStatus.AVAILABLE,
        input_artifact_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRef:
        run = self.read_run(run_id)
        artifact_path = Path(path)
        if not artifact_path.exists():
            diagnostic = Diagnostic(
                code="workflow_artifact_missing",
                severity=DiagnosticSeverity.ERROR,
                message=f"artifact file does not exist: {artifact_path}",
                details={"path": str(artifact_path)},
            )
            run.diagnostics.append(diagnostic)
            run.updated_at = now_iso()
            self.write_run(run)
            raise FileNotFoundError(diagnostic.message)

        aid = artifact_id or f"{artifact_type}_{uuid.uuid4().hex[:12]}"
        if aid in run.artifacts:
            raise FileExistsError(f"workflow artifact already exists: {aid}")
        artifact = ArtifactRef(
            id=aid,
            type=artifact_type,
            path=str(artifact_path),
            sha256=sha256_file(artifact_path),
            created_at=now_iso(),
            producer=producer,
            status=status,
            input_artifact_ids=list(input_artifact_ids or []),
            metadata=dict(metadata or {}),
        )
        run.artifacts[aid] = artifact
        if aid not in run.artifact_ids:
            run.artifact_ids.append(aid)
        run.updated_at = now_iso()
        self.write_run(run)
        return artifact

    def create_review_gate(
        self,
        run_id: str,
        *,
        gate_type: str,
        artifact_ids: list[str],
        requires_human_review: bool = True,
        gate_id: str | None = None,
    ) -> ReviewGate:
        run = self.read_run(run_id)
        gid = gate_id or f"{gate_type}_{uuid.uuid4().hex[:12]}"
        if gid in run.review_gates:
            raise FileExistsError(f"workflow review gate already exists: {gid}")
        gate = ReviewGate(
            id=gid,
            type=gate_type,
            status=ReviewGateStatus.OPEN,
            artifact_ids=list(artifact_ids),
            requires_human_review=requires_human_review,
            opened_at=now_iso(),
        )
        run.review_gates[gid] = gate
        if gid not in run.review_gate_ids:
            run.review_gate_ids.append(gid)
        run.updated_at = now_iso()
        self.write_run(run)
        return gate

    def update_review_gate(
        self,
        run_id: str,
        gate_id: str,
        *,
        status: ReviewGateStatus | str,
        resolution: dict[str, Any] | None = None,
    ) -> ReviewGate:
        run = self.read_run(run_id)
        gate = run.review_gates[gate_id]
        target_status = ReviewGateStatus(status)
        gate.status = target_status.value
        gate.resolution = dict(resolution or {})
        gate.resolved_at = None if target_status == ReviewGateStatus.OPEN else now_iso()
        run.review_gates[gate_id] = gate
        run.updated_at = now_iso()
        self.write_run(run)
        return gate

    def update_review_gate_artifacts(
        self,
        run_id: str,
        gate_id: str,
        *,
        artifact_ids: list[str],
    ) -> ReviewGate:
        run = self.read_run(run_id)
        gate = run.review_gates[gate_id]
        gate.artifact_ids = list(artifact_ids)
        run.review_gates[gate_id] = gate
        run.updated_at = now_iso()
        self.write_run(run)
        return gate
