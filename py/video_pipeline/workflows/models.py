"""Typed models for the V2 run-based workflow substrate."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorkflowFlow(str, Enum):
    SOURCE_ROOT = "source_root"
    RELOCATE = "relocate"


class WorkflowPhase(str, Enum):
    CREATED = "created"
    INVENTORY_READY = "inventory_ready"
    METADATA_EXTRACTED = "metadata_extracted"
    REVIEW_REQUIRED = "review_required"
    METADATA_ACCEPTED = "metadata_accepted"
    PLAN_READY = "plan_ready"
    APPLIED = "applied"
    COMPLETE = "complete"
    BLOCKED = "blocked"
    FAILED = "failed"


class WorkflowStatus(str, Enum):
    ACTIVE = "active"
    COMPLETE = "complete"
    BLOCKED = "blocked"
    FAILED = "failed"


class ArtifactStatus(str, Enum):
    AVAILABLE = "available"
    SUPERSEDED = "superseded"
    DELETED = "deleted"
    MISSING = "missing"


class ReviewGateStatus(str, Enum):
    OPEN = "open"
    APPROVED = "approved"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"
    CANCELLED = "cancelled"


class DiagnosticSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


def enum_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


def phase_status(phase: WorkflowPhase | str) -> WorkflowStatus:
    phase_value = WorkflowPhase(phase)
    if phase_value == WorkflowPhase.COMPLETE:
        return WorkflowStatus.COMPLETE
    if phase_value == WorkflowPhase.BLOCKED:
        return WorkflowStatus.BLOCKED
    if phase_value == WorkflowPhase.FAILED:
        return WorkflowStatus.FAILED
    return WorkflowStatus.ACTIVE


@dataclass
class Diagnostic:
    code: str
    severity: DiagnosticSeverity | str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": enum_value(self.severity),
            "message": self.message,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Diagnostic":
        return cls(
            code=str(data["code"]),
            severity=str(data["severity"]),
            message=str(data["message"]),
            details=dict(data.get("details") or {}),
        )


@dataclass
class ArtifactRef:
    id: str
    type: str
    path: str
    sha256: str
    created_at: str
    producer: str
    status: ArtifactStatus | str = ArtifactStatus.AVAILABLE
    input_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "path": self.path,
            "sha256": self.sha256,
            "createdAt": self.created_at,
            "producer": self.producer,
            "status": enum_value(self.status),
            "inputArtifactIds": list(self.input_artifact_ids),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ArtifactRef":
        return cls(
            id=str(data["id"]),
            type=str(data["type"]),
            path=str(data["path"]),
            sha256=str(data["sha256"]),
            created_at=str(data["createdAt"]),
            producer=str(data["producer"]),
            status=str(data.get("status") or ArtifactStatus.AVAILABLE.value),
            input_artifact_ids=[str(v) for v in data.get("inputArtifactIds") or []],
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass
class ReviewGate:
    id: str
    type: str
    status: ReviewGateStatus | str
    artifact_ids: list[str]
    requires_human_review: bool
    opened_at: str
    resolved_at: str | None = None
    resolution: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "status": enum_value(self.status),
            "artifactIds": list(self.artifact_ids),
            "requiresHumanReview": self.requires_human_review,
            "openedAt": self.opened_at,
            "resolvedAt": self.resolved_at,
            "resolution": self.resolution,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReviewGate":
        return cls(
            id=str(data["id"]),
            type=str(data["type"]),
            status=str(data["status"]),
            artifact_ids=[str(v) for v in data.get("artifactIds") or []],
            requires_human_review=bool(data["requiresHumanReview"]),
            opened_at=str(data["openedAt"]),
            resolved_at=data.get("resolvedAt"),
            resolution=dict(data.get("resolution") or {}),
        )


@dataclass
class NextAction:
    action: str
    label: str
    tool: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    requires_human_input: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "label": self.label,
            "tool": self.tool,
            "params": self.params,
            "requiresHumanInput": self.requires_human_input,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NextAction":
        return cls(
            action=str(data["action"]),
            label=str(data["label"]),
            tool=data.get("tool"),
            params=dict(data.get("params") or {}),
            requires_human_input=bool(data.get("requiresHumanInput", False)),
        )


@dataclass
class WorkflowRun:
    run_id: str
    flow: WorkflowFlow | str
    phase: WorkflowPhase | str
    status: WorkflowStatus | str
    created_at: str
    updated_at: str
    config_snapshot: dict[str, Any] = field(default_factory=dict)
    artifact_ids: list[str] = field(default_factory=list)
    review_gate_ids: list[str] = field(default_factory=list)
    diagnostics: list[Diagnostic] = field(default_factory=list)
    artifacts: dict[str, ArtifactRef] = field(default_factory=dict)
    review_gates: dict[str, ReviewGate] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "runId": self.run_id,
            "flow": enum_value(self.flow),
            "phase": enum_value(self.phase),
            "status": enum_value(self.status),
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "configSnapshot": self.config_snapshot,
            "artifactIds": list(self.artifact_ids),
            "reviewGateIds": list(self.review_gate_ids),
            "diagnostics": [d.to_dict() for d in self.diagnostics],
            "artifacts": {k: v.to_dict() for k, v in self.artifacts.items()},
            "reviewGates": {k: v.to_dict() for k, v in self.review_gates.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowRun":
        diagnostics = [Diagnostic.from_dict(v) for v in data.get("diagnostics") or []]
        artifacts = {
            str(k): ArtifactRef.from_dict(v)
            for k, v in dict(data.get("artifacts") or {}).items()
        }
        review_gates = {
            str(k): ReviewGate.from_dict(v)
            for k, v in dict(data.get("reviewGates") or {}).items()
        }
        return cls(
            run_id=str(data["runId"]),
            flow=str(data["flow"]),
            phase=str(data["phase"]),
            status=str(data["status"]),
            created_at=str(data["createdAt"]),
            updated_at=str(data["updatedAt"]),
            config_snapshot=dict(data.get("configSnapshot") or {}),
            artifact_ids=[str(v) for v in data.get("artifactIds") or []],
            review_gate_ids=[str(v) for v in data.get("reviewGateIds") or []],
            diagnostics=diagnostics,
            artifacts=artifacts,
            review_gates=review_gates,
        )


@dataclass
class WorkflowResult:
    ok: bool
    run_id: str
    flow: WorkflowFlow | str
    phase: WorkflowPhase | str
    outcome: str
    artifacts: list[ArtifactRef] = field(default_factory=list)
    gates: list[ReviewGate] = field(default_factory=list)
    next_actions: list[NextAction] = field(default_factory=list)
    diagnostics: list[Diagnostic] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "runId": self.run_id,
            "flow": enum_value(self.flow),
            "phase": enum_value(self.phase),
            "outcome": self.outcome,
            "artifacts": [a.to_dict() for a in self.artifacts],
            "gates": [g.to_dict() for g in self.gates],
            "nextActions": [a.to_dict() for a in self.next_actions],
            "diagnostics": [d.to_dict() for d in self.diagnostics],
        }
