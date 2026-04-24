"""State transition validation for V2 workflow runs."""

from __future__ import annotations

from .models import Diagnostic, DiagnosticSeverity, WorkflowPhase


VALID_TRANSITIONS: dict[WorkflowPhase, set[WorkflowPhase]] = {
    WorkflowPhase.CREATED: {
        WorkflowPhase.INVENTORY_READY,
        WorkflowPhase.METADATA_EXTRACTED,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.INVENTORY_READY: {
        WorkflowPhase.METADATA_EXTRACTED,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.METADATA_EXTRACTED: {
        WorkflowPhase.REVIEW_REQUIRED,
        WorkflowPhase.METADATA_ACCEPTED,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.REVIEW_REQUIRED: {
        WorkflowPhase.METADATA_ACCEPTED,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.METADATA_ACCEPTED: {
        WorkflowPhase.PLAN_READY,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.PLAN_READY: {
        WorkflowPhase.APPLIED,
        WorkflowPhase.COMPLETE,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.APPLIED: {
        WorkflowPhase.COMPLETE,
        WorkflowPhase.BLOCKED,
        WorkflowPhase.FAILED,
    },
    WorkflowPhase.COMPLETE: set(),
    WorkflowPhase.BLOCKED: set(),
    WorkflowPhase.FAILED: set(),
}

TERMINAL_PHASES = {
    WorkflowPhase.COMPLETE,
    WorkflowPhase.BLOCKED,
    WorkflowPhase.FAILED,
}


class InvalidTransitionError(ValueError):
    def __init__(self, diagnostic: Diagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.message)


def transition_diagnostic(from_phase: WorkflowPhase | str, to_phase: WorkflowPhase | str) -> Diagnostic:
    source = WorkflowPhase(from_phase)
    target = WorkflowPhase(to_phase)
    if source in TERMINAL_PHASES:
        code = "workflow_terminal_phase_transition"
        message = f"cannot transition from terminal phase '{source.value}' to '{target.value}'"
    else:
        code = "workflow_invalid_phase_transition"
        message = f"invalid workflow transition from '{source.value}' to '{target.value}'"
    return Diagnostic(
        code=code,
        severity=DiagnosticSeverity.ERROR,
        message=message,
        details={
            "fromPhase": source.value,
            "toPhase": target.value,
            "allowedNextPhases": sorted(p.value for p in VALID_TRANSITIONS[source]),
        },
    )


def can_transition(from_phase: WorkflowPhase | str, to_phase: WorkflowPhase | str) -> bool:
    source = WorkflowPhase(from_phase)
    target = WorkflowPhase(to_phase)
    return target in VALID_TRANSITIONS[source]


def validate_transition(from_phase: WorkflowPhase | str, to_phase: WorkflowPhase | str) -> None:
    if not can_transition(from_phase, to_phase):
        raise InvalidTransitionError(transition_diagnostic(from_phase, to_phase))
