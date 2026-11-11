"""Chunked, resumable relocate apply (issue #138).

One public call used to move every planned file synchronously. A deadline
killed the process, left the DB unrecorded and the run unaware of the log.
Here a call moves bounded chunks within a time budget. Each chunk has its own
receipt log, is recorded in the DB right after it finishes, and the run keeps
durable state under ``apply/``. A call that dies mid-chunk leaves ``inFlight``
set; ``reconcile_relocate_apply`` settles it before moving anything else.

Out of scope here (see ADR-0013): durable job service, native exclusion, and
temp-name copies in apply_move_plan.ps1 (shared with dedup).
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from .models import NextAction, WorkflowPhase

DEFAULT_CHUNK_ROWS = 10
# Must stay well below the public tool deadline (30 min in src/platform/runtime.ts).
DEFAULT_BUDGET_SECONDS = 20 * 60

STATE_NAME = "relocate_apply_state.json"
RESUME_TOOL = "video_pipeline_resume"


def apply_dir(run_dir: Path) -> Path:
    return run_dir / "apply"


def state_path(run_dir: Path) -> Path:
    return apply_dir(run_dir) / STATE_NAME


def load_state(run_dir: Path) -> dict[str, Any] | None:
    path = state_path(run_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(run_dir: Path, state: dict[str, Any], now: str) -> None:
    state["updatedAt"] = now
    path = state_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
        tmp = f.name
    os.replace(tmp, path)


def read_plan_rows(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict) and "_meta" not in row:
            rows.append(row)
    return rows


def read_receipts(path: Path) -> list[dict[str, Any]]:
    """Move receipts of one log. A crash may truncate the last line; ignore it."""
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        try:
            rec = json.loads(line)
        except (ValueError, TypeError):
            continue
        if isinstance(rec, dict) and rec.get("op") == "move" and not rec.get("dry_run"):
            out.append(rec)
    return out


def chunk_logs(state: dict[str, Any]) -> list[Path]:
    return [Path(c["localLog"]) for c in state["chunks"]]


def receipts_by_src(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for log in chunk_logs(state):
        for rec in read_receipts(log):
            found[str(rec.get("src"))] = rec
    return found


def pending_rows(state: dict[str, Any], plan_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    done = receipts_by_src(state)
    return [row for row in plan_rows if str(row.get("src")) not in done]


def candidate_row(state: dict[str, Any], chunk_index: int) -> dict[str, Any] | None:
    """First row of an interrupted chunk without a receipt: the only one that can be mid-move."""
    chunk = next((c for c in state["chunks"] if c["index"] == chunk_index), None)
    if chunk is None:
        return None
    done = receipts_by_src(state)
    for row in read_plan_rows(Path(chunk["localPlan"])):
        if str(row.get("src")) not in done:
            return row
    return None


def progress(run_dir: Path) -> dict[str, Any] | None:
    """Counts for status. Cheap: reads only the state file, the plan and receipt logs."""
    state = load_state(run_dir)
    if state is None:
        return None
    plan_rows = read_plan_rows(Path(state["internalPlan"]))
    receipts = receipts_by_src(state)
    failed = sum(1 for rec in receipts.values() if not rec.get("ok"))
    in_flight = state.get("inFlight")
    settlement = state.get("settlement")
    return {
        "total": len(plan_rows),
        "done": len(receipts),
        "succeeded": len(receipts) - failed,
        "failed": failed,
        "pending": max(0, len(plan_rows) - len(receipts)),
        "chunks": len(state["chunks"]),
        "startedAt": state.get("startedAt"),
        "updatedAt": state.get("updatedAt"),
        "inFlight": in_flight,
        "settlement": settlement,
        "state": "interrupted" if in_flight else ("settlement_required" if settlement else "ready"),
        "logs": [c["log"] for c in state["chunks"]],
    }


def applying_next_actions(run: Any, run_dir: Path) -> list[NextAction]:
    """Single definition used by results and by status reconstruction."""
    if run.phase != WorkflowPhase.APPLYING.value:
        return []
    info = progress(run_dir)

    def action(name: str, label: str, human: bool) -> NextAction:
        return NextAction(
            action=name, label=label, tool=RESUME_TOOL,
            params={"runId": run.run_id, "resumeAction": name}, requires_human_input=human,
        )

    if info is None or info["state"] == "interrupted":
        return [action("reconcile_relocate_apply", "Reconcile interrupted relocate apply", False)]
    if info["state"] == "settlement_required":
        return [action("settle_interrupted_move", "Settle the interrupted move (verified delete)", True)]
    return [action("continue_relocate_apply", "Continue relocate apply", False)]


def monotonic() -> float:
    return time.monotonic()


def is_partial_apply(stats: dict[str, Any]) -> bool:
    """Some moves succeeded and some failed: the run's work happened and must be kept."""
    return int(stats.get("failed") or 0) > 0 and int(stats.get("succeeded") or 0) > 0


def partial_apply_diagnostic(flow: str, stats: dict[str, Any]):
    from .models import Diagnostic, DiagnosticSeverity

    return Diagnostic(
        code=f"{flow}_apply_partial",
        severity=DiagnosticSeverity.WARNING,
        message=f"{stats.get('succeeded')} moves succeeded and {stats.get('failed')} failed; "
                "succeeded moves are recorded, failed files stayed where they were",
        details={"moveApplyStats": stats, "errorBreakdown": stats.get("errorBreakdown", {})},
    )


def replan_failed_action(flow: str) -> NextAction:
    return NextAction(
        action="replan_failed_moves",
        label="Preview the same flow again; files that failed are planned again from their current state",
        tool="video_pipeline_start",
        params={"flow": flow},
        requires_human_input=False,
    )
