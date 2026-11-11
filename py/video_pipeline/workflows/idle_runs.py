"""Find and bulk-supersede active runs that nobody has touched for a long time (issue #140).

Active runs left behind by abandoned reviews keep their gates open and crowd
``status``. ``supersede_run`` needs one successor per call, so cleaning hundreds
of them by hand is unrealistic. This module selects the runs that are safe to
supersede and reports the ones that are not, with the reason.

A run is never selected when superseding it would hide work in progress:
an apply that started (any chunked state or an interrupted pre-chunking log)
must be reconciled first, because its moves exist on disk and the run is the
only thing that knows about them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import chunked_apply
from .models import WorkflowPhase, WorkflowRun, WorkflowStatus
from .store import WorkflowStore

MIN_IDLE_DAYS = 1


def _parse(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def protection_reason(run: WorkflowRun, run_dir: Path) -> str | None:
    """Why this active run must not be superseded, or None when it is safe."""
    if run.phase == WorkflowPhase.APPLYING.value:
        return "apply_in_progress"
    if chunked_apply.load_state(run_dir) is not None:
        return "apply_state_present"
    if chunked_apply.legacy_interrupted_logs(run, run_dir):
        return "interrupted_apply_needs_reconcile"
    return None


def idle_active_runs(
    store: WorkflowStore,
    idle_days: int,
    *,
    now: datetime | None = None,
    flow: str | None = None,
) -> list[dict[str, Any]]:
    """Active runs whose last update is older than ``idle_days``, oldest first.

    The manifest mtime is a cheap pre-filter: a file written recently cannot belong
    to an idle run, so only old manifests are parsed.
    """
    if idle_days < MIN_IDLE_DAYS:
        raise ValueError(f"idleDays must be at least {MIN_IDLE_DAYS}")
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=idle_days)
    runs_root = store.runs_root
    if not runs_root.exists():
        return []
    found = []
    for manifest in runs_root.glob("*/run.json"):
        if datetime.fromtimestamp(manifest.stat().st_mtime, timezone.utc) >= cutoff:
            continue
        run = store.read_run(manifest.parent.name)
        updated = _parse(run.updated_at)
        if run.status != WorkflowStatus.ACTIVE.value or updated is None or updated >= cutoff:
            continue
        if flow is not None and run.flow != flow:
            continue
        found.append({
            "runId": run.run_id,
            "flow": run.flow,
            "phase": run.phase,
            "createdAt": run.created_at,
            "updatedAt": run.updated_at,
            "protected": protection_reason(run, manifest.parent),
        })
    return sorted(found, key=lambda item: (item["updatedAt"], item["runId"]))


def supersede_idle_runs(
    store: WorkflowStore,
    successor_run_id: str,
    *,
    idle_days: int,
    reason: str,
) -> dict[str, Any]:
    """Supersede every safe idle active run of the successor's flow.

    State only: no file moves. Runs that are protected, or not older than the
    successor, are left untouched and reported under ``skipped``.
    """
    reason_value = str(reason or "").strip()
    if not reason_value:
        raise ValueError("supersede reason is required")
    successor = store.read_run(successor_run_id)
    if successor.status != WorkflowStatus.ACTIVE.value:
        raise ValueError(f"the successor must be an active run: {successor.run_id} is {successor.status}")

    superseded: list[str] = []
    skipped: list[dict[str, str]] = []
    for item in idle_active_runs(store, idle_days, flow=successor.flow):
        run_id = item["runId"]
        if run_id == successor.run_id:
            continue
        if item["protected"]:
            skipped.append({"runId": run_id, "reason": item["protected"]})
        elif item["createdAt"] >= successor.created_at:
            skipped.append({"runId": run_id, "reason": "not_older_than_successor"})
        else:
            store.supersede_run(run_id, superseded_by_run_id=successor.run_id, reason=reason_value)
            superseded.append(run_id)
    return {"superseded": superseded, "skipped": skipped, "idleDays": idle_days}
