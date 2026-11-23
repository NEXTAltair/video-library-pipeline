"""Non-applying recovery of a committed metadata partition before its first preview."""
from __future__ import annotations

from contextlib import closing, contextmanager
import json
import signal
import threading

from video_pipeline.application.reviewed_metadata import PLACED_REVIEW_GATE_TYPE, commit_operation_id
from video_pipeline.platform.sqlite_snapshot import readonly_connection
from .models import NextAction

ACTION = "replan_accepted_metadata"


def pending_action(run):
    if run.flow != "relocate" or run.phase != "review_required" or run.status != "active":
        return None
    gates = [run.review_gates[gid] for gid in run.review_gate_ids]
    if any(g.type != PLACED_REVIEW_GATE_TYPE and g.status in {"open", "rejected"} and g.requires_human_review
           for g in gates):
        return None
    gate = next((g for g in reversed(gates) if g.type == "metadata_review"), None)
    if not gate or gate.status != "approved":
        return None
    res = gate.resolution or {}
    part = (res.get("metadataApply") or {}).get("metadataPartition") or {}
    queue = part.get("queueArtifactId")
    if res.get("action") != "apply_reviewed_metadata" or queue not in run.artifact_ids:
        return None
    # Once a new physical preview exists its own review contract takes over.
    if any(run.artifacts[aid].type in {"relocate_plan", "relocate_metadata_queue"}
           for aid in run.artifact_ids[run.artifact_ids.index(queue) + 1:]):
        return None
    return NextAction(action=ACTION, label="Regenerate the accepted subset preview (no moves)",
                      tool="video_pipeline_resume", requires_human_input=False,
                      params={"runId": run.run_id, "resumeAction": ACTION, "gateId": gate.id,
                              "artifactIds": [queue], "runRevision": run.revision})


def verify_commit(run_dir, db, gate):
    # Partition preflight permits exactly one blocking metadata gate. Look up
    # the record by the original accepting transaction's run+gate identity,
    # never just by an untrusted digest supplied by the caller.
    key = commit_operation_id(run_dir, [gate.id])
    with closing(readonly_connection(db)) as con:
        row = con.execute("SELECT digest,summary_json FROM workflow_metadata_commits WHERE operation_id=?",
                          (key,)).fetchone()
    receipt = gate.resolution["metadataApply"]
    if (not row or row[0] != receipt.get("acceptedDigest") or json.loads(row[1]) != receipt
            or receipt.get("ok") is not True or receipt.get("missing") != 0
            or receipt.get("rows") != len(receipt["metadataPartition"]["selectedPathIds"])
            or receipt.get("artifactIds") != gate.artifact_ids):
        raise ValueError("approved metadata partition does not match its committed database receipt")


class PlannerTerminated(SystemExit):
    """Preserve termination semantics while recording the signal under the writer lock."""


@contextmanager
def observe_termination():
    def stop(signum, _frame):
        raise PlannerTerminated(128 + signum)
    main = threading.current_thread() is threading.main_thread()
    if main:
        previous = signal.signal(signal.SIGTERM, stop)
    try:
        yield
    finally:
        if main:
            signal.signal(signal.SIGTERM, previous)
