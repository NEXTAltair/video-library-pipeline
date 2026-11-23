"""Retain approved metadata holds across non-applying relocate replans."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from video_pipeline.domain.companion_bundle import companion_paths
from video_pipeline.platform.pathscan_common import normalize_win_for_id, path_id_for, windows_to_wsl_path


def physical_paths(video):
    return {normalize_win_for_id(p) for p in (video, *companion_paths(video).values())}


def data_rows(rows):
    result = []
    for row in rows:
        if not isinstance(row, dict) or ("_meta" in row and len(row) != 1):
            raise ValueError("malformed metadata partition artifact row")
        if "_meta" not in row:
            result.append(row)
    return result


def owned_rows(artifact, run_dir):
    path = Path(windows_to_wsl_path(artifact.path)).resolve()
    if artifact.status != "available" or not path.is_relative_to(run_dir.resolve()):
        raise ValueError("metadata partition input ownership mismatch")
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != artifact.sha256:
        raise ValueError("metadata partition input checksum mismatch")
    return data_rows(json.loads(line) for line in payload.decode("utf-8-sig").splitlines() if line.strip())


def identities(rows, key):
    result = {}
    for row in rows:
        pid, path = row.get("path_id"), row.get(key)
        if not isinstance(pid, str) or not pid.strip() or pid in result or not isinstance(path, str) or not path.strip():
            raise ValueError("metadata partition row identity missing or duplicate")
        result[pid] = normalize_win_for_id(path)
    if len(set(result.values())) != len(result):
        raise ValueError("metadata partition has overlapping source paths")
    return result


def _partition_ids(values):
    if not isinstance(values, list) or not values or any(not isinstance(v, str) or not v.strip() for v in values):
        raise ValueError("invalid metadata partition IDs")
    if len(values) != len(set(values)):
        raise ValueError("duplicate metadata partition IDs")
    return set(values)


def reserve_exclusions(rows, selected, sources, reserved):
    for row in rows:
        pid = row.get("path_id")
        if (pid is None and row.get("status") == "skipped" and row.get("reason") == "unregistered_path"
                and row.get("src") and not row.get("dst") and not row.get("members")):
            # The original pre-registration preview had no ID. Registration
            # derives it from the path; only that exact subsequent identity counts.
            pid = path_id_for(row["src"])
        if pid in sources and normalize_win_for_id(row["src"]) != sources[pid]:
            raise ValueError("metadata partition source differs from earlier owned plan")
        if pid not in selected:
            for key in ("src", "dst"):
                if row.get(key):
                    reserved.update(physical_paths(row[key]))
            for member in row.get("members", []):
                reserved.update(normalize_win_for_id(member[key]) for key in ("src", "dst"))


def resolve_scope(run, run_dir):
    """Only approved, queue-bound decisions count; subsequent decisions may narrow, never widen."""
    history, selected, sources, reserved = [], {}, {}, set()
    observed_mtimes = {}
    for gid in run.review_gate_ids:
        gate = run.review_gates[gid]
        resolution = gate.resolution or {}
        summary = resolution.get("metadataApply") or {}
        if "metadataPartition" not in summary:
            continue
        if gate.type != "metadata_review" or gate.status != "approved" or resolution.get("action") != "apply_reviewed_metadata":
            raise ValueError("metadata partition is not an approved review decision")
        decision = summary["metadataPartition"]
        queue = run.artifacts.get(decision["queueArtifactId"])
        if (not queue or queue.type != "relocate_metadata_queue"
                or queue.sha256 != decision.get("queueSha256") or not gate.artifact_ids):
            raise ValueError("metadata partition queue binding mismatch")
        for aid in gate.artifact_ids:
            draft = run.artifacts[aid]
            if draft.type != "metadata_review_yaml" or len(draft.input_artifact_ids) != 1:
                raise ValueError("invalid partition review lineage")
            extract = run.artifacts[draft.input_artifact_ids[0]]
            if extract.type != "metadata_extract_output" or extract.input_artifact_ids != [queue.id]:
                raise ValueError("metadata partition does not belong to this gate")
        queue_rows = owned_rows(queue, run_dir)
        cohort = identities(queue_rows, "path")
        for row in queue_rows:
            if row.get("mtime_utc"):
                observed_mtimes.setdefault(row["path_id"], []).append(row["mtime_utc"])
        yes, no = (_partition_ids(decision[k]) for k in ("selectedPathIds", "heldPathIds"))
        if yes & no or yes | no != set(cohort):
            raise ValueError("metadata partition does not cover its queue")
        if history and any(selected.get(pid) != path for pid, path in cohort.items()):
            raise ValueError("later metadata partition exceeds retained scope")
        sources.update(cohort)
        selected = {pid: cohort[pid] for pid in yes}
        # Freeze all known excluded destinations too. A planned-metadata queue
        # can descend from a physical subset; its ancestor still owns held slots.
        preceding = run.artifact_ids[:run.artifact_ids.index(queue.id)]
        for aid in preceding:
            plan = run.artifacts[aid]
            if plan.type != "relocate_plan":
                continue
            reserve_exclusions(owned_rows(plan, run_dir), selected, sources, reserved)
        for pid, path in sources.items():
            if pid not in selected:
                reserved.update(physical_paths(path))
        history.append(hashlib.sha256(json.dumps([gid, decision], sort_keys=True).encode()).hexdigest())
        # Before the first bound replan, a newer plan/queue means the review
        # was for a superseded planning generation, not this one.
        for aid in run.artifact_ids[len(preceding) + 1:]:
            later = run.artifacts[aid]
            if later.type == "relocate_plan" and later.metadata.get("metadataScopeHistory", [])[:len(history)] == history:
                break
            if later.type in {"relocate_plan", "relocate_metadata_queue"}:
                raise ValueError("metadata partition refers to a stale planning generation")
        for aid in run.artifact_ids:
            child = run.artifacts[aid]
            if child.type != "relocate_plan" or child.metadata.get("metadataScopeHistory") != history:
                continue
            selection = child.metadata.get("subsetSelection")
            if not selection:
                continue
            parent = run.artifacts[selection["parentArtifactId"]]
            if parent.sha256 != selection["parentSha256"] or child.input_artifact_ids != [parent.id]:
                raise ValueError("retained subset parent binding changed")
            from video_pipeline.workflows.relocate_subset import partition_plan
            rows = owned_rows(parent, run_dir)
            chosen = partition_plan(rows, selection["selectedPathIds"], selection["heldPathIds"])
            narrowed = identities(chosen, "src")
            if chosen != owned_rows(child, run_dir) or any(selected.get(k) != v for k, v in narrowed.items()):
                raise ValueError("retained subset differs from approved scope")
            selected = narrowed
            reserve_exclusions(rows, selected, sources, reserved)
    for plan in run.artifacts.values():
        previous = plan.metadata.get("metadataScopeHistory", [])
        if plan.type == "relocate_plan" and previous != history[:len(previous)]:
            raise ValueError("retained metadata partition changed or disappeared")
    if not history:
        return None
    return {"selected": selected, "sources": sources, "reserved": sorted(reserved), "history": history,
            "observedMtimes": observed_mtimes}


def check_output(scope, rows, *, queue=False, subset=False):
    rows = data_rows(rows)
    result = identities(rows, "path" if queue else "src")
    selected = scope["selected"]
    if any(selected.get(pid) != path for pid, path in result.items()) or (
            not queue and not subset and result != selected):
        raise ValueError("planner output differs from approved metadata cohort")
    if queue:
        return
    reserved = set(scope["reserved"])
    for row in rows:
        if row.get("status") != "planned":
            continue
        for video in (row["src"], row["dst"]):
            if physical_paths(video) & reserved:
                raise ValueError("planned member overlaps a held source or destination")
        members = row.get("members", [])
        if members:
            src = {"video": row["src"], **companion_paths(row["src"])}
            dst = {"video": row["dst"], **companion_paths(row["dst"])}
            if any(m.get("kind") not in src or m.get("src") != src[m["kind"]] or m.get("dst") != dst[m["kind"]]
                   for m in members):
                raise ValueError("planned companion differs from its selected video")
