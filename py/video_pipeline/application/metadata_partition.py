"""Explicit per-draft metadata partitions, bound to one immutable relocate queue."""
from __future__ import annotations

import hashlib
import json

from video_pipeline.platform.pathscan_common import normalize_win_for_id


def _ids(values):
    if (not isinstance(values, list)
            or any(not isinstance(v, str) or not v.strip() or v.strip() != v for v in values)
            or len(set(values)) != len(values)):
        raise ValueError("metadata partition requires unique string path IDs")
    return set(values)


def split_rows(rows, decision):
    if not isinstance(decision, dict) or set(decision) != {"selected_path_ids", "held_path_ids"}:
        raise ValueError("metadata_partition requires selected_path_ids and held_path_ids")
    selected, held = (_ids(decision[key]) for key in ("selected_path_ids", "held_path_ids"))
    if selected & held or selected | held != _ids([row.get("path_id") for row in rows]):
        raise ValueError("metadata partition must cover each extraction path ID exactly once")
    return [r for r in rows if r["path_id"] in selected]


def _identities(rows):
    ids = _ids([r.get("path_id") for r in rows])
    if any(not isinstance(r.get("path"), str) or not r["path"].strip() for r in rows):
        raise ValueError("metadata partition requires source path identity")
    result = {r["path_id"]: normalize_win_for_id(r["path"]) for r in rows}
    if len(set(result.values())) != len(ids):
        raise ValueError("metadata partition contains overlapping source paths")
    return result


def bind_queue(run, run_dir, batches, local_path):
    queue_ids = {pid for b in batches for pid in b["queueIds"]}
    if len(queue_ids) != 1 or any(len(b["queueIds"]) != 1 for b in batches):
        raise ValueError("metadata partition must belong to one run-owned relocate queue")
    queue = run.artifacts.get(next(iter(queue_ids)))
    if not queue or queue.type != "relocate_metadata_queue" or queue.status != "available":
        raise ValueError("metadata partition source queue is missing or unavailable")
    if any(run.artifacts[aid].type in {"relocate_plan", "relocate_metadata_queue"}
           for aid in run.artifact_ids[run.artifact_ids.index(queue.id) + 1:]):
        raise ValueError("metadata partition source queue predates the current planning generation")
    path = local_path(queue.path)
    if not path.is_relative_to(run_dir.resolve()):
        raise ValueError("metadata partition queue escaped run directory")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != queue.sha256:
        raise ValueError("metadata partition queue checksum changed")
    queue_rows = [json.loads(line) for line in content.decode("utf-8-sig").splitlines() if line.strip()]
    if not all(isinstance(r, dict) and ("_meta" not in r or len(r) == 1) for r in queue_rows):
        raise ValueError("invalid metadata partition queue")
    expected = _identities([r for r in queue_rows if "_meta" not in r])
    if _identities([r for b in batches for r in b["cohortRows"]]) != expected:
        raise ValueError("extraction identities do not cover the original metadata queue")
    selected = {r["path_id"] for b in batches for r in b["rows"]}
    held = expected.keys() - selected
    if not selected or not held:
        raise ValueError("metadata partition requires both accepted and held rows across the gate")
    return {"queueArtifactId": queue.id, "queueSha256": queue.sha256,
            "selectedPathIds": sorted(selected), "heldPathIds": sorted(held)}
