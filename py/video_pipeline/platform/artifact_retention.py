"""Explicit cache lifecycle. Unknown data and evidence are never cleanup targets.

Cleanup means a same-filesystem rename into .trash, not permanent deletion.
An immutable plan and a durable intent make interrupted moves inspectable/restorable.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import uuid
import yaml

from video_pipeline.platform.pathscan_common import windows_to_wsl_path

POLICY = ".artifact-policy.json"
KIND = "video_pipeline_artifact_workspace"
ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
MEDIA_SUFFIXES = {".ts", ".m2ts", ".mp4", ".mkv", ".avi", ".mov", ".wmv"}


def valid_id(value):
    if not isinstance(value, str) or not ID.fullmatch(value) or ".." in value:
        raise ValueError("invalid artifact job/plan ID")
    return value


def safe_path(path):
    """Do not follow symlinks, including ancestors of the selected root."""
    path = Path(os.path.abspath(path))
    for part in [*reversed(path.parents), path]:
        if part.is_symlink():
            raise ValueError("symlink is not an artifact management boundary: " + str(part))
    return path


def read_json(path):
    safe_path(path)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, body):
    path = safe_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        json.dump(body, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
        temporary = stream.name
    os.replace(temporary, path)
    # Rename intents/receipts must survive a host crash, not just a process exit.
    sync_dir(path.parent)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def raise_walk_error(error):
    raise error


def tree_snapshot(path):
    path = safe_path(path)
    if not path.is_dir():
        raise ValueError("cache must be a directory")
    rows = []
    for directory, dirs, files in os.walk(path, followlinks=False, onerror=raise_walk_error):
        for item in [Path(directory), *[Path(directory) / name for name in sorted(files)]]:
            info = item.lstat()
            if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)) or item.is_symlink():
                raise ValueError("cache contains symlink/special file")
            if stat.S_ISREG(info.st_mode) and (info.st_nlink != 1 or item.suffix.lower() in MEDIA_SUFFIXES):
                raise ValueError("cache contains a linked file or recording")
            rows.append({"path": item.relative_to(path).as_posix(), "directory": item.is_dir(),
                         "size": 0 if item.is_dir() else info.st_size,
                         "mtime_ns": info.st_mtime_ns, "device": info.st_dev, "inode": info.st_ino,
                         "sha256": None if item.is_dir() else digest(item)})
        for name in dirs:
            if (Path(directory) / name).is_symlink():
                raise ValueError("cache contains symlink directory")
        dirs.sort()
    return sorted(rows, key=lambda row: row["path"])


def create_workspace(root, job_id=None, *, cache_days=7):
    from .writer_lock import workflow_writer
    root = safe_path(root)
    with workflow_writer(_ops_for_artifacts(root)):
        return _create_workspace(root, job_id, cache_days=cache_days)


def _create_workspace(root, job_id=None, *, cache_days=7):
    root = safe_path(root)
    job_id = valid_id(job_id or "job_" + uuid.uuid4().hex)
    if isinstance(cache_days, bool) or not isinstance(cache_days, int) or cache_days < 0:
        raise ValueError("cache_days must be a non-negative integer")
    job = root / job_id
    job.mkdir(parents=True, exist_ok=False)
    (job / "cache").mkdir()
    (job / "evidence").mkdir()
    write_json(job / POLICY, {"kind": KIND, "schema_version": 1, "job_id": job_id,
                             "status": "active", "cache_days": cache_days,
                             "created_at": datetime.now(timezone.utc).isoformat(), "completed_at": None})
    return job


def finish_workspace(job, *, status="complete"):
    from .writer_lock import workflow_writer
    job = safe_path(job)
    with workflow_writer(_ops_for_artifacts(job.parent)):
        return _finish_workspace(job, status=status)


def _finish_workspace(job, *, status="complete"):
    if status not in {"complete", "failed", "blocked"}:
        raise ValueError("invalid artifact workspace terminal status")
    job = safe_path(job)
    policy = read_json(job / POLICY)
    if (policy.get("kind") != KIND or policy.get("job_id") != job.name
            or policy.get("status") not in {"active", "blocked", "failed"}):
        raise ValueError("workspace is already complete or is not owned by this plugin")
    policy.update(status=status, completed_at=datetime.now(timezone.utc).isoformat())
    write_json(job / POLICY, policy)


def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, child in value.items():
            # A completed V2 run's readonly preview DB location is provenance,
            # not a checksum-bound artifact dependency. Actual artifacts still count.
            if key == "configSnapshot":
                continue
            yield from _strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)


def eligible(job, now):
    policy = read_json(job / POLICY)
    if policy.get("kind") != KIND or policy.get("schema_version") != 1 or policy.get("job_id") != valid_id(job.name):
        raise ValueError("unknown or invalid workspace policy")
    if policy.get("status") != "complete":
        return False, "active_or_unresolved"
    days = policy.get("cache_days")
    if isinstance(days, bool) or not isinstance(days, int) or days < 0:
        raise ValueError("invalid cache retention")
    completed = datetime.fromisoformat(policy["completed_at"])
    if completed.tzinfo is None:
        raise ValueError("completion timestamp must have a timezone")
    if now - completed.timestamp() < days * 86400:
        return False, "retention_not_elapsed"
    cache = safe_path(job / "cache")
    if not cache.exists():
        return False, "cache_absent"
    # Evidence and V2 gates override retention. Never age out a pending review.
    for directory, dirs, files in os.walk(job, followlinks=False, onerror=raise_walk_error):
        if Path(directory) == job:
            dirs[:] = [name for name in dirs if name != "cache"]
        for name in dirs + files:
            if (Path(directory) / name).is_symlink():
                raise ValueError("workspace contains an uninspectable symlink")
        for name in files:
            file = Path(directory) / name
            if file.suffix not in {".json", ".jsonl", ".yaml", ".yml", ".md", ".txt"} or file.name == POLICY:
                continue
            if file.suffix == ".json":
                body = read_json(file)
            elif file.suffix == ".jsonl":
                with file.open(encoding="utf-8-sig") as stream:
                    body = [json.loads(line) for line in stream if line.strip()]
            elif file.suffix in {".yaml", ".yml"}:
                body = yaml.safe_load(file.read_text(encoding="utf-8"))
            else:
                text = file.read_text(encoding="utf-8")
                if str(cache) in text or "cache/" in text or "cache\\" in text:
                    return False, "cache_referenced_by_evidence"
                continue
            if name == "run.json":
                gates = body.get("reviewGates", {})
                gates = gates.values() if isinstance(gates, dict) else gates
                if body.get("status") != "complete" or any(gate.get("status") == "open" for gate in gates):
                    return False, "workflow_or_review_open"
            for value in _strings(body):
                # Paths recorded relative to either the evidence file or workspace.
                for base in (file.parent, job):
                    candidate = Path(os.path.abspath(base / windows_to_wsl_path(value)))
                    if candidate.is_relative_to(cache):
                        return False, "cache_referenced_by_evidence"
    return True, "expired_completed_cache"


def inventory(root):
    """One stat pass; summary size is independent of the number of JPEGs."""
    root = safe_path(root)
    if not root.exists():
        return {"ok": True, "root": str(root), "exists": False, "files": 0, "bytes": 0,
                "groups": {}, "errors": [], "excludedLinks": 0}
    if not root.is_dir():
        raise ValueError("artifact root does not exist")
    groups = {}
    errors = []
    excluded_links = 0
    for directory, dirs, files in os.walk(root, followlinks=False, onerror=lambda error: errors.append(str(error))):
        for name in list(dirs):
            item = Path(directory) / name
            if item.is_symlink():
                excluded_links += 1
                dirs.remove(name)
        for name in files:
            item = Path(directory) / name
            group = item.relative_to(root).parts[0] if item.parent != root else "(root files)"
            row = groups.setdefault(group, {"files": 0, "bytes": 0, "types": {}})
            try:
                info = item.lstat()
                if stat.S_ISLNK(info.st_mode):
                    excluded_links += 1
                    continue
                if not stat.S_ISREG(info.st_mode):
                    errors.append("special file not counted: " + str(item))
                    continue
                suffix = item.suffix.lower() or "(no extension)"
                row["files"] += 1
                row["bytes"] += info.st_size
                typed = row["types"].setdefault(suffix, {"files": 0, "bytes": 0})
                typed["files"] += 1
                typed["bytes"] += info.st_size
            except OSError as error:
                errors.append(str(error))
    for name, row in groups.items():
        job = root / name
        row["managed"] = not (job / POLICY).is_symlink() and (job / POLICY).is_file() if name != "(root files)" else False
        row["cleanup_policy"] = "explicit_cache_only" if row["managed"] else "protected_unknown"
    return {"ok": not errors, "root": str(root), "files": sum(row["files"] for row in groups.values()),
            "bytes": sum(row["bytes"] for row in groups.values()), "groups": groups,
            "errors": errors[:100], "errorCount": len(errors), "excludedLinks": excluded_links}


@contextmanager
def maintenance_lock(root):
    root = safe_path(root)
    state = safe_path(root / ".maintenance")
    state.mkdir(exist_ok=True)
    from .writer_lock import workflow_writer
    # The canonical ops/artifacts/job namespace shares V2/daily ownership.
    # Legacy arbitrary trees use their parent, outside the archived tree.
    ops = _ops_for_artifacts(root)
    with workflow_writer(ops), safe_path(state / "lock").open("a+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield state


def _ops_for_artifacts(root):
    return root.parent.parent if root.name == "job" and root.parent.name == "artifacts" else root.parent


def plan_cleanup(root, *, now=None):
    root = safe_path(root)
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    if not math.isfinite(now):
        raise ValueError("invalid clock")
    with maintenance_lock(root) as state:
        candidates, protected = [], []
        for job in sorted(root.iterdir()):
            if job.name.startswith(".") or not job.is_dir():
                continue
            try:
                if job.is_symlink():
                    raise ValueError("symlink workspace")
                if not (job / POLICY).exists():
                    protected.append({"job": job.name, "reason": "unknown_legacy_data"})
                    continue
                allowed, reason = eligible(job, now)
                if not allowed:
                    protected.append({"job": job.name, "reason": reason})
                    continue
                snapshot = tree_snapshot(job / "cache")
                if not any(not row["directory"] for row in snapshot):
                    continue
                candidates.append({"job": job.name, "policy_sha256": digest(job / POLICY),
                                   "snapshot": snapshot})
            except Exception as error:
                # Invalid legacy evidence is held, not a reason to weaken guards
                # or lose the rest of the read-only cleanup preview.
                protected.append({"job": job.name, "reason": "inspection_failed", "error": str(error)})
        plan_id = "cleanup_" + uuid.uuid4().hex
        plan = {"kind": KIND, "schema_version": 1, "plan_id": plan_id, "root": str(root),
                "created_at": now, "candidates": candidates, "protected": protected,
                "files": sum(not row["directory"] for candidate in candidates for row in candidate["snapshot"]),
                "bytes": sum(row["size"] for candidate in candidates for row in candidate["snapshot"])}
        write_json(state / "plans" / (plan_id + ".json"), plan)
        return {"ok": True, "planId": plan_id, "planPath": str(state / "plans" / (plan_id + ".json")),
                "jobs": len(candidates), "files": plan["files"], "bytes": plan["bytes"], "protected": protected}


def load_plan(root, state, plan_id):
    plan_path = state / "plans" / (valid_id(plan_id) + ".json")
    plan = read_json(plan_path)
    if (plan.get("kind") != KIND or plan.get("schema_version") != 1 or plan.get("plan_id") != plan_id
            or plan.get("root") != str(root)):
        raise ValueError("artifact plan root/identity mismatch")
    jobs = [valid_id(row["job"]) for row in plan["candidates"]]
    if len(set(jobs)) != len(jobs):
        raise ValueError("duplicate cleanup job")
    return plan, digest(plan_path)


def apply_cleanup(root, plan_id):
    root = safe_path(root)
    with maintenance_lock(root) as state:
        plan, checksum = load_plan(root, state, plan_id)
        receipt_path = state / "receipts" / (plan_id + ".json")
        if receipt_path.exists():
            raise ValueError("cleanup already started; inspect or restore, never replay")
        now = datetime.now(timezone.utc).timestamp()
        # Validate ALL sources before moving even one cache.
        for row in plan["candidates"]:
            job = safe_path(root / row["job"])
            allowed, reason = eligible(job, now)
            if not allowed or digest(job / POLICY) != row["policy_sha256"] or tree_snapshot(job / "cache") != row["snapshot"]:
                raise ValueError("stale cleanup plan: " + row["job"] + " (" + reason + ")")
        trash = safe_path(root / ".trash" / plan_id)
        trash.mkdir(parents=True, exist_ok=False)
        receipt = {"plan_id": plan_id, "plan_sha256": checksum, "status": "applying", "moved": []}
        write_json(receipt_path, receipt)
        for row in plan["candidates"]:
            source = safe_path(root / row["job"] / "cache")
            destination = safe_path(trash / row["job"] / "cache")
            destination.parent.mkdir(exist_ok=False)
            if tree_snapshot(source) != row["snapshot"]:
                raise ValueError("cache changed during cleanup; restore using the saved intent")
            os.rename(source, destination)
            sync_dir(source.parent)
            sync_dir(destination.parent)
            receipt["moved"].append(row["job"])
            write_json(receipt_path, receipt)
        receipt["status"] = "complete"
        write_json(receipt_path, receipt)
        return {"ok": True, "planId": plan_id, "movedJobs": len(receipt["moved"]),
                "files": plan["files"], "bytes": plan["bytes"], "trashPath": str(trash), "permanentlyDeleted": 0}


def restore_cleanup(root, plan_id, *, apply=False):
    root = safe_path(root)
    with maintenance_lock(root) as state:
        plan, checksum = load_plan(root, state, plan_id)
        receipt_path = state / "receipts" / (plan_id + ".json")
        receipt = read_json(receipt_path)
        if receipt.get("plan_id") != plan_id or receipt.get("plan_sha256") != checksum:
            raise ValueError("cleanup intent changed")
        if receipt.get("status") == "restored":
            raise ValueError("cleanup already restored")
        moves = []
        # Recover from a crash between rename and receipt update via exact tree hashes.
        for row in plan["candidates"]:
            target = safe_path(root / row["job"] / "cache")
            source = safe_path(root / ".trash" / plan_id / row["job"] / "cache")
            if source.exists() and target.exists():
                raise ValueError("restore destination occupied: " + str(target))
            existing = source if source.exists() else target
            if not existing.exists() or tree_snapshot(existing) != row["snapshot"]:
                raise ValueError("restore source missing or changed: " + row["job"])
            if source.exists():
                moves.append((source, target))
        if apply:
            receipt["status"] = "restoring"
            write_json(receipt_path, receipt)
            for source, target in moves:
                if target.exists() or target.is_symlink():
                    raise ValueError("restore destination occupied")
                os.rename(source, target)
                sync_dir(source.parent)
                sync_dir(target.parent)
            receipt["status"] = "restored"
            write_json(receipt_path, receipt)
        return {"ok": True, "planId": plan_id, "preview": not apply, "restoreJobs": len(moves), "permanentlyDeleted": 0}
