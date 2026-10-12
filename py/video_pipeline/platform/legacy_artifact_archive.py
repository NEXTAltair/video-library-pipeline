"""Move a legacy artifacts tree outside plugin capture, preserving every byte/link.

This is archival, NOT cache cleanup. Source/destination and the complete tree are
bound to a saved plan; interrupted renames can be restored without overwriting.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from video_pipeline.platform.artifact_retention import (
    KIND, digest, maintenance_lock, read_json, safe_path, sync_dir, write_json,
    raise_walk_error, valid_id,
)


def archive_snapshot(root):
    root = safe_path(root)
    if not root.is_dir():
        raise ValueError("legacy artifacts root must be a directory")
    framed = hashlib.sha256()
    files = size = links = 0
    for directory, dirs, names in os.walk(root, followlinks=False, onerror=raise_walk_error):
        dirs.sort()
        names.sort()
        items = [Path(directory)]
        for name in list(dirs):
            item = Path(directory) / name
            if item.is_symlink():
                items.append(item)
                dirs.remove(name)
        items.extend(Path(directory) / name for name in names)
        for item in items:
            info = item.lstat()
            row = {"path": item.relative_to(root).as_posix(), "device": info.st_dev,
                   "inode": info.st_ino, "mode": info.st_mode, "mtime_ns": info.st_mtime_ns}
            if stat.S_ISLNK(info.st_mode):
                row["link"] = os.readlink(item)
                links += 1
            elif stat.S_ISREG(info.st_mode):
                row.update(size=info.st_size, sha256=digest(item))
                files += 1
                size += info.st_size
            elif not stat.S_ISDIR(info.st_mode):
                raise ValueError("special file cannot be archived: " + str(item))
            framed.update(json.dumps(row, sort_keys=True).encode() + b"\0")
    return {"sha256": framed.hexdigest(), "files": files, "bytes": size, "links": links}


def plan_archive(root, destination):
    root, destination = safe_path(root), safe_path(destination)
    if root.name != "artifacts" or not root.is_dir():
        raise ValueError("archive source must be an existing artifacts/ directory")
    if destination.is_relative_to(root.parent):
        raise ValueError("archive destination must be outside the plugin source tree")
    if destination.exists():
        raise ValueError("archive destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if root.stat().st_dev != destination.parent.stat().st_dev:
        raise ValueError("archive requires the same filesystem; no copy/delete fallback")
    with maintenance_lock(destination.parent) as state:
        plan_id = "archive_" + uuid.uuid4().hex
        plan = {"kind": KIND, "schema_version": 1, "plan_id": plan_id,
                "root": str(root), "destination": str(destination), "snapshot": archive_snapshot(root)}
        path = state / "archive-plans" / (plan_id + ".json")
        write_json(path, plan)
        return {"ok": True, "preview": True, "planPath": str(path), "source": str(root),
                "destination": str(destination), **plan["snapshot"]}


def load_archive_plan(root, plan_path):
    plan_path = safe_path(plan_path)
    plan = read_json(plan_path)
    destination = safe_path(plan["destination"])
    expected = destination.parent / ".maintenance" / "archive-plans" / (valid_id(plan["plan_id"]) + ".json")
    if (plan.get("kind") != KIND or plan.get("schema_version") != 1 or plan.get("root") != str(safe_path(root))
            or safe_path(root).name != "artifacts" or plan_path != expected or destination.is_relative_to(safe_path(root).parent)):
        raise ValueError("archive plan identity/boundary mismatch")
    return plan, destination, digest(plan_path)


def apply_archive(root, plan_path):
    root = safe_path(root)
    plan_path = safe_path(plan_path)
    plan, destination, checksum = load_archive_plan(root, plan_path)
    with maintenance_lock(destination.parent) as state:
        receipt_path = state / "archive-receipts" / (plan["plan_id"] + ".json")
        if receipt_path.exists():
            raise ValueError("archive already started; inspect/restore instead of replay")
        if destination.exists() or destination.is_symlink() or archive_snapshot(root) != plan["snapshot"]:
            raise ValueError("archive plan changed or destination occupied")
        write_json(receipt_path, {"plan_sha256": checksum, "planPath": str(plan_path), "status": "archiving"})
        os.rename(root, destination)
        sync_dir(root.parent)
        sync_dir(destination.parent)
        if archive_snapshot(destination) != plan["snapshot"]:
            raise ValueError("archive tree changed; preserve intent and reconcile before restore")
        write_json(receipt_path, {"plan_sha256": checksum, "planPath": str(plan_path), "status": "archived"})
        return {"ok": True, "receiptPath": str(receipt_path), "source": str(root),
                "destination": str(destination), "permanentlyDeleted": 0, **plan["snapshot"]}


def restore_archive(root, receipt_path, *, apply=False):
    root, receipt_path = safe_path(root), safe_path(receipt_path)
    receipt = read_json(receipt_path)
    plan, destination, checksum = load_archive_plan(root, receipt["planPath"])
    expected = destination.parent / ".maintenance" / "archive-receipts" / (plan["plan_id"] + ".json")
    if receipt_path != expected or receipt.get("plan_sha256") != checksum or receipt.get("status") == "restored":
        raise ValueError("changed or already restored archive receipt")
    with maintenance_lock(destination.parent):
        if root.exists() or root.is_symlink():
            if not destination.exists() and not root.is_symlink() and archive_snapshot(root) == plan["snapshot"]:
                if apply:
                    receipt["status"] = "restored"
                    write_json(receipt_path, receipt)
                return {"ok": True, "preview": not apply, "alreadyAtOriginal": True, "permanentlyDeleted": 0}
            raise ValueError("original artifacts path is occupied; restoration never overwrites")
        if archive_snapshot(destination) != plan["snapshot"]:
            raise ValueError("archived tree changed")
        if apply:
            receipt["status"] = "restoring"
            write_json(receipt_path, receipt)
            os.rename(destination, root)
            sync_dir(destination.parent)
            sync_dir(root.parent)
            receipt["status"] = "restored"
            write_json(receipt_path, receipt)
        return {"ok": True, "preview": not apply, "source": str(destination), "destination": str(root), "permanentlyDeleted": 0}
