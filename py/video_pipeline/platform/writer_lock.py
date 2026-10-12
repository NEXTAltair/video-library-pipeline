"""Reentrant, process-wide writer scopes. Never unlink a live lock inode.

All V2, daily, dedup and artifact-maintenance writers take the ops scope first.
SQLite still arbitrates database transactions; this scope also covers native
side effects and manifest read-modify-write, not just the final JSON replace.
"""
from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

from .pathscan_common import windows_to_wsl_path

_registry_guard = threading.Lock()
_registry = {}


@contextmanager
def writer_lock(path, *, timeout=30.0):
    key = str(Path(path).resolve())
    with _registry_guard:
        entry = _registry.setdefault(key, {"mutex": threading.RLock(), "depth": 0})
    deadline = time.monotonic() + timeout
    if not entry["mutex"].acquire(timeout=timeout):
        raise TimeoutError(f"writer scope busy: {key}")
    handle = None
    try:
        if entry["depth"] == 0:
            Path(key).parent.mkdir(parents=True, exist_ok=True)
            handle = open(key, "a+b")
            if os.name == "nt":
                import msvcrt
                if handle.seek(0, os.SEEK_END) == 0:
                    handle.write(b"\0")
                    handle.flush()
            else:
                import fcntl
            while True:
                try:
                    if os.name == "nt":
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except (BlockingIOError, PermissionError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"writer scope busy: {key}")
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        entry["depth"] += 1
        try:
            yield
        finally:
            entry["depth"] -= 1
    finally:
        # close releases the OS lock, including on exceptions/process death.
        if handle is not None:
            handle.close()
        entry["mutex"].release()


def workflow_writer(ops_root, *, timeout=30.0):
    root = Path(windows_to_wsl_path(str(ops_root))).resolve()
    return writer_lock(root / ".workflow-writer.lock", timeout=timeout)


def serialized_workflow(fn):
    """Service entrypoint (self, config); keep nested resume/apply reentrant."""
    @wraps(fn)
    def wrapped(self, config, *args, **kwargs):
        with workflow_writer(config.windows_ops_root):
            return fn(self, config, *args, **kwargs)
    return wrapped


def serialized_store(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        with workflow_writer(self.windows_ops_root):
            return fn(self, *args, **kwargs)
    return wrapped


def serialized_ops_function(fn):
    """Maintenance entrypoint (db, ops_root, ...), same scope as V2 services."""
    @wraps(fn)
    def wrapped(db, ops_root, *args, **kwargs):
        with workflow_writer(ops_root):
            return fn(db, ops_root, *args, **kwargs)
    return wrapped
