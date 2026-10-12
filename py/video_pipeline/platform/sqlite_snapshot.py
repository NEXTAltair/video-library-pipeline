"""Checked SQLite online snapshots shared by every backup/preview producer."""
from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path


def readonly_connection(path):
    source = Path(path).resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"SQLite source is not a file: {source}")
    return sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=5)


def check_integrity(path):
    with closing(readonly_connection(path)) as con:
        result = [row[0] for row in con.execute("PRAGMA integrity_check")]
        if result != ["ok"]:
            raise ValueError(f"SQLite snapshot integrity failed: {result[:10]}")


def online_snapshot(source_path, destination, *, timeout=30):
    source = Path(source_path).resolve(strict=True)
    target = Path(destination).absolute()
    if source == target.resolve():
        raise ValueError("snapshot destination is the source database")
    target.parent.mkdir(parents=True, exist_ok=True)
    # Reserve the name before opening SQLite. Never overwrite an older backup.
    fd = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    deadline = time.monotonic() + timeout

    def progress(status, remaining, total):
        if time.monotonic() >= deadline:
            raise TimeoutError("SQLite snapshot deadline exceeded")

    try:
        with closing(readonly_connection(source)) as original, closing(sqlite3.connect(target)) as clone:
            original.backup(clone, pages=256, progress=progress, sleep=0.05)
            clone.commit()
            # A snapshot is a standalone artifact, never dependent on sibling
            # -wal/-shm files created by a later read-only inspection.
            clone.execute("PRAGMA journal_mode=DELETE")
        check_integrity(target)
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
        if os.name != "nt":
            fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    except BaseException:
        # Preserve failed output for inspection without advertising it as a backup.
        if target.exists():
            target.rename(target.with_name(target.name + ".failed_" + uuid.uuid4().hex))
        raise
    return target
