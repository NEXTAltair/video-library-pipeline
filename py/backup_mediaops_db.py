"""SQLite DB のバックアップ・リストア・ローテーション。

バックアップ方式: SQLite online backup + integrity_check。
命名規則: mediaops.sqlite.bak_{YYYYMMDD}_{HHMMSS}[_{descriptor}]_{unique_id}
保存先: DB と同じディレクトリ (e.g. /mnt/b/_AI_WORK/db/)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path
from video_pipeline.platform.sqlite_snapshot import online_snapshot, check_integrity, readonly_connection


def _ts_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _bak_pattern(db_path: str) -> re.Pattern:
    base = os.path.basename(db_path)
    return re.compile(r"^" + re.escape(base) + r"\.bak_(\d{8}_\d{6})(?:_(.+))?$")


def backup_db(db_path: str, descriptor: str = "") -> tuple[bool, str, str]:
    """WALを含む整合したSQLite snapshotを、一意の新規パスへ作成する。

    Args:
        db_path: 現在の DB パス
        descriptor: 省略可。バックアップ名のサフィックス (e.g. "pre_apply", "pre_relocate")

    Returns:
        (success, backup_path, error_message)
    """
    if not os.path.isfile(db_path):
        return False, "", f"db_path does not exist or is not a file: {db_path}"

    ts = _ts_now()
    desc = descriptor.strip()
    if desc and not re.fullmatch(r"[A-Za-z0-9_.-]+", desc):
        return False, "", "invalid backup descriptor"
    if desc:
        backup_path = f"{db_path}.bak_{ts}_{desc}"
    else:
        backup_path = f"{db_path}.bak_{ts}"

    backup_path += "_" + uuid.uuid4().hex
    try:
        online_snapshot(db_path, backup_path)
    except (OSError, sqlite3.Error, ValueError, TimeoutError) as e:
        return False, "", f"snapshot failed: {e}"

    return True, backup_path, ""


def list_backups(db_path: str) -> list[dict]:
    """既存バックアップの一覧を返す。

    Returns: [{"path": str, "size_bytes": int, "created_at": str, "descriptor": str}, ...]
    新しい順にソート。
    """
    db_dir = os.path.dirname(os.path.abspath(db_path))
    pattern = _bak_pattern(db_path)
    results = []

    try:
        entries = os.listdir(db_dir)
    except OSError:
        return []

    for name in entries:
        if ".failed_" in name or name.endswith(("-wal", "-shm", "-journal")):
            continue
        m = pattern.match(name)
        if not m:
            continue
        full_path = os.path.join(db_dir, name)
        try:
            stat = os.stat(full_path)
            size = stat.st_size
            mtime = stat.st_mtime
        except OSError:
            continue

        ts_str = m.group(1)  # YYYYMMDD_HHMMSS
        descriptor = m.group(2) or ""
        try:
            created_at = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").isoformat()
        except ValueError:
            created_at = ts_str

        results.append({
            "path": full_path,
            "size_bytes": size,
            "created_at": created_at,
            "descriptor": descriptor,
            "_mtime": mtime,
        })

    results.sort(key=lambda x: x["_mtime"], reverse=True)
    for r in results:
        del r["_mtime"]
    return results


def restore_db(db_path: str, backup_path: str) -> tuple[bool, str]:
    """バックアップから DB をリストアする。

    リストア前に現在の DB を .bak_{ts}_pre_restore として退避。

    Returns:
        (success, error_message)
    """
    if not os.path.isfile(backup_path):
        return False, f"backup_path does not exist or is not a file: {backup_path}"

    try:
        check_integrity(backup_path)
    except (OSError, sqlite3.Error, ValueError) as e:
        return False, f"invalid restore snapshot: {e}"
    if Path(db_path).resolve() == Path(backup_path).resolve():
        return False, "cannot restore a database onto itself"

    if os.path.isfile(db_path):
        ok, pre_restore_path, err = backup_db(db_path, descriptor="pre_restore")
        if not ok:
            return False, f"failed to back up current DB before restore: {err}"

    try:
        with closing(readonly_connection(backup_path)) as source, closing(sqlite3.connect(db_path)) as target:
            source.backup(target)
        check_integrity(db_path)
    except (OSError, sqlite3.Error, ValueError) as e:
        return False, f"restore failed: {e}"

    return True, ""


def rotate_backups(db_path: str, keep: int = 10) -> tuple[int, list[str]]:
    """Compatibility no-op: unclassified snapshots may be referenced by old runs.

    Returns:
        (deleted_count, deleted_paths)
    """
    if keep < 0:
        raise ValueError("keep must be nonnegative")
    return 0, []


def _main():
    parser = argparse.ArgumentParser(description="SQLite DB backup/restore/rotate utility")
    parser.add_argument("--db", required=True, help="Path to the SQLite DB file")
    parser.add_argument(
        "--action",
        required=True,
        choices=["backup", "list", "restore", "rotate"],
        help="Action to perform",
    )
    parser.add_argument("--descriptor", default="", help="Backup name suffix (for --action backup)")
    parser.add_argument("--backup-path", help="Path to the backup file (for --action restore)")
    parser.add_argument("--keep", type=int, default=10, help="Number of backups to keep (for --action rotate)")
    args = parser.parse_args()

    if args.action == "backup":
        ok, backup_path, err = backup_db(args.db, descriptor=args.descriptor)
        result = {"ok": ok, "action": "backup", "backup_path": backup_path, "error": err}
        print(json.dumps(result, ensure_ascii=False))
        if not ok:
            raise SystemExit(1)

    elif args.action == "list":
        backups = list_backups(args.db)
        result = {"ok": True, "action": "list", "backups": backups, "count": len(backups)}
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.action == "restore":
        if not args.backup_path:
            parser.error("--backup-path is required for --action restore")
        ok, err = restore_db(args.db, args.backup_path)
        result = {"ok": ok, "action": "restore", "backup_path": args.backup_path, "error": err}
        print(json.dumps(result, ensure_ascii=False))
        if not ok:
            raise SystemExit(1)

    elif args.action == "rotate":
        deleted_count, deleted_paths = rotate_backups(args.db, keep=args.keep)
        result = {
            "ok": True,
            "action": "rotate",
            "keep": args.keep,
            "deleted_count": deleted_count,
            "deleted_paths": deleted_paths,
            "retention": "protected_unclassified",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    _main()
