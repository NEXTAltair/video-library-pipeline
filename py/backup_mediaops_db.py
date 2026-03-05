"""SQLite DB のバックアップ・リストア・ローテーション。

バックアップ方式: shutil.copy2() によるファイルコピー。
命名規則: mediaops.sqlite.bak_{YYYYMMDD}_{HHMMSS}[_{descriptor}]
保存先: DB と同じディレクトリ (e.g. /mnt/b/_AI_WORK/db/)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path


def _ts_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _bak_pattern(db_path: str) -> re.Pattern:
    base = os.path.basename(db_path)
    return re.compile(r"^" + re.escape(base) + r"\.bak_(\d{8}_\d{6})(?:_(.+))?$")


def backup_db(db_path: str, descriptor: str = "") -> tuple[bool, str, str]:
    """DB ファイルをコピーしてバックアップを作成する。

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
    if desc:
        backup_path = f"{db_path}.bak_{ts}_{desc}"
    else:
        backup_path = f"{db_path}.bak_{ts}"

    try:
        shutil.copy2(db_path, backup_path)
    except OSError as e:
        return False, "", f"copy failed: {e}"

    orig_size = os.path.getsize(db_path)
    bak_size = os.path.getsize(backup_path)
    if orig_size != bak_size:
        return False, backup_path, f"size mismatch after copy: orig={orig_size} bak={bak_size}"

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

    if os.path.isfile(db_path):
        ok, pre_restore_path, err = backup_db(db_path, descriptor="pre_restore")
        if not ok:
            return False, f"failed to back up current DB before restore: {err}"

    try:
        shutil.copy2(backup_path, db_path)
    except OSError as e:
        return False, f"restore copy failed: {e}"

    bak_size = os.path.getsize(backup_path)
    restored_size = os.path.getsize(db_path)
    if bak_size != restored_size:
        return False, f"size mismatch after restore: backup={bak_size} restored={restored_size}"

    return True, ""


def rotate_backups(db_path: str, keep: int = 10) -> tuple[int, list[str]]:
    """古いバックアップを削除して keep 件に保つ。

    Returns:
        (deleted_count, deleted_paths)
    """
    backups = list_backups(db_path)
    to_delete = backups[keep:]
    deleted_paths = []
    for entry in to_delete:
        try:
            os.remove(entry["path"])
            deleted_paths.append(entry["path"])
        except OSError:
            pass
    return len(deleted_paths), deleted_paths


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
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    _main()
