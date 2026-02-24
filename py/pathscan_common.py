#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any

from windows_pwsh_bridge import run_pwsh_jsonl

DEFAULT_EXTENSIONS = [".mp4"]
DEFAULT_SCAN_RETRY_COUNT = 1
MAX_SUMMARY_WARNINGS = 200


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_compact(d: datetime | None = None) -> str:
    dt_obj = d or datetime.now()
    return dt_obj.strftime("%Y%m%d_%H%M%S")


def normalize_win_for_id(p: str) -> str:
    return p.replace("/", "\\").lower()


def split_win(p: str) -> tuple[str | None, str | None, str | None, str | None]:
    wp = PureWindowsPath(p)
    drive = wp.drive[:-1] if wp.drive.endswith(":") else (wp.drive or None)
    name = wp.name or None
    ext = wp.suffix or None
    parent = str(wp.parent) if str(wp.parent) not in (".", "") else None
    return drive, parent, name, ext


def canonicalize_windows_path(s: str) -> str:
    raw = str(s)
    if raw.startswith("/mnt/") and len(raw) > 6 and raw[6] == "/":
        drive = raw[5].upper()
        rest = raw.split(f"/mnt/{raw[5]}/", 1)[1].replace("/", "\\")
        p = f"{drive}:\\{rest}"
    else:
        p = raw.replace("/", "\\")
    if len(p) >= 2 and p[1] == ":":
        p = p[0].upper() + p[1:]
    return p


def windows_to_wsl_path(s: str) -> str:
    m = re.match(r"^([A-Za-z]):(?:[\\/](.*))?$", str(s))
    if not m:
        return str(s)
    drive = m.group(1).lower()
    rest = (m.group(2) or "").replace("\\", "/")
    return f"/mnt/{drive}/{rest}" if rest else f"/mnt/{drive}"


def wsl_to_windows_path(s: str) -> str:
    s = str(s)
    if s.startswith("/mnt/") and len(s) > 6 and s[6] == "/":
        drive = s[5].upper()
        rest = s.split(f"/mnt/{s[5]}/", 1)[1].replace("/", "\\")
        return f"{drive}:\\{rest}"
    return canonicalize_windows_path(s)


def as_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
    return default


def parse_json_arg(v: str | None, fallback: Any) -> Any:
    if v is None:
        return fallback
    s = v.strip()
    if not s:
        return fallback
    try:
        return json.loads(s)
    except Exception as e:
        raise SystemExit(f"invalid JSON arg: {e}")


def strip_quotes(s: str) -> str:
    t = s.strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1]
    return t


def parse_simple_yaml_lists(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    data: dict[str, Any] = {}
    current_list_key: str | None = None
    lines = path.read_text(encoding="utf-8").splitlines()
    for i, raw in enumerate(lines, start=1):
        line = raw.rstrip()
        if not line.strip():
            continue
        if line.lstrip().startswith("#"):
            continue
        m_key_list = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*$", line)
        if m_key_list:
            key = m_key_list.group(1)
            current_list_key = key
            if key not in data:
                data[key] = []
            if not isinstance(data[key], list):
                raise SystemExit(f"invalid YAML at {path}:{i}: key '{key}' used as both scalar and list")
            continue
        m_key_value = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+?)\s*$", line)
        if m_key_value:
            key = m_key_value.group(1)
            value = strip_quotes(m_key_value.group(2))
            if value.isdigit():
                data[key] = int(value)
            else:
                data[key] = value
            current_list_key = None
            continue
        m_list_item = re.match(r"^\s*-\s*(.+?)\s*$", line)
        if m_list_item and current_list_key:
            value = strip_quotes(m_list_item.group(1))
            lst = data.get(current_list_key)
            if not isinstance(lst, list):
                raise SystemExit(f"invalid YAML at {path}:{i}: list item outside list key")
            lst.append(value)
            continue
        raise SystemExit(f"invalid YAML syntax at {path}:{i}: {line}")
    return data


def ensure_exts(exts: list[str] | None) -> list[str]:
    vals = exts or DEFAULT_EXTENSIONS
    out: list[str] = []
    for e in vals:
        s = str(e or "").strip().lower()
        if not s:
            continue
        if not s.startswith("."):
            s = "." + s
        out.append(s)
    return sorted(set(out)) or DEFAULT_EXTENSIONS


@dataclass
class ScannedFile:
    wsl_path: str
    win_path: str
    drive: str | None
    dir: str | None
    name: str | None
    ext: str | None
    size: int
    mtime_utc: str | None
    corrupt_candidate: bool
    corrupt_reason: str | None


def safe_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def read_head_ok(path: Path, read_bytes: int) -> tuple[bool, str | None]:
    try:
        with path.open("rb") as f:
            _ = f.read(max(1, read_bytes))
        return True, None
    except Exception as e:
        return False, str(e)


def scan_files_with_windows_fallback(
    unresolved_dirs_wsl: list[str],
    exts: set[str],
    detect_corruption: bool,
    read_bytes: int,
    windows_ops_root: str,
) -> tuple[list[ScannedFile], list[str], int]:
    warnings: list[str] = []
    if not unresolved_dirs_wsl:
        return [], warnings, 0

    scripts_root = Path(windows_ops_root) / "scripts"
    enum_script = scripts_root / "enumerate_files_jsonl.ps1"
    if not enum_script.exists():
        warnings.append(f"windows fallback unavailable: script missing: {enum_script}")
        return [], warnings, 1

    roots_win = [canonicalize_windows_path(wsl_to_windows_path(p)) for p in unresolved_dirs_wsl]
    exts_json = json.dumps(sorted(exts), ensure_ascii=False)
    roots_json = json.dumps(roots_win, ensure_ascii=False)
    try:
        rows = run_pwsh_jsonl(
            str(enum_script),
            [
                "-RootsJson",
                roots_json,
                "-ExtensionsJson",
                exts_json,
                f"-DetectCorruption:{'$true' if detect_corruption else '$false'}",
                "-CorruptionReadBytes",
                str(max(1, int(read_bytes))),
            ],
            normalize_args=False,
        )
    except Exception as e:
        warnings.append(f"windows fallback invocation failed: {e}")
        return [], warnings, 1

    files: list[ScannedFile] = []
    fallback_error_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if "_meta" in row or "_meta_end" in row:
            continue
        kind = str(row.get("kind") or "file")
        if kind == "warning":
            fallback_error_count += 1
            warnings.append(
                "windows enumerate warning: "
                + f"root={row.get('root')} code={row.get('code')} path={row.get('path')} :: {row.get('message')}"
            )
            continue
        if kind != "file":
            continue
        try:
            win_path = canonicalize_windows_path(str(row.get("path") or ""))
            if not win_path:
                raise ValueError("missing path")
            drive, dir_, name, ext2 = split_win(win_path)
            files.append(
                ScannedFile(
                    wsl_path=windows_to_wsl_path(win_path),
                    win_path=win_path,
                    drive=drive,
                    dir=dir_,
                    name=name,
                    ext=ext2,
                    size=int(row.get("size") or 0),
                    mtime_utc=str(row.get("mtimeUtc") or "") or None,
                    corrupt_candidate=bool(row.get("corruptCandidate")),
                    corrupt_reason=(str(row.get("corruptReason")) if row.get("corruptReason") is not None else None),
                )
            )
        except Exception as e:
            fallback_error_count += 1
            warnings.append(f"windows enumerate parse failed: row={safe_json(row)} :: {e}")
    return files, warnings, fallback_error_count


def collect_scanned_file(path: Path, exts: set[str], detect_corruption: bool, read_bytes: int, warnings: list[str]) -> ScannedFile | None:
    ext = path.suffix.lower()
    if ext not in exts:
        return None
    try:
        st = path.stat()
    except Exception as e:
        warnings.append(f"stat failed: {path} :: {e}")
        return None

    mtime = datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()
    win_path = canonicalize_windows_path(wsl_to_windows_path(str(path)))
    drive, dir_, name, ext2 = split_win(win_path)
    corrupt = False
    reason = None
    if detect_corruption:
        if int(st.st_size) == 0:
            corrupt = True
            reason = "size_zero"
        else:
            ok, err = read_head_ok(path, read_bytes)
            if not ok:
                corrupt = True
                reason = f"read_failed:{err}"
    return ScannedFile(
        wsl_path=str(path),
        win_path=win_path,
        drive=drive,
        dir=dir_,
        name=name,
        ext=ext2,
        size=int(st.st_size),
        mtime_utc=mtime,
        corrupt_candidate=corrupt,
        corrupt_reason=reason,
    )


def scan_files(
    roots: list[str],
    exts: set[str],
    detect_corruption: bool,
    read_bytes: int,
    scan_retry_count: int,
    windows_ops_root: str | None = None,
) -> tuple[list[ScannedFile], list[str], list[str], dict[str, Any]]:
    files: list[ScannedFile] = []
    warnings: list[str] = []
    errors: list[str] = []
    fallback_stats: dict[str, Any] = {
        "windowsFallbackUsed": False,
        "windowsFallbackDirs": 0,
        "windowsFallbackFiles": 0,
        "windowsFallbackErrorCount": 0,
    }
    seen: set[str] = set()
    for root_raw in roots:
        root = Path(root_raw)
        if not root.exists():
            errors.append(f"root missing: {root_raw}")
            continue
        if not root.is_dir():
            errors.append(f"root is not a directory: {root_raw}")
            continue

        failed_dirs: set[str] = set()

        def _collect_file(path: Path) -> None:
            row = collect_scanned_file(path, exts, detect_corruption, read_bytes, warnings)
            if row is None:
                return
            key = normalize_win_for_id(row.win_path)
            if key in seen:
                return
            seen.add(key)
            files.append(row)

        def _on_walk_error(e: OSError) -> None:
            failed_path = str(getattr(e, "filename", "") or "")
            if failed_path:
                failed_dirs.add(failed_path)
            warnings.append(f"walk failed: root={root_raw} path={failed_path or '?'} :: {e}")

        for dirpath, _dirnames, filenames in os.walk(str(root), onerror=_on_walk_error):
            for fname in filenames:
                _collect_file(Path(dirpath) / fname)

        unresolved = set(failed_dirs)
        retries = max(0, int(scan_retry_count))
        for attempt in range(1, retries + 1):
            if not unresolved:
                break
            attempt_failed: set[str] = set()
            attempt_targets = sorted(unresolved)
            unresolved = set()
            for target in attempt_targets:
                target_path = Path(target)
                if not target_path.exists() or not target_path.is_dir():
                    warnings.append(f"walk retry skipped: root={root_raw} path={target} reason=not_directory")
                    continue

                def _on_retry_error(e: OSError) -> None:
                    retry_failed_path = str(getattr(e, "filename", "") or target)
                    attempt_failed.add(retry_failed_path)
                    warnings.append(
                        f"walk retry failed: root={root_raw} path={retry_failed_path} attempt={attempt}/{retries} :: {e}"
                    )

                for dirpath, _dirnames, filenames in os.walk(str(target_path), onerror=_on_retry_error):
                    for fname in filenames:
                        _collect_file(Path(dirpath) / fname)
            unresolved = attempt_failed

        if unresolved:
            warnings.append(f"walk unresolved: root={root_raw} dirs={len(unresolved)}")
            if windows_ops_root:
                fallback_stats["windowsFallbackUsed"] = True
                fallback_stats["windowsFallbackDirs"] = int(fallback_stats["windowsFallbackDirs"]) + len(unresolved)
                fallback_files, fallback_warnings, fallback_error_count = scan_files_with_windows_fallback(
                    unresolved_dirs_wsl=sorted(unresolved),
                    exts=exts,
                    detect_corruption=detect_corruption,
                    read_bytes=read_bytes,
                    windows_ops_root=windows_ops_root,
                )
                warnings.extend(fallback_warnings)
                fallback_stats["windowsFallbackErrorCount"] = (
                    int(fallback_stats["windowsFallbackErrorCount"]) + int(fallback_error_count)
                )
                for row in fallback_files:
                    key = normalize_win_for_id(row.win_path)
                    if key in seen:
                        continue
                    seen.add(key)
                    files.append(row)
                    fallback_stats["windowsFallbackFiles"] = int(fallback_stats["windowsFallbackFiles"]) + 1

    return files, warnings, errors, fallback_stats
