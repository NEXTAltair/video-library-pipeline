#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from typing import Any

WIN_PWSH_CANDIDATES = [
    "/mnt/c/Program Files/PowerShell/7/pwsh.exe",
    "pwsh.exe",
]


def is_wsl_mnt_path(s: str) -> bool:
    return str(s).startswith("/mnt/") and len(str(s)) > 6 and str(s)[6] == "/"


def wsl_path_to_win_str(s: str) -> str:
    p = str(s)
    if not is_wsl_mnt_path(p):
        return p
    drive = p[5].upper()
    rest = p.split(f"/mnt/{p[5]}/", 1)[1].replace("/", "\\")
    return f"{drive}:\\" + rest


def canonicalize_windows_path(s: str) -> str:
    p = wsl_path_to_win_str(str(s)).replace("/", "\\")
    if len(p) >= 2 and p[1] == ":":
        return p[0].upper() + p[1:]
    return p


def _normalize_arg_for_pwsh(arg: str) -> str:
    return canonicalize_windows_path(arg)


def find_pwsh7_executable() -> str:
    for exe in WIN_PWSH_CANDIDATES:
        try:
            cp = subprocess.run(
                [exe, "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except FileNotFoundError:
            continue
        if cp.returncode == 0:
            return exe
    raise RuntimeError(f"pwsh.exe not found (tried: {', '.join(WIN_PWSH_CANDIDATES)})")


def run_pwsh_file(file_win_or_wsl: str, args: list[str], *, normalize_args: bool = True) -> str:
    file_path = wsl_path_to_win_str(file_win_or_wsl)
    norm_args = [_normalize_arg_for_pwsh(a) for a in args] if normalize_args else [str(a) for a in args]
    exe = find_pwsh7_executable()
    cmd = [exe, "-NoProfile", "-File", file_path, *norm_args]
    try:
        cp = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
        )
    except FileNotFoundError:
        # Defensive: executable was found during probe but disappeared before invocation.
        raise RuntimeError(f"pwsh executable vanished before invocation: {exe}")
    # Do not normalize PowerShell output text here.
    # JSON/JSONL payloads can contain exact filesystem paths, and Unicode normalization
    # (e.g. NFKC) can change code points and cause false `src_not_found` failures.
    stdout = cp.stdout or ""
    stderr = cp.stderr or ""
    if cp.returncode == 0:
        return stdout
    msg_parts = [p for p in [stdout.strip(), stderr.strip()] if p]
    details = "\n".join(msg_parts) if msg_parts else f"pwsh failed rc={cp.returncode}"
    raise RuntimeError(f"{details} (exe={exe})")


def run_pwsh_json(file_win_or_wsl: str, args: list[str], *, normalize_args: bool = True) -> dict[str, Any]:
    raw = run_pwsh_file(file_win_or_wsl, args, normalize_args=normalize_args)
    for line in reversed(raw.splitlines()):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            return obj
    raise RuntimeError("pwsh json parse failed: no JSON object found in stdout")


def run_pwsh_jsonl(file_win_or_wsl: str, args: list[str], *, normalize_args: bool = True) -> list[dict[str, Any]]:
    raw = run_pwsh_file(file_win_or_wsl, args, normalize_args=normalize_args)
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows
