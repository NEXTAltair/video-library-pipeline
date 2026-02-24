#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
from pathlib import PureWindowsPath
from typing import Any

FORB = re.compile(r'[<>:"/\\|?*]')
CTRL = re.compile(r"[\x00-\x1f]")
TRAIL = re.compile(r"[\. ]+$")
WS = re.compile(r"[\s\u3000]+")
DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}


def safe_dir_name(name: str, maxlen: int = 60) -> str:
    s = (name or "").strip()
    s = CTRL.sub("", s)
    s = FORB.sub("＿", s)
    s = WS.sub(" ", s)
    s = TRAIL.sub("", s)
    if not s:
        s = "UNKNOWN"
    if len(s) > maxlen:
        h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]
        s = s[: maxlen - 9].rstrip() + "_" + h
    return s


def has_required_db_contract(md: dict[str, Any]) -> bool:
    for k in DB_CONTRACT_REQUIRED:
        if k not in md:
            return False
    return isinstance(md.get("needs_review"), bool)


def extract_year_month_from_air_date(air_date: Any) -> tuple[str | None, str | None, str | None]:
    s = str(air_date or "").strip()
    if not s:
        return None, None, "missing_air_date"
    try:
        y, m, _ = s.split("-", 2)
    except Exception:
        return None, None, "invalid_air_date"
    if not (len(y) == 4 and y.isdigit() and len(m) == 2 and m.isdigit()):
        return None, None, "invalid_air_date"
    return y, m, None


def build_expected_dest_path(dest_root_win: str, src_path_win: str, md: dict[str, Any]) -> tuple[str | None, str | None]:
    if not isinstance(md, dict):
        return None, "invalid_metadata_contract"
    if not has_required_db_contract(md):
        return None, "invalid_metadata_contract"

    if not md.get("program_title"):
        return None, "missing_program_title"
    y, m, err = extract_year_month_from_air_date(md.get("air_date"))
    if err:
        return None, err

    prog_dir = safe_dir_name(str(md.get("program_title") or ""))
    filename = PureWindowsPath(str(src_path_win)).name
    if not filename:
        return None, "missing_filename"
    dst = str(dest_root_win).rstrip("\\") + f"\\{prog_dir}\\{y}\\{m}\\{filename}"
    return dst, None
