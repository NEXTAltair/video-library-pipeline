"""Parse EDCB .program.txt companion files.

EDCB/EpgTimer records a .program.txt next to each .ts recording.
This module extracts structured metadata from that text file.

Format (UTF-8 with BOM):
  Line 1: "2026/02/25(水) 22:45～23:35"          ← date/time range
  Line 2: "ＮＨＫ　ＢＳ"                           ← broadcaster (full-width)
  Line 3: "番組タイトル[二][字]"                     ← title with annotations
  Line 4: (blank)
  Line 5+: description
  ...
  "詳細情報" section (optional)
  "ジャンル :" section
  Technical info (映像, 音声, etc.)
  OriginalNetworkID / TransportStreamID / ServiceID / EventID
"""

from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path
from typing import Any


# ── full-width → half-width normalization ──────────────────────
def _nfkc(s: str) -> str:
    """NFKC normalize (full-width → half-width for alphanumeric/symbols)."""
    return unicodedata.normalize("NFKC", s)


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


# ── line 1: date/time range ───────────────────────────────────
_RE_DATETIME_RANGE = re.compile(
    r"(\d{4})/(\d{2})/(\d{2})\([^)]*\)\s*(\d{1,2}):(\d{2})"
    r"[～~]"
    r"(\d{1,2}):(\d{2})"
)


def _parse_datetime_range(line: str) -> dict[str, Any] | None:
    line_h = _nfkc(line)
    m = _RE_DATETIME_RANGE.search(line_h)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    sh, sm = int(m.group(4)), int(m.group(5))
    eh, em = int(m.group(6)), int(m.group(7))
    # EDCB uses 24-29 for late-night (e.g. 25:00 = next day 01:00)
    air_date = f"{y}-{mo}-{d}"
    start_time = f"{sh:02d}:{sm:02d}"
    end_time = f"{eh:02d}:{em:02d}"
    return {
        "air_date": air_date,
        "start_time": start_time,
        "end_time": end_time,
        "start_hour": sh,
        "start_minute": sm,
    }


# ── line 3: title with annotations ────────────────────────────
_RE_ANNOTATIONS = re.compile(r"\[([^\]]*)\]")
_KNOWN_ANNOTATIONS = {"字", "二", "双", "デ", "S", "B", "N", "多", "解", "新", "再", "終", "無料", "手", "映"}


def _parse_title_line(line: str) -> dict[str, Any]:
    line_h = _nfkc(line).strip()
    annotations: list[str] = []
    for m in _RE_ANNOTATIONS.finditer(line_h):
        annotations.append(m.group(1))
    clean_title = _RE_ANNOTATIONS.sub("", line_h).strip()
    is_rebroadcast = "再" in annotations
    return {
        "official_title": clean_title,
        "title_raw": line.strip(),
        "annotations": annotations,
        "is_rebroadcast_flag": is_rebroadcast,
    }


# ── genre section ─────────────────────────────────────────────
_RE_GENRE_LINE = re.compile(r"^(.+?)\s*-\s*(.+)$")


def _parse_genres(lines: list[str]) -> list[dict[str, str]]:
    genres: list[dict[str, str]] = []
    for line in lines:
        line = _nfkc(line).strip()
        if not line:
            continue
        m = _RE_GENRE_LINE.match(line)
        if m:
            genres.append({"category": m.group(1).strip(), "subcategory": m.group(2).strip()})
        elif line and line not in ("ジャンル :", "ジャンル:"):
            genres.append({"category": line, "subcategory": ""})
    return genres


# ── network IDs ───────────────────────────────────────────────
_RE_NET_ID = re.compile(r"^(OriginalNetworkID|TransportStreamID|ServiceID|EventID):(\d+)")


def _parse_network_ids(lines: list[str]) -> dict[str, int]:
    ids: dict[str, int] = {}
    for line in lines:
        line_h = _nfkc(line).strip()
        m = _RE_NET_ID.match(line_h)
        if m:
            ids[m.group(1)] = int(m.group(2))
    return ids


# ── main parser ───────────────────────────────────────────────
def parse_program_txt(path: Path) -> dict[str, Any] | None:
    """Parse a single EDCB .program.txt file into a structured dict.

    Returns None if the file cannot be parsed.
    """
    try:
        text = path.read_text(encoding="utf-8-sig")
    except Exception:
        return None

    lines = text.splitlines()
    if len(lines) < 3:
        return None

    # Line 1: date/time
    dt = _parse_datetime_range(lines[0])
    if not dt:
        return None

    # Line 2: broadcaster
    broadcaster_raw = lines[1].strip()
    broadcaster = _nfkc(broadcaster_raw).strip()

    # Line 3: title
    title_info = _parse_title_line(lines[2])

    # Lines 4+: description (up to "詳細情報" or "ジャンル")
    desc_lines: list[str] = []
    detail_lines: list[str] = []
    genre_lines: list[str] = []
    netid_lines: list[str] = []

    section = "description"
    for line in lines[3:]:
        stripped = _nfkc(line).strip()

        if stripped == "詳細情報" or stripped == "詳細情報":
            section = "detail"
            continue
        if stripped.startswith("ジャンル") and ":" in stripped:
            section = "genre"
            continue
        if stripped.startswith("映像 :") or stripped.startswith("映像:"):
            section = "technical"
            continue
        if _RE_NET_ID.match(stripped):
            section = "netid"
            netid_lines.append(line)
            continue

        if section == "description":
            desc_lines.append(line.strip())
        elif section == "detail":
            detail_lines.append(line)
        elif section == "genre":
            genre_lines.append(line)
        elif section == "netid":
            netid_lines.append(line)

    description = "\n".join(desc_lines).strip()
    genres = _parse_genres(genre_lines)
    network_ids = _parse_network_ids(netid_lines)

    # Extract detail sub-sections
    detail_sections: dict[str, str] = {}
    current_key: str | None = None
    current_val: list[str] = []
    for line in detail_lines:
        stripped = _nfkc(line).strip()
        if stripped.startswith("- "):
            if current_key and current_val:
                detail_sections[current_key] = "\n".join(current_val).strip()
            current_key = stripped[2:].strip()
            current_val = []
        elif current_key is not None:
            current_val.append(stripped)
    if current_key and current_val:
        detail_sections[current_key] = "\n".join(current_val).strip()

    return {
        "air_date": dt["air_date"],
        "start_time": dt["start_time"],
        "end_time": dt["end_time"],
        "start_hour": dt["start_hour"],
        "start_minute": dt["start_minute"],
        "broadcaster": broadcaster,
        "broadcaster_raw": broadcaster_raw,
        "official_title": title_info["official_title"],
        "title_raw": title_info["title_raw"],
        "annotations": title_info["annotations"],
        "is_rebroadcast_flag": title_info["is_rebroadcast_flag"],
        "description": description,
        "epg_genres": genres,
        "detail_sections": detail_sections,
        "network_ids": network_ids,
        "source_file": str(path),
    }


# ── match key generation ──────────────────────────────────────

_WS = re.compile(r"[\s\u3000]+")
_BAD = re.compile(r"[<>:\"/\\|?*]")
_DATE_PATTERNS = [
    re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})"),
    re.compile(r"(\d{4})-(\d{2})-(\d{2})-(\d{2})[：:](\d{2})"),
    re.compile(r"(\d{4}) (\d{2}) (\d{2}) (\d{2}) (\d{2})"),
    re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})"),
]


def _extract_date_time_from_filename(filename: str) -> tuple[str | None, str | None]:
    """Extract air_date and start_time from a filename."""
    name = _nfkc(filename)
    for pat in _DATE_PATTERNS:
        m = pat.search(name)
        if m:
            y, mo, d, h, mi = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
            # Handle compact 4-digit hour+minute (e.g. "2100" for 21:00)
            if len(h) == 4:
                mi = h[2:]
                h = h[:2]
            return f"{y}-{mo}-{d}", f"{int(h):02d}:{int(mi):02d}"
    return None, None


def _extract_base_title_from_filename(filename: str) -> str:
    """Extract the program title portion from a filename, stripping date/ext."""
    name = _nfkc(os.path.splitext(filename)[0])
    # Remove date/time suffix
    for pat in _DATE_PATTERNS:
        m = pat.search(name)
        if m:
            name = name[: m.start()].rstrip(" _-")
            break
    # Normalize whitespace
    name = _WS.sub(" ", name).strip()
    return name


def match_key_from_filename(filename: str) -> str | None:
    """Generate a match key from a filename for correlating TS and encoded files.

    Key format: normalized_title::YYYY-MM-DD::HH:MM
    Returns None if date/time cannot be extracted.
    """
    air_date, start_time = _extract_date_time_from_filename(filename)
    if not air_date or not start_time:
        return None
    base = _extract_base_title_from_filename(filename)
    if not base:
        return None
    # Normalize: lowercase, strip bad chars, collapse whitespace
    key = _BAD.sub("", base.lower())
    key = _WS.sub("_", key).strip("_")
    return f"{key}::{air_date}::{start_time}"


def match_key_from_epg(epg: dict[str, Any]) -> str | None:
    """Generate a match key from parsed EPG metadata.

    Key format: normalized_title::broadcaster_norm::YYYY-MM-DD::HH:MM
    Includes broadcaster to avoid collisions when the same title airs
    on different channels at the same time (e.g. terrestrial / one-seg).
    """
    air_date = epg.get("air_date")
    start_time = epg.get("start_time")
    title = epg.get("official_title", "")
    broadcaster = epg.get("broadcaster", "")
    if not air_date or not start_time or not title:
        return None
    key = _BAD.sub("", _nfkc(title).lower())
    key = _WS.sub("_", key).strip("_")
    bc = _BAD.sub("", _nfkc(broadcaster).lower()) if broadcaster else ""
    bc = _WS.sub("_", bc).strip("_") if bc else ""
    return f"{key}::{bc}::{air_date}::{start_time}"


def datetime_key_from_filename(filename: str) -> str | None:
    """Secondary match key using only date+time (for fuzzy title matching).

    Returns "YYYY-MM-DD::HH:MM" or None.
    """
    air_date, start_time = _extract_date_time_from_filename(filename)
    if not air_date or not start_time:
        return None
    return f"{air_date}::{start_time}"


def datetime_key_from_epg(epg: dict[str, Any]) -> str | None:
    """Secondary match key using only date+time from EPG data."""
    air_date = epg.get("air_date")
    start_time = epg.get("start_time")
    if not air_date or not start_time:
        return None
    return f"{air_date}::{start_time}"
