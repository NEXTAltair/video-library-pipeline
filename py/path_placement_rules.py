#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import Path, PureWindowsPath
from typing import Any

FORB = re.compile(r'[<>:"/\\|?*]')
CTRL = re.compile(r"[\x00-\x1f]")
TRAIL = re.compile(r"[\. ]+$")
WS = re.compile(r"[\s\u3000]+")
DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}
SWALLOWED_TITLE_THRESHOLD = 8

# ▽/▼/◇ がprogram_titleに含まれる場合、サブタイトル混入の可能性が高い
SUBTITLE_SEPARATORS = re.compile(r"[▽▼◇]")

TITLE_RELATED_REASONS = {
    "needs_review_flagged",
    "program_title_may_include_description",
    "suspicious_program_title",
    "relocate_suspicious_program_title",
    "suspicious_program_title_shortened",
    "relocate_suspicious_program_title_shortened",
    "subtitle_separator_in_program_title",
    "relocate_subtitle_separator_in_program_title",
}


def detect_subtitle_in_program_title(program_title: str) -> bool:
    """program_titleにサブタイトル区切り文字(▽▼◇)が含まれるかチェック"""
    return bool(SUBTITLE_SEPARATORS.search(program_title or ""))


def normalize_title_for_comparison(s: str) -> str:
    """Normalize a title for comparison (casefold + NFKC)."""
    return unicodedata.normalize("NFKC", str(s or "")).casefold().strip()


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
    """Legacy single-dest path builder (backward compatible)."""
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


# ── Drive Routes (multi-destination routing) ─────────────────


def _load_yaml_file(yaml_path: Path) -> dict[str, Any]:
    """Load YAML file using PyYAML."""
    import yaml

    with yaml_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


class DriveRoute:
    """A single genre → destination route."""

    def __init__(self, data: dict[str, Any]):
        self.genre: str = str(data.get("genre") or "")
        self.dest_root: str = str(data.get("dest_root") or "").rstrip("\\")
        self.layout: str = normalize_layout_name(str(data.get("layout") or "by_program_year_month"))
        self.epg_genre_match: list[str] = data.get("epg_genre_match") or []
        self.title_patterns: list[str] = data.get("title_patterns") or []


class DriveRoutes:
    """Multi-destination routing configuration."""

    def __init__(self, default_dest: str, default_layout: str, routes: list[DriveRoute]):
        self.default_dest = default_dest.rstrip("\\")
        self.default_layout = default_layout
        self.routes = routes

    def resolve(self, md: dict[str, Any]) -> tuple[DriveRoute | None, str]:
        """Find the matching route for given metadata.

        Returns (route, reason). route is None if no match → use default.
        """
        # 0. Pre-resolved genre fast-path
        pre = md.get("genre")
        if pre:
            for route in self.routes:
                if route.genre == pre:
                    return route, f"pre_resolved_genre:{pre}"

        # 1. Try EPG genre match
        epg_genres = md.get("epg_genres") or []
        epg_genre_str = md.get("epg_genre") or ""
        # Collect all genre strings
        genre_strings: list[str] = []
        if isinstance(epg_genres, list):
            for g in epg_genres:
                if isinstance(g, dict):
                    cat = g.get("category", "")
                    sub = g.get("subcategory", "")
                    if cat and sub:
                        genre_strings.append(f"{cat} - {sub}")
                    elif cat:
                        genre_strings.append(cat)
                elif isinstance(g, str):
                    genre_strings.append(g)
        if epg_genre_str:
            genre_strings.append(str(epg_genre_str))

        for route in self.routes:
            for pattern in route.epg_genre_match:
                for gs in genre_strings:
                    if _epg_genre_matches(gs, pattern):
                        return route, f"epg_genre:{pattern}"

        # 2. Try title pattern match
        program_title = str(md.get("program_title") or "")
        title_lower = program_title.lower()
        for route in self.routes:
            for pattern in route.title_patterns:
                if pattern.lower() in title_lower:
                    return route, f"title_pattern:{pattern}"

        return None, "no_match"


def _epg_genre_matches(genre_str: str, pattern: str) -> bool:
    """Check if an EPG genre string matches a pattern (supports * wildcard)."""
    if pattern.endswith(" - *"):
        prefix = pattern[:-4]
        return genre_str.startswith(prefix)
    return genre_str == pattern


def load_drive_routes(yaml_path: str | Path) -> DriveRoutes:
    """Load drive routes from a YAML config file."""
    p = Path(yaml_path)
    if not p.exists():
        raise FileNotFoundError(f"drive_routes.yaml not found: {p}")

    data = _load_yaml_file(p)

    default_dest = str(data.get("default_dest") or "")
    default_layout = normalize_layout_name(str(data.get("default_layout") or "by_program_year_month"))
    routes = [DriveRoute(r) for r in data.get("routes", []) if r.get("genre")]

    return DriveRoutes(default_dest=default_dest, default_layout=default_layout, routes=routes)


# ── Layout builders ──────────────────────────────────────────


_SYLLABARY_RANGES = [
    ("ア", "ア", "オ"),  # ア行
    ("カ", "カ", "ゴ"),  # カ行
    ("サ", "サ", "ゾ"),  # サ行
    ("タ", "タ", "ド"),  # タ行
    ("ナ", "ナ", "ノ"),  # ナ行
    ("ハ", "ハ", "ポ"),  # ハ行
    ("マ", "マ", "モ"),  # マ行
    ("ヤ", "ヤ", "ヨ"),  # ヤ行
    ("ラ", "ラ", "ロ"),  # ラ行
    ("ワ", "ワ", "ン"),  # ワ行
]


def normalize_layout_name(layout: str) -> str:
    """Normalize layout aliases to canonical names."""
    if layout == "by_series":
        return "by_title"
    return layout


def _title_to_syllabary_folder(title: str) -> str:
    """Map a title to its Japanese syllabary category folder (ア, カ, サ...)."""
    if not title:
        return "MV"
    # Normalize to katakana for comparison
    first_char = title[0]
    # Check if it starts with ASCII/number
    if first_char.isascii():
        return "MV"
    # Convert hiragana to katakana if needed
    kata = unicodedata.normalize("NFKC", first_char)
    if "\u3040" <= kata <= "\u309f":  # hiragana range
        kata = chr(ord(kata) + 0x60)  # convert to katakana

    for folder, start, end in _SYLLABARY_RANGES:
        if start <= kata <= end:
            return folder

    # Kanji or other - try to detect common prefixes
    return "MV"


def build_routed_dest_path(
    routes: DriveRoutes,
    src_path_win: str,
    md: dict[str, Any],
) -> tuple[str | None, str | None, str | None]:
    """Build destination path using drive routes.

    Returns (dest_path, genre_route, error).
    """
    if not isinstance(md, dict):
        return None, None, "invalid_metadata_contract"
    if not has_required_db_contract(md):
        return None, None, "invalid_metadata_contract"
    if not md.get("program_title"):
        return None, None, "missing_program_title"

    filename = PureWindowsPath(str(src_path_win)).name
    if not filename:
        return None, None, "missing_filename"

    route, reason = routes.resolve(md)
    dest_root = route.dest_root if route else routes.default_dest
    layout = route.layout if route else routes.default_layout
    genre = route.genre if route else "default"

    if not dest_root:
        return None, genre, "no_dest_root"

    prog_title = str(md.get("program_title") or "")

    if layout == "by_program_year_month":
        y, m, err = extract_year_month_from_air_date(md.get("air_date"))
        if err:
            return None, genre, err
        prog_dir = safe_dir_name(prog_title)
        dst = f"{dest_root}\\{prog_dir}\\{y}\\{m}\\{filename}"

    elif layout == "by_syllabary":
        folder = _title_to_syllabary_folder(prog_title)
        dst = f"{dest_root}\\{folder}\\{filename}"

    elif layout == "by_title":
        # Use program_title as title folder name (legacy alias: by_series)
        series_dir = safe_dir_name(prog_title, maxlen=80)
        dst = f"{dest_root}\\{series_dir}\\{filename}"

    elif layout == "flat":
        dst = f"{dest_root}\\{filename}"

    else:
        # Fallback to by_program_year_month
        y, m, err = extract_year_month_from_air_date(md.get("air_date"))
        if err:
            return None, genre, err
        prog_dir = safe_dir_name(prog_title)
        dst = f"{dest_root}\\{prog_dir}\\{y}\\{m}\\{filename}"

    return dst, genre, None
