#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import Path, PureWindowsPath
from typing import Any

from .title_text import clean_title_edges

FORB = re.compile(r'[<>:"/\\|?*]')
CTRL = re.compile(r"[\x00-\x1f]")
TRAIL = re.compile(r"[\. ]+$")
WS = re.compile(r"[\s\u3000]+")
DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}
SWALLOWED_TITLE_THRESHOLD = 8

# ▽/▼/◇/「 がprogram_titleに含まれる場合、サブタイトル混入の可能性が高い
SUBTITLE_SEPARATORS = re.compile(r"[▽▼◇「]")

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
    """program_titleにサブタイトル区切り文字(▽▼◇「)が含まれるかチェック"""
    return bool(SUBTITLE_SEPARATORS.search(program_title or ""))


def clean_program_title(title: str) -> str:
    """最初のサブタイトル区切り文字で分割し、前半を返す。"""
    return SUBTITLE_SEPARATORS.split(str(title or ""), maxsplit=1)[0].strip()


def normalize_title_for_comparison(s: str) -> str:
    """Normalize a title for comparison (casefold + NFKC)."""
    return unicodedata.normalize("NFKC", str(s or "")).casefold().strip()


def legacy_safe_dir_name(name: str, maxlen: int = 60) -> str:
    """Old escaping, only for discovering pre-existing library roots."""
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


def safe_dir_name(name: str, maxlen: int = 60) -> str:
    """Readable Windows-safe folder component, never a change to recording names."""
    s = CTRL.sub("", clean_title_edges(name))
    s = s.translate(str.maketrans({'?': '？', '<': '＜', '>': '＞', ':': '：',
                                 '"': '', '*': '＊', '/': '／', '\\': '＼', '|': '｜'}))
    s = WS.sub(" ", s)
    s = re.sub(r"^[\s_＿.]+|[\s_＿.]+$", "", s)
    if not s:
        s = "UNKNOWN"
    if len(s) > maxlen:
        h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]
        s = s[:maxlen - 9].rstrip() + "_" + h
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
        self.layout_rules: list[LayoutRule] = _parse_layout_rules(data.get("layout_rules"), self.genre)
        self._layout_by_program: dict[str, str] = {}
        for rule in self.layout_rules:
            for program in rule.programs:
                key = normalize_title_for_comparison(program)
                existing = self._layout_by_program.get(key)
                if existing is not None and existing != rule.layout:
                    raise ValueError(f"route {self.genre}: program listed under two layouts: {program}")
                self._layout_by_program[key] = rule.layout

    def layout_for(self, md: dict[str, Any]) -> str:
        """Layout inside this route's dest_root.

        An explicit program list wins so one program never splits across
        layouts when its per-episode EPG genre drifts. Otherwise only the
        primary (first) EPG genre is consulted; secondary genres are too noisy.
        """
        if not self.layout_rules:
            return self.layout
        title = normalize_title_for_comparison(str(md.get("program_title") or ""))
        listed = self._layout_by_program.get(title)
        if listed:
            return listed
        primary = _primary_epg_genre(md)
        if primary:
            for rule in self.layout_rules:
                if any(_epg_genre_matches(primary, pattern) for pattern in rule.epg_genre_match):
                    return rule.layout
        return self.layout


KNOWN_LAYOUTS = {"by_program_year_month", "by_syllabary", "by_title", "flat"}


class LayoutRule:
    """Per-program / per-primary-EPG-genre layout inside one genre route."""

    def __init__(self, layout: str, programs: tuple[str, ...], epg_genre_match: tuple[str, ...]):
        self.layout = layout
        self.programs = programs
        self.epg_genre_match = epg_genre_match


def _string_list(value: Any, field: str, where: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(v, str) or not v.strip() for v in value):
        raise ValueError(f"{where}: {field} must be a list of nonempty strings")
    return tuple(v.strip() for v in value)


def _parse_layout_rules(raw: Any, genre: str) -> list[LayoutRule]:
    if raw is None:
        return []
    where = f"route {genre} layout_rules"
    if not isinstance(raw, list):
        raise ValueError(f"{where} must be a list")
    rules: list[LayoutRule] = []
    for item in raw:
        if not isinstance(item, dict) or not isinstance(item.get("layout"), str):
            raise ValueError(f"{where}: each rule needs a layout")
        layout = normalize_layout_name(item["layout"])
        if layout not in KNOWN_LAYOUTS:
            raise ValueError(f"{where}: unknown layout {item['layout']}")
        programs = _string_list(item.get("programs"), "programs", where)
        patterns = _string_list(item.get("epg_genre_match"), "epg_genre_match", where)
        if not programs and not patterns:
            raise ValueError(f"{where}: rule for {layout} needs programs or epg_genre_match")
        rules.append(LayoutRule(layout, programs, patterns))
    return rules


def _primary_epg_genre(md: dict[str, Any]) -> str:
    epg_genres = md.get("epg_genres")
    if isinstance(epg_genres, list) and epg_genres:
        g = epg_genres[0]
        if isinstance(g, dict):
            cat = str(g.get("category") or "")
            sub = str(g.get("subcategory") or "")
            return f"{cat} - {sub}" if cat and sub else cat
        if isinstance(g, str):
            return g
    return str(md.get("epg_genre") or "")


class WorkRoute(DriveRoute):
    """Explicit work identities change storage only, never genre or title."""

    def __init__(self, data: dict[str, Any]):
        if not isinstance(data, dict):
            raise ValueError("work route must be a mapping")
        works = data.get("works")
        root = data.get("dest_root")
        if not isinstance(works, list) or not works or any(not isinstance(w, str) or not w.strip() for w in works):
            raise ValueError("work route requires a nonempty list of work titles")
        if not isinstance(root, str) or not PureWindowsPath(root).is_absolute() or ".." in PureWindowsPath(root).parts:
            raise ValueError("work route requires an absolute Windows dest_root")
        if data.get("layout", "by_title") != "by_title":
            raise ValueError("work route layout must be by_title")
        super().__init__({**data, "layout": "by_title"})
        self.works = tuple(w.strip() for w in works)
        self.root = PureWindowsPath(root)


class DriveRoutes:
    """Multi-destination routing configuration."""

    def __init__(self, default_dest: str, default_layout: str, routes: list[DriveRoute], movie_grouping=None, work_routes=None):
        self.default_dest = default_dest.rstrip("\\")
        self.default_layout = default_layout
        self.routes = routes
        self.movie_grouping = movie_grouping
        if work_routes is None:
            work_routes = []
        if not isinstance(work_routes, list):
            raise ValueError("work_routes must be a list")
        self.work_routes = [WorkRoute(data) for data in work_routes]
        self._work_routes_by_title: dict[str, tuple[WorkRoute, str]] = {}
        for route in self.work_routes:
            for title in route.works:
                key = normalize_title_for_comparison(title)
                if key in self._work_routes_by_title:
                    raise ValueError("duplicate normalized work route title: " + title)
                self._work_routes_by_title[key] = (route, title)

    def resolve_work(self, title: str) -> tuple[WorkRoute, str] | None:
        return self._work_routes_by_title.get(normalize_title_for_comparison(title))

    def contains_grouped_source(self, source: str) -> bool:
        path = PureWindowsPath(source)
        return any(path.is_relative_to(route.root) for route in self.work_routes)

    def resolve(self, md: dict[str, Any]) -> tuple[DriveRoute | None, str]:
        """Find the matching route for given metadata.

        Returns (route, reason). route is None if no match → use default.
        """
        # 0. Pre-resolved genre fast-path
        pre = md.get("genre")
        if pre:
            # Metadata extraction uses Japanese labels; route ids are stable
            # English keys. Do not discard an explicit, resolved genre.
            pre = {"アニメ": "anime", "特撮": "tokusatsu", "映画": "movie", "ドラマ": "drama",
                   "ドキュメンタリー・情報": "documentary", "バラエティ": "variety",
                   "ニュース・報道": "news", "放送大学": "education"}.get(str(pre), pre)
            for route in self.routes:
                if route.genre == pre:
                    return route, f"pre_resolved_genre:{pre}"

        # Unambiguous EPG categories (e.g. movie) retain priority. The shared
        # anime/tokusatsu category is a fallback, not proof of anime.
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

        shared_category_match = None
        for route in self.routes:
            for pattern in route.epg_genre_match:
                for gs in genre_strings:
                    if _epg_genre_matches(gs, pattern):
                        if pattern in {"アニメ/特撮 - *", "アニメ/特撮"}:
                            if shared_category_match is None:
                                shared_category_match = (route, f"epg_genre:{pattern}")
                            continue
                        return route, f"epg_genre:{pattern}"

        title_lower = normalize_title_for_comparison(str(md.get("program_title") or ""))
        for route in self.routes:
            for pattern in route.title_patterns:
                if pattern and normalize_title_for_comparison(pattern) in title_lower:
                    return route, f"title_pattern:{pattern}"
        if shared_category_match:
            return shared_category_match
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
    work_routes = data.get("work_routes", [])
    if not isinstance(work_routes, list):
        raise ValueError("work_routes must be a list")

    movie_grouping = None
    if data.get("movie_grouping_path"):
        from .movie_grouping import MovieGrouping
        from video_pipeline.platform.pathscan_common import windows_to_wsl_path
        grouping_path = Path(windows_to_wsl_path(str(data["movie_grouping_path"])).replace("\\", "/"))
        if not grouping_path.is_absolute():
            grouping_path = p.parent / grouping_path
        movie_grouping = MovieGrouping(_load_yaml_file(grouping_path))
    return DriveRoutes(default_dest=default_dest, default_layout=default_layout, routes=routes, movie_grouping=movie_grouping, work_routes=work_routes)


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
    genre = route.genre if route else "default"
    work_match = routes.resolve_work(str(md["program_title"]))
    if work_match:
        work_route, declared_title = work_match
        dst = work_route.root / safe_dir_name(declared_title, maxlen=80) / filename
        return str(dst), genre, None
    if routes.contains_grouped_source(src_path_win):
        return None, genre, "series_work_unmapped"
    explicit_genre = str(md.get("genre") or "").casefold()
    non_movie = bool(explicit_genre and explicit_genre not in ("movie", "映画"))
    animated_work = explicit_genre in ("anime", "アニメ", "アニメ/特撮")
    if routes.movie_grouping and (not non_movie or animated_work):
        from .movie_grouping import has_movie_evidence
        group = routes.movie_grouping.resolve_placement(
            str(md["program_title"]), filename,
            movie_evidence=has_movie_evidence(md) or bool(route and route.genre == "movie"),
            allow_frame_fallback=not non_movie)
        is_series_member = bool(group and not group.frame_fallback)
        movie_like = not non_movie and (bool(group) or has_movie_evidence(md) or bool(route and route.genre == "movie"))
        if not is_series_member and movie_like and routes.movie_grouping.work_layout_series_containing(src_path_win):
            # A per-work series keeps films that match no declared work where
            # the owner filed them, instead of sending them to the generic
            # movie route or a broadcast-frame folder.
            return str(PureWindowsPath(str(src_path_win))), "movie", None
        if group:
            group_genre = (route.genre if route else "anime") if non_movie else "movie"
            series_dir = PureWindowsPath(group.root.rstrip("\\") + "\\") / safe_dir_name(group.folder, maxlen=80)
            if group.work_folder and group.layout == "by_work":
                return str(series_dir / group.work_folder / filename), group_genre, None
            if group.work_folder:
                y, m, err = extract_year_month_from_air_date(md.get("air_date"))
                if err:
                    return None, group_genre, err
                return str(series_dir / group.work_folder / y / m / filename), group_genre, None
            return str(series_dir / filename), group_genre, None
    dest_root = route.dest_root if route else routes.default_dest
    layout = route.layout_for(md) if route else routes.default_layout
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
