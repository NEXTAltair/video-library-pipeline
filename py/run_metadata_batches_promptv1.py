"""Re-extract metadata using the current prompt_v1 policy (LLM-style parsing rules)."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path, PurePath
from typing import Any

from edcb_program_parser import match_key_from_filename, datetime_key_from_filename
from db_helpers import latest_path_metadata, reconstruct_broadcast_data
from franchise_resolver import resolve_franchise
from genre_resolver import resolve_genre
from path_placement_rules import DB_CONTRACT_REQUIRED, SUBTITLE_SEPARATORS
from plan_validation import detect_swallowed_program_title
from source_history import make_entry
from pathscan_common import now_iso
from title_resolution import load_canonical_title_sources, suggest_canonical_title

WS = re.compile(r"[\s\u3000]+")
BAD = re.compile(r"[<>:\"/\\\\|?*]")
UND = re.compile(r"_+")

PAT_US = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})(?=\.[^.]+$)")
PAT_SP = re.compile(r"(\d{4}) (\d{2}) (\d{2}) (\d{2}) (\d{2})(?=\.[^.]+$)")
PAT_COL = re.compile(r"(\d{4}) (\d{2}) (\d{2}) (\d{2})[：:](\d{2})(?=\.[^.]+$)")
PAT_US_DUP = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})[-_]\(\d+\)(?=\.[^.]+$)")
PAT_US_PAREN = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})\(\d+\)(?=\.[^.]+$)")
PAT_HHMM = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{4})(?=\.[^.]+$)")
PAT_COMPACT_14 = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?=\.[^.]+$)")
PAT_COMPACT_12 = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(?=\.[^.]+$)")
PAT_HYPH_HHMM = re.compile(r"(\d{4})-(\d{2})-(\d{2})-(\d{4})(?=\.[^.]+$)")

REQUIRED = {
    "program_title",
    "episode_no",
    "subtitle",
    "air_date",
    "confidence",
    "needs_review",
    "model",
    "extraction_version",

    "evidence",
}
ACTIVE_TITLE_ARCHITECTURE = "A_AI_PRIMARY_WITH_GUARDRAILS"
DEFERRED_TITLE_ARCHITECTURES = (
    "B_FULL_AI_MIN_RULES",
    "C_LEGACY_PARSER_WITH_AI_REVIEW",
    "D_RULE_ENGINE_PRIMARY_WITH_AI_FALLBACK",
    "E_TWO_STAGE_LIGHT_PARSE_THEN_AI",
)

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def validate_rows(rows: list[dict]) -> list[str]:
    errs: list[str] = []
    for i, obj in enumerate(rows, start=1):
        if not (obj.get("path_id") or obj.get("path")):
            errs.append(f"row {i}: missing path_id/path")
        missing = sorted([k for k in REQUIRED if k not in obj])
        if missing:
            errs.append(f"row {i}: missing keys: {missing}")
        missing_contract = sorted([k for k in DB_CONTRACT_REQUIRED if k not in obj])
        if missing_contract:
            errs.append(f"row {i}: missing DB contract keys: {missing_contract}")
        c = obj.get("confidence")
        if not (isinstance(c, (int, float)) and 0 <= float(c) <= 1):
            errs.append(f"row {i}: confidence must be 0..1")
        if not isinstance(obj.get("needs_review"), bool):
            errs.append(f"row {i}: needs_review must be bool")
    return errs


def norm_ws(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    return WS.sub(" ", s).strip()


def extract_air_date(name: str) -> str | None:
    n = norm_ws(name)
    for p in (PAT_US_DUP, PAT_US_PAREN, PAT_US, PAT_SP, PAT_COL, PAT_HHMM, PAT_COMPACT_14, PAT_COMPACT_12, PAT_HYPH_HHMM):
        m = p.search(n)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def strip_suffix(name: str) -> str:
    n = norm_ws(name)
    for p in (PAT_US_DUP, PAT_US_PAREN, PAT_US, PAT_SP, PAT_COL, PAT_HHMM, PAT_COMPACT_14, PAT_COMPACT_12, PAT_HYPH_HHMM):
        m = p.search(n)
        if m:
            return n[: m.start()].rstrip(" _-")
    return os.path.splitext(n)[0]


def norm_key(title: str) -> str:
    t = norm_ws(title).replace(" ", "_")
    t = BAD.sub("", t)
    t = UND.sub("_", t).strip("_")
    return t or "UNKNOWN"


def _normalize_title_compare(s: str) -> str:
    return BAD.sub("", norm_ws(str(s or "")).lower()).replace(" ", "")


def _score_title_overlap(base: str, candidate: str) -> int:
    b = _normalize_title_compare(base)
    c = _normalize_title_compare(candidate)
    if not b or not c:
        return 0
    if b == c:
        return 10_000
    if b in c:
        return len(b)
    if c in b:
        return len(c)
    score = 0
    for token in re.split(r"[\s_]+", norm_ws(base)):
        t = _normalize_title_compare(token)
        if t and t in c:
            score += len(t)
    return score



def drop_label_words(s: str) -> str:
    s = re.sub(r"(名作選|傑作選)", "", s)
    s = s.replace("選", "")
    return s.strip(" _-")


def parse_episode(base: str) -> int | None:
    m = re.search(r"第\s*(\d+)\s*話", base)
    if m:
        return int(m.group(1))
    m = re.search(r"\((\d+)\)\s*$", base)
    if m:
        return int(m.group(1))
    return None


def parse_quoted_subtitle(s: str) -> str | None:
    m = re.search(r"「([^」]{1,120})」", s)
    return m.group(1) if m else None


def _normalize_alias_key(s: str) -> str:
    return norm_ws(s).lower()


class HintSet:
    """ヒントの2形式 (alias_map / regex_rules) をまとめて保持するコンテナ。

    alias_map:   {正規化済み文字列キー → canonical_title}  — canonical_title/aliases 形式
    regex_rules: [(compiled_pattern, canonical_title, field), ...]  — rules: 形式 (program_aliases.yaml)
                 field: "base" = ファイル名base に適用, "program_title" = LLM出力のprogram_titleに適用
    """

    def __init__(
        self,
        alias_map: dict[str, str] | None = None,
        regex_rules: list[tuple[re.Pattern, str, str]] | None = None,
    ) -> None:
        self.alias_map: dict[str, str] = alias_map or {}
        self.regex_rules: list[tuple[re.Pattern, str, str]] = regex_rules or []

    def __bool__(self) -> bool:
        return bool(self.alias_map or self.regex_rules)

    def __len__(self) -> int:
        return len(self.alias_map) + len(self.regex_rules)


def _iter_hint_items(obj: Any) -> list[dict[str, Any]]:
    """canonical_title/aliases 形式のアイテムを返す。"""
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        out: list[dict[str, Any]] = []
        for key in ("hints", "user_learned"):
            seq = obj.get(key)
            if isinstance(seq, list):
                out.extend([x for x in seq if isinstance(x, dict)])
        return out
    return []


def _iter_regex_rules(obj: Any) -> list[tuple[re.Pattern, str, str]]:
    """rules: 形式 (program_aliases.yaml) から (compiled_pattern, canonical_title, field) を返す。
    field: "base" = ファイル名baseに適用, "program_title" = LLM出力のprogram_titleに適用。
    未指定時のデフォルトは "base"。
    """
    if not isinstance(obj, dict):
        return []
    rules = obj.get("rules")
    if not isinstance(rules, list):
        return []
    result: list[tuple[re.Pattern, str, str]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if not rule.get("enabled", True):
            continue
        match = rule.get("match")
        set_ = rule.get("set")
        if not isinstance(match, dict) or not isinstance(set_, dict):
            continue
        pattern = match.get("regex")
        canonical = set_.get("program_title")
        field = match.get("field", "base")
        if field not in ("base", "program_title"):
            field = "base"
        if not isinstance(pattern, str) or not isinstance(canonical, str):
            continue
        if not pattern or not canonical:
            continue
        try:
            result.append((re.compile(pattern), canonical.strip(), field))
        except re.error as e:
            print(f"W invalid regex in rules: pattern={pattern!r} error={e}")
    return result


class HintLoadFailure(RuntimeError):
    def __init__(self, status: dict[str, Any]) -> None:
        super().__init__(str(status.get("hintsLoadError") or "failed to load hints"))
        self.status = status


def _default_hints_status(path: str | None) -> dict[str, Any]:
    return {
        "hintsPath": str(path or ""),
        "hintsFilePresent": False,
        "hintsParserAvailable": yaml is not None,
        "hintsLoadable": False,
        "hintsLoaded": False,
        "hintsLoadError": None,
        "hintsAliasCount": 0,
        "hintsRegexRulesCount": 0,
    }


def _hint_load_error(status: dict[str, Any], message: str) -> HintLoadFailure:
    status["hintsLoadable"] = False
    status["hintsLoaded"] = False
    status["hintsLoadError"] = message
    return HintLoadFailure(status)


def _load_hint_items_from_file(path: Path) -> tuple[list[dict[str, Any]], list[tuple[re.Pattern, str, str]]]:
    """ファイルから (alias_items, regex_rules) を読み込む。"""
    if not path.exists():
        return [], []
    obj: Any = None
    suffix = path.suffix.lower()
    if suffix == ".json":
        try:
            with path.open("r", encoding="utf-8-sig") as f:
                obj = json.load(f)
        except Exception as e:
            raise ValueError(f"failed to load JSON hints: path={path} error={e}") from e
    elif suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError(f"PyYAML is required to load hints YAML: path={path}")
        try:
            with path.open("r", encoding="utf-8-sig") as f:
                obj = yaml.safe_load(f)
        except Exception as e:
            raise ValueError(f"failed to load YAML hints: path={path} error={e}") from e
    if obj is None:
        return [], []
    return _iter_hint_items(obj), _iter_regex_rules(obj)


def _load_hints_with_status(path: str | None) -> tuple[HintSet, dict[str, Any]]:
    status = _default_hints_status(path)
    if not path:
        return HintSet(), status
    p = Path(path)
    if not p.exists():
        print(f"W hints file missing: {p}", file=sys.stderr)
        return HintSet(), status
    status["hintsFilePresent"] = True

    files: list[Path]
    if p.is_dir():
        files = sorted(
            [x for x in p.glob("*") if x.suffix.lower() in {".yaml", ".yml", ".json"}]
        )
    else:
        files = [p]

    alias_map: dict[str, str] = {}
    regex_rules: list[tuple[re.Pattern, str, str]] = []
    for fp in files:
        try:
            alias_items, rules = _load_hint_items_from_file(fp)
        except Exception as e:
            raise _hint_load_error(status, str(e)) from e
        for item in alias_items:
            canonical = item.get("canonical_title")
            if not isinstance(canonical, str):
                continue
            canonical = canonical.strip()
            if not canonical:
                continue
            aliases = item.get("aliases")
            raw_aliases: list[str] = []
            if isinstance(aliases, list):
                raw_aliases.extend([a for a in aliases if isinstance(a, str)])
            raw_aliases.append(canonical)
            for alias in raw_aliases:
                key = _normalize_alias_key(alias)
                if key:
                    alias_map[key] = canonical
        regex_rules.extend(rules)

    hints = HintSet(alias_map=alias_map, regex_rules=regex_rules)
    status["hintsLoadable"] = True
    status["hintsLoaded"] = bool(hints)
    status["hintsAliasCount"] = len(hints.alias_map)
    status["hintsRegexRulesCount"] = len(hints.regex_rules)
    return hints, status


def _load_hints(path: str | None) -> tuple[HintSet, bool]:
    hints, status = _load_hints_with_status(path)
    return hints, bool(status["hintsLoaded"])


def _canonicalize_title_from_hints(base: str, parsed_program_title: str, alias_map: HintSet) -> str:
    # 1. regex_rules を先に評価 (program_aliases.yaml の rules: 形式)
    for pattern, canonical, field in alias_map.regex_rules:
        target = parsed_program_title if field == "program_title" else base
        m = pattern.search(target)
        if m:
            try:
                return m.expand(canonical)  # \1 等のバックリファレンスを展開
            except re.error:
                return canonical

    # 2. alias_map で完全一致 (exact key lookup)
    key = _normalize_alias_key(parsed_program_title)
    if key in alias_map.alias_map:
        return alias_map.alias_map[key]

    # 3. alias_map でサブストリング一致 (substring fallback)
    base_norm = _normalize_alias_key(base)
    for alias_key, canonical in alias_map.alias_map.items():
        if alias_key and alias_key in base_norm:
            return canonical

    return parsed_program_title


def parse_program_and_subtitle(base: str) -> tuple[str, str | None]:
    b = base
    m = re.match(r"^【NHK地域局発】(.+)$", b)
    if m:
        rest = drop_label_words(m.group(1).strip("_ "))
        return "NHK地域局発", (rest or None)

    m = re.match(r"^【ハートネット(?:TV|tv|ｔｖ)】\s*虹クロ[_\s]*(.+)$", b)
    if m:
        rest = m.group(1).strip("_ ")
        q = parse_quoted_subtitle(rest)
        sub = q or rest.split("▼")[0].split("▽")[0].strip(" _")
        return "虹クロ", (sub[:120] if sub else None)

    if "アナザーストーリーズ" in b:
        m = re.search(r"アナザーストーリーズ(?:選)?(.+)$", b)
        sub = None
        if m:
            rest = m.group(1).lstrip("選")
            rest = rest.replace("_", " ")
            rest = rest.split("▼")[0].split("▽")[0].strip(" ")
            sub = rest[:120] if rest else None
        return "アナザーストーリーズ", sub

    m = re.match(r"^[『「]([^』」]+)[』」](.*)$", b)
    if m:
        title = drop_label_words(m.group(1).strip())
        rest = drop_label_words(m.group(2).strip("_ "))
        sub = parse_quoted_subtitle(rest) or parse_quoted_subtitle(b)
        if sub and sub == title:
            sub = None
        return title, sub

    m = re.match(r"^【([^】]+)】\s*(.+)?$", b)
    if m:
        tag = drop_label_words(m.group(1).strip())
        rest = drop_label_words((m.group(2) or "").strip("_ "))
        if tag.startswith("特選") and rest:
            tok = rest.split(" ")[0].split("_")[0]
            return tok, parse_quoted_subtitle(rest)
        if rest:
            tok = rest.split(" ")[0].split("_")[0]
            if len(tok) >= 2 and not tok.startswith("▼"):
                return tok, parse_quoted_subtitle(rest)
        return tag, None

    prog = b.split(" ")[0].split("_")[0].strip()
    return prog[:80], parse_quoted_subtitle(b)


class _ProgramDictionary:
    """Known program titles loaded from programs table for dictionary-first matching.

    Entries are sorted by normalized length (longest first) so that specific
    matches like "NHKスペシャル" are preferred over shorter "NHK".
    """

    def __init__(self, con: sqlite3.Connection):
        self._entries: list[tuple[str, str]] = []  # (normalized, canonical_title)
        try:
            rows = con.execute("SELECT DISTINCT canonical_title FROM programs").fetchall()
        except sqlite3.OperationalError:
            return
        seen: set[str] = set()
        for row in rows:
            title = str(row[0] or "").strip()
            if not title or title == "UNKNOWN":
                continue
            norm = _normalize_title_compare(title)
            if norm and norm not in seen and len(norm) >= 2:
                seen.add(norm)
                self._entries.append((norm, title))
        self._entries.sort(key=lambda x: len(x[0]), reverse=True)

    @property
    def count(self) -> int:
        return len(self._entries)

    def match(self, base_norm: str) -> str | None:
        """Find the longest known program title contained in base_norm."""
        if not base_norm:
            return None
        for norm, canonical in self._entries:
            if norm in base_norm:
                return canonical
        return None


def _match_from_known_programs(
    base: str,
    program_dict: _ProgramDictionary | None,
    alias_hints: HintSet,
) -> str | None:
    """Dictionary-first match: search known programs in raw filename base.

    Returns canonical_title if found, None otherwise.
    Priority: regex rules > programs table (longest match) > alias map (substring).
    """
    # 1. Alias regex rules (highest specificity — human-curated patterns)
    for pattern, canonical, field in alias_hints.regex_rules:
        target = base
        m = pattern.search(target)
        if m:
            try:
                return m.expand(canonical)
            except re.error:
                return canonical

    base_norm = _normalize_title_compare(base)
    if not base_norm:
        return None

    # 2. Programs table (longest match first)
    if program_dict:
        result = program_dict.match(base_norm)
        if result:
            return result

    # 3. Alias YAML substring match
    alias_norm = _normalize_alias_key(base)
    for alias_key, canonical in alias_hints.alias_map.items():
        if alias_key and alias_key in alias_norm:
            return canonical

    return None


def _choose_epg_title_candidate(base: str, epg_candidates: list[dict]) -> str | None:
    """Pick the best-matching EPG title from candidates.

    Returns the raw EPG ``official_title`` string — this is an episode-level
    title (番組名+サブタイトル+説明), NOT a clean program name.  Callers must
    run ``parse_program_and_subtitle()`` on the result before using it as a
    program title.
    """
    if not epg_candidates:
        return None
    ranked = sorted(
        (
            (_score_title_overlap(base, str(c.get("official_title") or "")), str(c.get("official_title") or "").strip())
            for c in epg_candidates
        ),
        key=lambda x: x[0],
        reverse=True,
    )
    best_score, best_title = ranked[0]
    if best_score <= 0 or not best_title:
        return None
    return best_title


def extract_title_ai_primary(
    name: str,
    base: str,
    alias_hints: HintSet,
    epg_candidates: list[dict] | None = None,
    program_dict: _ProgramDictionary | None = None,
) -> dict:
    # Phase 1: Dictionary match — known programs (programs table + alias YAML)
    dict_match = _match_from_known_programs(base, program_dict, alias_hints)
    if dict_match:
        return {
            "program_title": dict_match,
            "subtitle": parse_quoted_subtitle(base),
            "episode_no": parse_episode(base),
            "air_date": extract_air_date(name),
            "title_extraction_path": "dictionary_match",
        }

    # Phase 2: Rule-based parsing (fallback for unknown programs)
    prog, sub = parse_program_and_subtitle(base)
    source = "rule_based_fallback"
    canonical = _canonicalize_title_from_hints(base=base, parsed_program_title=prog, alias_map=alias_hints)
    if canonical != prog:
        prog = canonical
        source = "rule_based_fallback+hints"

    # Phase 3: EPG hint (conservative — only for genuinely bad parses)
    epg_title_raw = _choose_epg_title_candidate(base, epg_candidates or [])
    if epg_title_raw:
        # EPG official_title is almost always "番組名+サブタイトル+説明" — NOT a clean program name.
        # Extract the program name portion before using it as a candidate.
        epg_prog, _ = parse_program_and_subtitle(epg_title_raw)
        prog_norm = _normalize_title_compare(prog)
        epg_norm = _normalize_title_compare(epg_prog)
        # Only adopt EPG program name when the parsed title is genuinely bad.
        # Never override a valid short title with a longer EPG string.
        should_take_epg = (
            not prog_norm
            or prog in ("UNKNOWN", "")
            or SUBTITLE_SEPARATORS.search(prog) is not None
            or len(prog) > 80
        )
        if should_take_epg and epg_norm and prog_norm != epg_norm:
            prog = epg_prog
            source = "rule_based_fallback+epg_hint"

    return {
        "program_title": prog,
        "subtitle": sub,
        "episode_no": parse_episode(base),
        "air_date": extract_air_date(name),
        "title_extraction_path": source,
    }


class _EpgCache:
    """Pre-loaded EPG metadata cache for fast lookup by match_key / datetime_key.

    ``official_title`` in each record is the full EPG episode-level title
    (番組名+サブタイトル+説明), not a clean program name.
    """

    def __init__(self, con: sqlite3.Connection):
        self._by_match_key: dict[str, dict] = {}
        self._by_title_dt: dict[str, list[dict]] = {}
        self._by_datetime_key: dict[str, list[dict]] = {}
        self._by_filename_stem: dict[str, dict] = {}
        old_factory = con.row_factory
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(
                """
                SELECT b.broadcast_id, b.program_id, b.match_key, b.data_json,
                       b.is_rebroadcast_flag, b.epg_genres, b.description,
                       b.official_title, b.annotations
                FROM broadcasts b
                """,
            ).fetchall()
        except sqlite3.Error:
            return
        finally:
            con.row_factory = old_factory
        for row in rows:
            data = reconstruct_broadcast_data(row)
            data["broadcast_id"] = row[0]
            data["program_id"] = row[1]
            mk = row[2] or data.get("match_key")
            dk = data.get("datetime_key")
            if mk:
                self._by_match_key[mk] = data
                parts = str(mk).split("::")
                if len(parts) == 4:
                    title_dt = f"{parts[0]}::{parts[2]}::{parts[3]}"
                    self._by_title_dt.setdefault(title_dt, []).append(data)
                elif len(parts) == 3:
                    self._by_title_dt.setdefault(str(mk), []).append(data)
            if dk:
                self._by_datetime_key.setdefault(str(dk), []).append(data)
            # ファイル名ステムによる 1:1 マッチング（最も信頼性が高い）
            stem = data.get("ts_filename_stem")
            if stem:
                self._by_filename_stem[str(stem)] = data

    def lookup(self, match_key: str | None, datetime_key: str | None, filename_stem: str | None = None) -> dict | None:
        # ファイル名ステムが最優先（タイトル+日時のあいまいマッチより確実）
        if filename_stem and filename_stem in self._by_filename_stem:
            return self._by_filename_stem[filename_stem]
        if match_key:
            if match_key in self._by_match_key:
                return self._by_match_key[match_key]
            if match_key in self._by_title_dt:
                return self._by_title_dt[match_key][0]
        if datetime_key and datetime_key in self._by_datetime_key:
            return self._by_datetime_key[datetime_key][0]
        return None

    def candidates(self, match_key: str | None, datetime_key: str | None) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()

        def _push(items: list[dict] | None) -> None:
            if not items:
                return
            for item in items:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("official_title") or "").strip()
                if not title:
                    continue
                key = _normalize_title_compare(title)
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(item)

        if match_key:
            direct = self._by_match_key.get(match_key)
            if isinstance(direct, dict):
                _push([direct])
            _push(self._by_title_dt.get(match_key))
        if datetime_key:
            _push(self._by_datetime_key.get(datetime_key))
        return out


def _to_epg_hint_payload(epg_candidates: list[dict]) -> dict:
    titles: list[str] = []
    seen_titles: set[str] = set()
    start_at: list[str] = []
    seen_start_at: set[str] = set()
    for item in epg_candidates:
        title = str(item.get("official_title") or "").strip()
        if title and title not in seen_titles:
            titles.append(title)
            seen_titles.add(title)
        air_date = str(item.get("air_date") or "").strip()
        start_time = str(item.get("start_time") or "").strip()
        if air_date and start_time:
            dt = f"{air_date} {start_time}"
            if dt not in seen_start_at:
                start_at.append(dt)
                seen_start_at.add(dt)
    return {
        "epg_hint_titles": titles[:5],
        "epg_hint_start_at": start_at[:5],
    }


def _enrich_with_epg(row: dict, epg: dict | None) -> None:
    """Add EPG-sourced fields to a metadata row (in-place)."""
    if not epg:
        return
    row["broadcaster"] = epg.get("broadcaster")
    row["epg_genre"] = None
    genres = epg.get("epg_genres")
    if isinstance(genres, list) and genres:
        first = genres[0]
        if isinstance(first, dict):
            cat = first.get("category", "")
            sub = first.get("subcategory", "")
            row["epg_genre"] = f"{cat} - {sub}" if sub else cat
        elif isinstance(first, str):
            row["epg_genre"] = first
    row["epg_genres"] = genres
    row["is_rebroadcast_flag"] = epg.get("is_rebroadcast_flag", False)
    if epg.get("description"):
        row["epg_description"] = epg["description"][:300]


def enforce_db_contract(row: dict) -> None:
    missing = [k for k in DB_CONTRACT_REQUIRED if k not in row]
    if missing:
        raise ValueError(f"missing DB contract keys: {missing}")
    if not isinstance(row.get("needs_review"), bool):
        raise ValueError("needs_review must be bool")


def load_queue(path: str):
    first_data_row = True
    with open(path, "r", encoding="utf-8-sig") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            obj = json.loads(line)
            if i == 0 and isinstance(obj, dict) and "_meta" in obj:
                continue
            # Validate that queue rows have required fields
            if first_data_row:
                first_data_row = False
                if isinstance(obj, dict) and not obj.get("path_id"):
                    raise SystemExit(
                        f"ERROR: queue file does not contain 'path_id' field (row 1). "
                        f"This looks like an inventory file, not a metadata queue. "
                        f"Use the 'queue' path from Stage 1 output, not 'inventory'. "
                        f"File: {path}"
                    )
            yield obj


def write_jsonl(path: Path, rows: list[dict]):
    with open(path, "w", encoding="utf-8") as fo:
        for r in rows:
            fo.write(json.dumps(r, ensure_ascii=False) + "\n")


def _build_llm_input_rows(batch: list[dict], epg_cache: _EpgCache) -> list[dict]:
    rows: list[dict] = []
    for rec in batch:
        rec_out = dict(rec)
        mk = match_key_from_filename(str(rec.get("name") or ""))
        dk = datetime_key_from_filename(str(rec.get("name") or ""))
        epg_candidates = epg_cache.candidates(mk, dk)
        rec_out.update(_to_epg_hint_payload(epg_candidates))
        rows.append(rec_out)
    return rows


def _get_latest_llm_metadata(con: sqlite3.Connection, path_id: str) -> dict | None:
    md, _ = latest_path_metadata(con, path_id)
    return md


def _build_locked_row(existing: dict, rec: dict, model: str, extraction_version: str) -> dict:
    row = dict(existing)
    program_title = str(row.get("program_title") or "")
    row["path_id"] = rec.get("path_id") or row.get("path_id")
    row["path"] = rec.get("path") or row.get("path")
    row["program_title"] = program_title
    row["episode_no"] = row.get("episode_no")
    row["subtitle"] = row.get("subtitle")
    row["air_date"] = row.get("air_date")
    c = row.get("confidence")
    row["confidence"] = float(c) if isinstance(c, (int, float)) else 0.9
    row["needs_review"] = bool(row.get("needs_review"))
    row["model"] = row.get("model") or model
    row["extraction_version"] = row.get("extraction_version") or extraction_version

    row["evidence"] = row.get("evidence") or {"source_name": "manual_review_lock", "raw": rec.get("name")}
    row["human_reviewed"] = True
    row.setdefault("title_architecture", ACTIVE_TITLE_ARCHITECTURE)
    row.setdefault("title_extraction_path", "human_review_lock")
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--queue", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--start-batch", type=int, default=1)
    ap.add_argument("--max-batches", type=int, default=None)
    ap.add_argument("--model", default="openai-codex/gpt-5.2")
    ap.add_argument("--extraction-version", default="prompt_v1_20260208")
    ap.add_argument("--hints", default="")
    ap.add_argument("--franchise-rules", default="")
    ap.add_argument("--ignore-human-reviewed", action="store_true")
    ap.add_argument("--prepare-only", action="store_true",
                    help="Write input JSONL batches and exit without running extraction or upserting to DB.")
    args = ap.parse_args()

    try:
        alias_hints, hints_status = _load_hints_with_status(args.hints)
    except HintLoadFailure as e:
        print(
            json.dumps(
                {
                    "ok": False,
                    "tool": "run_metadata_batches_promptv1",
                    **e.status,
                },
                ensure_ascii=False,
            )
        )
        return 2
    print(
        f"INFO title_architecture={ACTIVE_TITLE_ARCHITECTURE} "
        f"deferred={','.join(DEFERRED_TITLE_ARCHITECTURES)} "
        f"hints_loaded={str(hints_status['hintsLoaded']).lower()} "
        f"alias_count={hints_status['hintsAliasCount']} "
        f"regex_rules_count={hints_status['hintsRegexRulesCount']}"
    )
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    batch: list[dict] = []
    batch_idx = 0
    processed = 0
    preserved_human_reviewed = 0
    title_auto_corrected = 0
    generated_input_jsonl_paths: list[str] = []
    generated_output_jsonl_paths: list[str] = []
    from mediaops_schema import connect_db
    db_con = connect_db(args.db)
    epg_cache = _EpgCache(db_con)
    program_dict = _ProgramDictionary(db_con)
    canonical_sources = load_canonical_title_sources(db_con)
    print(f"INFO program_dictionary_count={program_dict.count}")

    def flush():
        nonlocal batch, batch_idx, processed, preserved_human_reviewed, title_auto_corrected
        nonlocal generated_input_jsonl_paths, generated_output_jsonl_paths
        if not batch:
            return
        batch_idx += 1
        batch_no = args.start_batch + batch_idx - 1
        bpath = outdir / f"llm_filename_extract_input_{batch_no:04d}_{len(batch):04d}.jsonl"
        epath = outdir / f"llm_filename_extract_output_{batch_no:04d}_{len(batch):04d}.jsonl"
        llm_input_rows = _build_llm_input_rows(batch, epg_cache)
        write_jsonl(bpath, llm_input_rows)

        rows = []
        path_program_links: list[tuple[str, str, str | None, str, str]] = []
        for rec in batch:
            pid = rec.get("path_id")
            if pid and not args.ignore_human_reviewed:
                existing = _get_latest_llm_metadata(db_con, str(pid))
                if isinstance(existing, dict) and existing.get("human_reviewed") is True:
                    locked_row = _build_locked_row(existing, rec, args.model, args.extraction_version)
                    enforce_db_contract(locked_row)
                    rows.append(locked_row)
                    preserved_human_reviewed += 1
                    continue

            name = rec["name"]
            base = strip_suffix(name)
            mk = match_key_from_filename(name)
            dk = datetime_key_from_filename(name)
            epg_candidates = epg_cache.candidates(mk, dk)
            title_res = extract_title_ai_primary(
                name=name, base=base, alias_hints=alias_hints,
                epg_candidates=epg_candidates, program_dict=program_dict,
            )
            air = title_res.get("air_date")
            prog = str(title_res.get("program_title") or "")
            sub = title_res.get("subtitle")
            ep = title_res.get("episode_no")

            needs = False
            reasons: list[str] = []

            # Auto-correct program_title against DB human_reviewed canonical titles
            _suggested, _match_source = suggest_canonical_title(prog, canonical_sources, min_extra_chars=3)
            if _suggested and _suggested != prog:
                prog = _suggested
                reasons.append(f"auto_corrected_from_{_match_source}")
                title_auto_corrected += 1

            # Dictionary match → high confidence; rule-based fallback → baseline
            extraction_path = title_res.get("title_extraction_path") or ""
            conf = 0.92 if extraction_path == "dictionary_match" else 0.78
            if air is None:
                mtime = rec.get("mtime_utc") or rec.get("mtimeUtc")
                if isinstance(mtime, str) and len(mtime) >= 10 and mtime[4] == "-" and mtime[7] == "-":
                    air = mtime[:10]
                    reasons.append("air_date_from_mtime")
                    conf = min(conf, 0.72)
                else:
                    if prog not in ("塚原卜伝",):
                        needs = True
                        reasons.append("missing_air_date")
                        conf = min(conf, 0.65)

            if prog in ("UNKNOWN", ""):
                needs = True
                reasons.append("unknown_program_title")
                conf = 0.4
            if prog == "虹クロ" and not sub:
                needs = True
                reasons.append("niji_kuro_missing_subtitle")
                conf = min(conf, 0.65)
            if len(prog) > 80:
                needs = True
                reasons.append("program_title_too_long")
                conf = min(conf, 0.7)
            if detect_swallowed_program_title(str(rec.get("path") or ""), {"program_title": prog}):
                needs = True
                if "program_title_may_include_description" not in reasons:
                    reasons.append("program_title_may_include_description")
                conf = min(conf, 0.65)
            if SUBTITLE_SEPARATORS.search(prog):
                needs = True
                if "subtitle_separator_in_program_title" not in reasons:
                    reasons.append("subtitle_separator_in_program_title")
                conf = min(conf, 0.65)

            row = {
                "path_id": rec["path_id"],
                "path": rec["path"],
                "program_title": prog,
                "episode_no": ep,
                "subtitle": sub,
                "air_date": air,
                "genre": None,
                "franchise": None,
                "broadcaster": None,
                "channel": None,
                "confidence": round(float(conf), 2),
                "needs_review": bool(needs),
                "needs_review_reason": ",".join(reasons) if reasons else None,
                "model": args.model,
                "extraction_version": args.extraction_version,

                "title_architecture": ACTIVE_TITLE_ARCHITECTURE,
                "title_extraction_path": title_res.get("title_extraction_path"),
                "evidence": {"source_name": "filename", "raw": name},
            }

            # EPG enrichment: look up ingested program.txt data
            # ファイル名ステムで 1:1 照合（タイトル+日時によるあいまいマッチより優先）
            _fs = PurePath(str(rec.get("path") or "")).stem or None
            epg = epg_cache.lookup(mk, dk, filename_stem=_fs)
            _enrich_with_epg(row, epg)
            if epg and rec.get("path_id") and epg.get("program_id"):
                path_program_links.append((
                    str(rec["path_id"]),
                    str(epg["program_id"]),
                    str(epg.get("broadcast_id")) if epg.get("broadcast_id") else None,
                    "reextract",
                    now_iso(),
                ))

            row["genre"] = resolve_genre(row)
            row["franchise"] = resolve_franchise(row, args.franchise_rules or None)
            row["source_history"] = [make_entry("rule_based", [k for k, v in row.items() if v is not None and k not in {"path_id", "path"}])]

            enforce_db_contract(row)
            rows.append(row)

        write_jsonl(epath, rows)
        errs = validate_rows(rows)
        if errs:
            for e in errs[:20]:
                print(f"E {e}")
            raise SystemExit(f"validation failed: {epath} errors={len(errs)}")

        from upsert_path_metadata_jsonl import main as _upsert_main  # type: ignore
        import sys

        sys.argv = ["upsert", "--db", args.db, "--in", str(epath), "--source", "rule_based"]
        if args.franchise_rules:
            sys.argv += ["--franchise-rules", args.franchise_rules]
        if _upsert_main() != 0:
            raise SystemExit(f"upsert failed: {epath}")

        if path_program_links:
            try:
                db_con.executemany(
                    """
                    INSERT INTO path_programs (path_id, program_id, broadcast_id, source, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(path_id, program_id) DO UPDATE SET
                      broadcast_id=excluded.broadcast_id,
                      source=excluded.source,
                      updated_at=excluded.updated_at
                    """,
                    path_program_links,
                )
                db_con.commit()
            except sqlite3.OperationalError as e:
                if "path_programs" in str(e):
                    print(f"W path_programs upsert skipped (table may not exist yet): {e}", file=sys.stderr)
                else:
                    raise

        generated_input_jsonl_paths.append(str(bpath))
        generated_output_jsonl_paths.append(str(epath))
        processed += len(batch)
        batch = []

    if args.prepare_only:
        # --prepare-only: write input JSONL batches only, no extraction or upsert
        def flush_prepare_only():
            nonlocal batch, batch_idx
            if not batch:
                return
            batch_idx += 1
            batch_no = args.start_batch + batch_idx - 1
            bpath = outdir / f"llm_filename_extract_input_{batch_no:04d}_{len(batch):04d}.jsonl"
            llm_input_rows = _build_llm_input_rows(batch, epg_cache)
            write_jsonl(bpath, llm_input_rows)
            generated_input_jsonl_paths.append(str(bpath))
            batch = []

        for rec in load_queue(args.queue):
            batch.append(rec)
            if len(batch) >= args.batch_size:
                flush_prepare_only()
                if args.max_batches is not None and batch_idx >= args.max_batches:
                    break
        flush_prepare_only()
        db_con.close()
        print(f"OK prepare_only batches={batch_idx} total_records={sum(1 for _ in [])}")
        print(
            json.dumps(
                {
                    "ok": True,
                    "tool": "run_metadata_batches_promptv1",
                    "prepareOnly": True,
                    "queuePath": str(args.queue),
                    "outdir": str(outdir),
                    "batchSize": int(args.batch_size),
                    "maxBatches": int(args.max_batches) if args.max_batches is not None else None,
                    "batches": int(batch_idx),
                    "inputJsonlPaths": generated_input_jsonl_paths,
                    "latestInputJsonlPath": generated_input_jsonl_paths[-1] if generated_input_jsonl_paths else None,
                    **hints_status,
                },
                ensure_ascii=False,
            )
        )
        return 0

    try:
        for rec in load_queue(args.queue):
            batch.append(rec)
            if len(batch) >= args.batch_size:
                flush()
                if args.max_batches is not None and batch_idx >= args.max_batches:
                    break
        flush()
    finally:
        db_con.close()
    print(f"OK preserved_human_reviewed={preserved_human_reviewed}")
    print(f"OK title_auto_corrected={title_auto_corrected}")
    print(f"OK processed={processed} batches={batch_idx}")
    print(
        json.dumps(
            {
                "ok": True,
                "tool": "run_metadata_batches_promptv1",
                "prepareOnly": False,
                "queuePath": str(args.queue),
                "outdir": str(outdir),
                "batchSize": int(args.batch_size),
                "maxBatches": int(args.max_batches) if args.max_batches is not None else None,
                "model": str(args.model),
                "extractionVersion": str(args.extraction_version),
                "preserveHumanReviewed": not bool(args.ignore_human_reviewed),
                "preservedHumanReviewed": int(preserved_human_reviewed),
                "titleAutoCorrected": int(title_auto_corrected),
                "processed": int(processed),
                "batches": int(batch_idx),
                "inputJsonlPaths": generated_input_jsonl_paths,
                "outputJsonlPaths": generated_output_jsonl_paths,
                "latestInputJsonlPath": generated_input_jsonl_paths[-1] if generated_input_jsonl_paths else None,
                "latestOutputJsonlPath": generated_output_jsonl_paths[-1] if generated_output_jsonl_paths else None,
                **hints_status,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
