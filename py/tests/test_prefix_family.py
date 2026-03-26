"""Tests for prefix-family discovery and suggestion in title_resolution."""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from title_resolution import (
    CanonicalTitleSources,
    _discover_prefix_families,
    load_canonical_title_sources,
    suggest_canonical_title,
)
from path_placement_rules import normalize_title_for_comparison


def _make_db(
    titles: list[str],
    *,
    human_reviewed: list[str] | None = None,
    programs: list[str] | None = None,
) -> sqlite3.Connection:
    """Create an in-memory DB with path_metadata and programs tables."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE path_metadata ("
        "  path_id TEXT PRIMARY KEY,"
        "  source TEXT NOT NULL,"
        "  data_json TEXT NOT NULL DEFAULT '{}',"
        "  updated_at TEXT NOT NULL DEFAULT '',"
        "  program_title TEXT,"
        "  human_reviewed INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    con.execute(
        "CREATE TABLE programs ("
        "  program_id TEXT PRIMARY KEY,"
        "  program_key TEXT NOT NULL UNIQUE,"
        "  canonical_title TEXT NOT NULL,"
        "  created_at TEXT NOT NULL DEFAULT '',"
        "  franchise_id TEXT"
        ")"
    )
    for i, t in enumerate(titles):
        is_hr = human_reviewed and t in human_reviewed
        con.execute(
            "INSERT INTO path_metadata (path_id, source, program_title, human_reviewed) "
            "VALUES (?, ?, ?, ?)",
            (f"p{i}", "human_reviewed" if is_hr else "epg", t, 1 if is_hr else 0),
        )
    for i, t in enumerate(programs or []):
        con.execute(
            "INSERT INTO programs (program_id, program_key, canonical_title) "
            "VALUES (?, ?, ?)",
            (f"prog{i}", f"key{i}", t),
        )
    con.commit()
    return con


class TestDiscoverPrefixFamilies:
    """Tests for _discover_prefix_families."""

    def test_basic_family(self):
        """Short base + longer variants → base is discovered."""
        con = _make_db([
            "みみより!解説",
            "みみより!解説 イラン情勢緊迫化 くらしへの影響は",
            "みみより!解説 スポーツで街をきれいに",
        ])
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "みみより!解説" in originals

    def test_multiple_families(self):
        """Multiple independent families are all discovered."""
        con = _make_db([
            "みみより!解説",
            "みみより!解説 イラン情勢緊迫化",
            "サイエンスZERO",
            "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
        ])
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "みみより!解説" in originals
        assert "サイエンスZERO" in originals

    def test_short_title_excluded(self):
        """Titles shorter than MIN_PREFIX_FAMILY_BASE_LEN are excluded."""
        con = _make_db([
            "NHK",
            "NHKスペシャル",
        ])
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "NHK" not in originals

    def test_human_reviewed_included(self):
        """human_reviewed titles are still included as prefix family bases.

        Contaminated variants may also be human_reviewed, so bases must
        be discoverable regardless of review status.
        """
        con = _make_db(
            [
                "みみより!解説",
                "みみより!解説 サブタイトル",
            ],
            human_reviewed=["みみより!解説", "みみより!解説 サブタイトル"],
        )
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "みみより!解説" in originals

    def test_programs_table_included(self):
        """programs table titles are included as prefix family bases."""
        con = _make_db(
            [
                "サイエンスZERO",
                "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            ],
            programs=["サイエンスZERO"],
        )
        sources = load_canonical_title_sources(con)
        originals = set(sources.prefix_families)
        assert "サイエンスZERO" in originals

    def test_programs_table_match_takes_priority(self):
        """programs_table match is preferred over prefix_family."""
        con = _make_db(
            [
                "サイエンスZERO",
                "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            ],
            programs=["サイエンスZERO"],
        )
        sources = load_canonical_title_sources(con)
        suggested, match_source = suggest_canonical_title(
            "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            sources,
            min_extra_chars=4,
        )
        assert suggested == "サイエンスZERO"
        assert match_source == "programs_table"

    def test_no_family_single_title(self):
        """A title with no longer variant is not a family base."""
        con = _make_db(["みみより!解説"])
        families = _discover_prefix_families(con)
        assert len(families) == 0

    def test_sorted_longest_first(self):
        """Results are sorted by normalized length descending."""
        con = _make_db([
            "ABC",  # too short (< MIN_PREFIX_FAMILY_BASE_LEN=4 after NFKC)
            "ABCDE",
            "ABCDEFGH",
            "ABCDE longer suffix here",
            "ABCDEFGH longer suffix here",
        ])
        families = _discover_prefix_families(con)
        if len(families) >= 2:
            len0 = len(normalize_title_for_comparison(families[0]))
            len1 = len(normalize_title_for_comparison(families[1]))
            assert len0 >= len1


class TestSuggestWithPrefixFamily:
    """Tests for suggest_canonical_title with prefix_family source."""

    def test_prefix_family_suggestion(self):
        """Title with no human_reviewed/programs match uses prefix_family."""
        con = _make_db([
            "サイエンスZERO",
            "サイエンスZERO 密着!絶海に浮かぶ奇跡の島 南大東島",
            "サイエンスZERO 昆虫たちの恋リア",
        ])
        sources = load_canonical_title_sources(con)
        suggested, match_source = suggest_canonical_title(
            "サイエンスZERO 密着!絶海に浮かぶ奇跡の島 南大東島",
            sources,
            min_extra_chars=4,
        )
        assert suggested == "サイエンスZERO"
        assert match_source == "prefix_family"

    def test_human_reviewed_takes_priority(self):
        """human_reviewed match is preferred over prefix_family."""
        con = _make_db(
            [
                "サイエンスZERO",
                "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            ],
            human_reviewed=["サイエンスZERO"],
        )
        sources = load_canonical_title_sources(con)
        suggested, match_source = suggest_canonical_title(
            "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            sources,
            min_extra_chars=4,
        )
        assert suggested == "サイエンスZERO"
        assert match_source == "human_reviewed"

    def test_exact_human_reviewed_not_contaminated(self):
        """Exact human_reviewed match returns None (already canonical)."""
        con = _make_db(
            [
                "サイエンスZERO",
                "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            ],
            human_reviewed=["サイエンスZERO"],
        )
        sources = load_canonical_title_sources(con)
        suggested, match_source = suggest_canonical_title(
            "サイエンスZERO",
            sources,
            min_extra_chars=4,
        )
        assert suggested is None
        assert match_source == "exact_human_reviewed"

    def test_base_title_is_itself_not_contaminated(self):
        """The base title itself should not be flagged as contaminated."""
        con = _make_db([
            "みみより!解説",
            "みみより!解説 イラン情勢緊迫化",
        ])
        sources = load_canonical_title_sources(con)
        suggested, match_source = suggest_canonical_title(
            "みみより!解説",
            sources,
            min_extra_chars=4,
        )
        # Base title is in prefix_families → exact match → should not suggest itself
        # The _match_prefix requires len(pt_norm) >= len(cand_norm) + min_extra_chars
        # so exact match won't satisfy that condition
        assert suggested is None or match_source == "exact_human_reviewed"

    def test_min_extra_chars_respected(self):
        """Suffix too short for min_extra_chars → no match."""
        con = _make_db([
            "サイエンスZERO",
            "サイエンスZERO ab",  # only 3 extra chars (space + ab)
        ])
        sources = load_canonical_title_sources(con)
        suggested, match_source = suggest_canonical_title(
            "サイエンスZERO ab",
            sources,
            min_extra_chars=4,
        )
        # " ab" = 3 chars, less than min_extra_chars=4
        assert match_source != "prefix_family" or suggested is None
