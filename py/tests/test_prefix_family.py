"""Tests for prefix-family discovery and suggestion in title_resolution."""
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


class TestDiscoverPrefixFamilies:
    """Tests for _discover_prefix_families."""

    def test_basic_family(self, make_db):
        """Short base + longer variants → base is discovered."""
        con = make_db([
            "みみより!解説",
            "みみより!解説 イラン情勢緊迫化 くらしへの影響は",
            "みみより!解説 スポーツで街をきれいに",
        ])
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "みみより!解説" in originals

    def test_multiple_families(self, make_db):
        """Multiple independent families are all discovered."""
        con = make_db([
            "みみより!解説",
            "みみより!解説 イラン情勢緊迫化",
            "サイエンスZERO",
            "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
        ])
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "みみより!解説" in originals
        assert "サイエンスZERO" in originals

    def test_short_title_excluded(self, make_db):
        """Titles shorter than MIN_PREFIX_FAMILY_BASE_LEN are excluded."""
        con = make_db([
            "NHK",
            "NHKスペシャル",
        ])
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "NHK" not in originals

    def test_human_reviewed_included(self, make_db):
        """human_reviewed titles are still included as prefix family bases.

        Contaminated variants may also be human_reviewed, so bases must
        be discoverable regardless of review status.
        """
        con = make_db(
            [
                "みみより!解説",
                "みみより!解説 サブタイトル",
            ],
            human_reviewed=["みみより!解説", "みみより!解説 サブタイトル"],
        )
        families = _discover_prefix_families(con)
        originals = set(families)
        assert "みみより!解説" in originals

    def test_programs_table_included(self, make_db):
        """programs table titles are included as prefix family bases."""
        con = make_db(
            [
                "サイエンスZERO",
                "サイエンスZERO 密着!絶海に浮かぶ奇跡の島",
            ],
            programs=["サイエンスZERO"],
        )
        sources = load_canonical_title_sources(con)
        originals = set(sources.prefix_families)
        assert "サイエンスZERO" in originals

    def test_programs_table_match_takes_priority(self, make_db):
        """programs_table match is preferred over prefix_family."""
        con = make_db(
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

    def test_no_family_single_title(self, make_db):
        """A title with no longer variant is not a family base."""
        con = make_db(["みみより!解説"])
        families = _discover_prefix_families(con)
        assert len(families) == 0

    def test_sorted_longest_first(self, make_db):
        """Results are sorted by normalized length descending."""
        con = make_db([
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

    def test_prefix_family_suggestion(self, make_db):
        """Title with no human_reviewed/programs match uses prefix_family."""
        con = make_db([
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

    def test_human_reviewed_takes_priority(self, make_db):
        """human_reviewed match is preferred over prefix_family."""
        con = make_db(
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

    def test_exact_human_reviewed_not_contaminated(self, make_db):
        """Exact human_reviewed match returns None (already canonical)."""
        con = make_db(
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

    def test_base_title_is_itself_not_contaminated(self, make_db):
        """The base title itself should not be flagged as contaminated."""
        con = make_db([
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

    def test_min_extra_chars_respected(self, make_db):
        """Suffix too short for min_extra_chars → no match."""
        con = make_db([
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
