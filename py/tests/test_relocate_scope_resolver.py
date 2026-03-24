from __future__ import annotations

import unittest

from relocate_scope_resolver import resolve_affected_roots


class ResolveAffectedRootsTests(unittest.TestCase):
    def test_prefers_structural_layout_when_title_already_updated(self) -> None:
        paths = [r"B:\VideoLibrary\汚染されたフォルダ名\2026\03\movie.ts"]
        old_titles = {"クリーンな番組名"}

        roots = resolve_affected_roots(paths, old_titles)

        self.assertEqual(roots, [r"B:\VideoLibrary"])

    def test_uses_old_title_fallback_when_layout_shape_not_detected(self) -> None:
        paths = [r"B:\VideoLibrary\古い番組名\special\movie.ts"]
        old_titles = {"古い番組名"}

        roots = resolve_affected_roots(paths, old_titles)

        self.assertEqual(roots, [r"B:\VideoLibrary"])

    def test_drive_root_fallback_when_no_layout_or_old_title_match(self) -> None:
        paths = [r"D:\misc\movie.ts"]
        old_titles = {"無関係タイトル"}

        roots = resolve_affected_roots(paths, old_titles)

        self.assertEqual(roots, ["D:\\"])


if __name__ == "__main__":
    unittest.main()
