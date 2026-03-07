import unittest

from relocate_existing_files import (
    looks_like_swallowed_program_title,
    looks_like_truncated_program_title,
)


class RelocateSuspiciousTitleTests(unittest.TestCase):
    def test_swallowed_program_title_detects_prefix_plus_episode_text(self):
        src = r"B:\VideoLibrary\RNC_news_every\2026\03\RNC_news_every▽特集.mp4"
        md = {"program_title": "RNC_news_every きょうの特集"}
        self.assertTrue(looks_like_swallowed_program_title(src, md))

    def test_truncated_program_title_detects_shortened_llm_title(self):
        src = r"B:\VideoLibrary\RNC_news_every\2026\03\foo.mp4"
        md = {"program_title": "RNC"}
        self.assertTrue(looks_like_truncated_program_title(src, md))

    def test_truncated_program_title_not_triggered_when_title_matches_folder(self):
        src = r"B:\VideoLibrary\RNC_news_every\2026\03\foo.mp4"
        md = {"program_title": "RNC_news_every"}
        self.assertFalse(looks_like_truncated_program_title(src, md))


if __name__ == "__main__":
    unittest.main()
