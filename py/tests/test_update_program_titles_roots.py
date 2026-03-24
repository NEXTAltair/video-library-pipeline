"""Tests for infer_affected_root in update_program_titles."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from update_program_titles import infer_affected_root


def test_layout_detection_standard():
    """<root>\\<prog>\\<YYYY>\\<MM>\\<file> 構造からルートを検出できる。"""
    path = r"B:\VideoLibrary\ヒューマニエンス\2026\03\file.ts"
    result = infer_affected_root(path, "ヒューマニエンス")
    assert result == r"B:\VideoLibrary"


def test_layout_detection_ignores_old_title():
    """layout検出が成功すればold_titleに依存しない。"""
    path = r"B:\VideoLibrary\SomeProgram\2025\12\ep.ts"
    result = infer_affected_root(path, "全然違うタイトル")
    assert result == r"B:\VideoLibrary"


def test_old_title_match_nonstandard_layout():
    """年月レイアウトでないパスはold_titleセグメントマッチでルートを返す。"""
    path = r"B:\VideoLibrary\ヒューマニエンス\ep01.ts"
    result = infer_affected_root(path, "ヒューマニエンス")
    assert result == r"B:\VideoLibrary"


def test_safe_dir_name_fallback():
    """禁止文字を含むold_titleはsafe_dir_name変換後でもマッチできる。"""
    # "A/B" → safe_dir_name → "A＿B"
    path = r"B:\VideoLibrary\A＿B\ep.ts"
    result = infer_affected_root(path, "A/B")
    assert result == r"B:\VideoLibrary"


def test_drive_root_fallback():
    """layout・old_titleどちらもマッチしない場合はドライブルートを返す。"""
    path = r"B:\SomeRandomDir\file.ts"
    result = infer_affected_root(path, "存在しないタイトル")
    assert result == "B:\\"


def test_empty_path_returns_none():
    """空パスはNoneを返す。"""
    result = infer_affected_root("", "タイトル")
    assert result is None


def test_forward_slash_path():
    """スラッシュ区切りのパスも正しく処理される。"""
    path = "B:/VideoLibrary/番組名/2024/06/file.ts"
    result = infer_affected_root(path, "番組名")
    assert result == r"B:\VideoLibrary"
