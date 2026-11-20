"""Series opt-in per-work layout (#195) and evidence-gated franchise rules."""
from pathlib import Path, PureWindowsPath

import pytest

from video_pipeline.domain.franchise_resolver import resolve_franchise
from video_pipeline.domain.movie_grouping import MovieGrouping
from video_pipeline.domain.path_placement_rules import DriveRoutes, build_routed_dest_path

SERIES = r'N:\シリーズ_ゴジラ+モンスターバース'
RULES = str(Path(__file__).resolve().parents[2] / 'rules' / 'franchise_rules.yaml')


def grouping(layout='by_work_year_month'):
    godzilla = {'folder': 'シリーズ_ゴジラ+モンスターバース', 'dest_root': 'N:\\',
                'works': ['ゴジラ-1.0', 'ゴジラ-1.0/C', 'ゴジラvsコング', 'ゴジラ対ヘドラ', 'ゴジラ×メカゴジラ',
                          {'title': 'ゴジラxコング 新たなる帝国', 'aliases': ['ゴジラxコング'], 'folder': 'ゴジラxコング'}]}
    if layout:
        godzilla['work_layout'] = layout
    gundam = {'folder': 'シリーズ_ガンダム', 'dest_root': r'D:\Anime', 'works': ['機動戦士ガンダムUC']}
    return MovieGrouping({'default_dest_root': 'L:\\', 'frames': ['金曜ロードショー'], 'series': [godzilla, gundam]})


def route(md, name, layout='by_work_year_month'):
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_program_year_month', [], movie_grouping=grouping(layout))
    return build_routed_dest_path(routes, SERIES + '\\' + name, {'needs_review': False, **md})


def test_series_work_layout_places_film_under_work_year_month():
    name = '金曜ロードショー「ゴジラ-1.0」★早くも地上波初放送2024_11_01_21_00.mp4'
    dst, genre, err = route({'program_title': 'ゴジラ-1.0', 'air_date': '2024-11-01', 'genre': '映画'}, name)
    assert err is None and genre == 'movie'
    assert dst == SERIES + r'\ゴジラ-1.0\2024\11' + '\\' + name


def test_work_folder_uses_existing_replacement_for_forbidden_characters():
    name = '[初][映]ゴジラ－１．０／Ｃ[SS][字]2024-07-07-00：30.mp4'
    dst, _, err = route({'program_title': 'ゴジラ-1.0/C', 'air_date': '2024-07-07', 'genre': '映画'}, name)
    assert err is None
    assert PureWindowsPath(dst).parent.parent.parent.name == 'ゴジラ-1.0＿C'
    assert dst == SERIES + r'\ゴジラ-1.0＿C\2024\07' + '\\' + name


def test_alias_maps_to_declared_folder():
    name = 'ゴジラxコング_新たなる帝国(吹替版)2025_02_08_21_00.mp4'
    dst, _, err = route({'program_title': 'ゴジラxコング', 'air_date': '2025-02-08', 'genre': '映画'}, name)
    assert err is None and dst == SERIES + r'\ゴジラxコング\2025\02' + '\\' + name


def test_unlisted_film_stays_flat_and_default_layout_is_unchanged():
    # Not a declared work: no series membership at all (unchanged behaviour).
    name = '[映]シン・ゴジラ[SS][字]2023-11-03-21：00.mp4'
    md = {'program_title': 'シン・ゴジラ', 'air_date': '2023-11-03', 'genre': '映画'}
    assert route(md, name)[0] == r'B:\VideoLibrary\シン・ゴジラ\2023\11' + '\\' + name
    # A series without work_layout keeps the flat series-root placement.
    name = '[映]ゴジラ－１．０[SS][字]2024-07-21-14：05.mp4'
    md = {'program_title': 'ゴジラ-1.0', 'air_date': '2024-07-21', 'genre': '映画'}
    assert route(md, name, layout=None)[0] == SERIES + '\\' + name
    assert grouping(None).resolve('ゴジラ-1.0', name, True) == ('N:', 'シリーズ_ゴジラ+モンスターバース')


def test_other_series_keep_flat_layout():
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_title', [], movie_grouping=grouping())
    md = {'program_title': '機動戦士ガンダムUC', 'air_date': '2026-01-01', 'needs_review': False, 'genre': '映画'}
    assert build_routed_dest_path(routes, r'B:\old\a.mp4', md)[0] == r'D:\Anime\シリーズ_ガンダム\a.mp4'


def test_work_layout_requires_air_date():
    dst, genre, err = route({'program_title': 'ゴジラ対ヘドラ', 'air_date': None, 'genre': '映画'}, 'x.mp4')
    assert dst is None and err == 'missing_air_date'


def test_non_movie_mentioning_godzilla_is_not_grouped():
    name = '家、ついて行ってイイですか？ゴジラ・ジブリ・高市早苗 2026 05 24 20 50.mp4'
    md = {'program_title': 'ゴジラ', 'air_date': '2026-05-24', 'genre': 'バラエティ'}
    dst = route(md, name)[0]
    assert 'シリーズ_' not in dst


@pytest.mark.parametrize('layout,works', [
    ('by_year', ['A']),
])
def test_invalid_layout_rejected(layout, works):
    with pytest.raises(ValueError):
        MovieGrouping({'series': [{'folder': 'S', 'dest_root': 'N:\\', 'works': works, 'work_layout': layout}]})


@pytest.mark.parametrize('folder', ['a/b', '..', '', 'x:y'])
def test_unsafe_work_folder_rejected(folder):
    with pytest.raises(ValueError):
        MovieGrouping({'series': [{'folder': 'S', 'dest_root': 'N:\\', 'work_layout': 'by_work_year_month',
                                   'works': [{'title': 'A', 'folder': folder}]}]})


# ── franchise_rules evidence gate ───────────────────────────


@pytest.mark.parametrize('md', [
    # おはスタ / さんま御殿 / スイッチインタビュー style rows seen in the DB.
    {'program_title': 'おはスタ【目覚めスッキリの快眠方法を伝授!/ちびゴジラの逆襲】', 'genre': 'ドキュメンタリー・情報'},
    {'program_title': 'さんま御殿流行乗る人乗らない人バトル!ゴジラ山崎監督が(秘)を溺愛', 'genre': None},
    {'program_title': 'MLB歴代メジャーリーガーベストゲーム「世界のゴジラ 松井秀喜」', 'genre': 'スポーツ'},
    {'program_title': 'ゴジラ', 'path': r'B:\未視聴\家、ついて行ってイイですか？ゴジラ・ジブリ 2026 05 24.mp4'},
])
def test_godzilla_mentions_without_film_evidence_get_no_franchise(md):
    assert resolve_franchise(md, RULES) is None


@pytest.mark.parametrize('md', [
    {'program_title': 'ゴジラ-1.0', 'genre': '映画'},
    {'program_title': 'ゴジラvsコング', 'epg_genres': [{'category': '映画', 'subcategory': '洋画'}]},
    {'program_title': 'ゴジラ(1984年)', 'path': r'B:\未視聴\[2K][映]ゴジラ（1984年）2023-11-09.mp4'},
    {'program_title': 'ゴジラ・ザ・シリーズ', 'genre': '特撮'},
])
def test_godzilla_films_keep_franchise(md):
    assert resolve_franchise(md, RULES) == 'シリーズ_ゴジラ+モンスターバース'


def test_series_folder_alone_does_not_prove_membership():
    variety = {'program_title': '家、ついて行ってイイですか？', 'genre': 'バラエティ',
               'path': SERIES + r'\ゴジラ\2026\05\家、ついて行ってイイですか？ゴジラ.mp4'}
    assert resolve_franchise(variety) is None
    film = {'program_title': 'Unknown', 'genre': '映画', 'path': SERIES + r'\ゴジラ-1.0\2024\11\x.mp4'}
    assert resolve_franchise(film) == 'シリーズ_ゴジラ+モンスターバース'


def test_rules_without_requirement_keep_substring_behaviour():
    md = {'program_title': '機動戦士ガンダムSEED', 'genre': 'アニメ'}
    assert resolve_franchise(md, RULES) == 'シリーズ_ガンダム'
    md = {'program_title': '仮面ライダーガヴ', 'genre': None}
    assert resolve_franchise(md, RULES) == 'シリーズ_仮面ライダー'
