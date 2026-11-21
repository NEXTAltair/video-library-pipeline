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
    # Not a declared work and filed outside the series: no series membership.
    name = '[映]シン・ゴジラ[SS][字]2023-11-03-21：00.mp4'
    md = {'program_title': 'シン・ゴジラ', 'air_date': '2023-11-03', 'genre': '映画', 'needs_review': False}
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_program_year_month', [], movie_grouping=grouping())
    assert build_routed_dest_path(routes, r'B:\未視聴' + '\\' + name, md)[0] == \
        r'B:\VideoLibrary\シン・ゴジラ\2023\11' + '\\' + name
    # Already inside the per-work series: stays where it is (#198).
    assert route(md, name)[0] == SERIES + '\\' + name
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


# ── #198: unlisted films already inside a per-work series stay put ──


def frame_grouping(layout='by_work_year_month'):
    data = {'default_dest_root': 'L:\\', 'frames': ['金曜ロードショー', '日曜ゴジラ劇場'],
            'series': [{'folder': 'シリーズ_ゴジラ+モンスターバース', 'dest_root': 'N:\\', 'works': ['ゴジラ-1.0']}]}
    if layout:
        data['series'][0]['work_layout'] = layout
    return MovieGrouping(data)


def movie_routes(layout='by_work_year_month'):
    from video_pipeline.domain.path_placement_rules import DriveRoute
    movie = DriveRoute({'genre': 'movie', 'dest_root': 'L:\\', 'layout': 'by_syllabary', 'epg_genre_match': ['映画 - *']})
    variety = DriveRoute({'genre': 'variety', 'dest_root': r'E:\Bバラエティ', 'layout': 'by_program_year_month'})
    return DriveRoutes(r'B:\VideoLibrary', 'by_program_year_month', [movie, variety],
                       movie_grouping=frame_grouping(layout))


@pytest.mark.parametrize('src', [
    SERIES + r'\ゴジラＶＳビオランテ◆特集2023-10-22-16：30.mp4',
    SERIES + r'\その他ゴジラ\ゴジラＶＳビオランテ.mp4',
])
def test_unlisted_film_inside_work_layout_series_stays(src):
    md = {'program_title': 'ゴジラVSビオランテ', 'air_date': '2023-10-22', 'needs_review': False, 'genre': '映画'}
    dst, genre, err = build_routed_dest_path(movie_routes(), src, md)
    assert (dst, genre, err) == (src, 'movie', None)


def test_frame_broadcast_of_unlisted_film_inside_series_stays():
    src = SERIES + r'\日曜ゴジラ劇場　映画「キングコング対ゴジラ」2023-10-08.mp4'
    md = {'program_title': 'キングコング対ゴジラ', 'air_date': '2023-10-08', 'needs_review': False}
    assert build_routed_dest_path(movie_routes(), src, md)[0] == src


def test_listed_work_inside_series_still_moves_to_its_work_folder():
    src = SERIES + r'\金曜ロードショー「ゴジラ-1.0」2024_11_01_21_00.mp4'
    md = {'program_title': 'ゴジラ-1.0', 'air_date': '2024-11-01', 'needs_review': False, 'genre': '映画'}
    assert build_routed_dest_path(movie_routes(), src, md)[0] == \
        SERIES + r'\ゴジラ-1.0\2024\11\金曜ロードショー「ゴジラ-1.0」2024_11_01_21_00.mp4'


def test_non_movie_inside_series_leaves_it():
    src = SERIES + r'\ゴジラ\2026\05\家、ついて行ってイイですか？ゴジラ 2026 05 24 20 50.mp4'
    md = {'program_title': '家、ついて行ってイイですか？', 'air_date': '2026-05-24', 'needs_review': False,
          'genre': 'バラエティ'}
    dst = build_routed_dest_path(movie_routes(), src, md)[0]
    assert dst == r'E:\Bバラエティ\家、ついて行ってイイですか？\2026\05\家、ついて行ってイイですか？ゴジラ 2026 05 24 20 50.mp4'


def test_flat_series_and_outside_sources_keep_previous_routing():
    md = {'program_title': 'ゴジラVSビオランテ', 'air_date': '2023-10-22', 'needs_review': False, 'genre': '映画'}
    assert build_routed_dest_path(movie_routes(None), SERIES + r'\v.mp4', md)[0] == r'L:\カ\v.mp4'
    assert build_routed_dest_path(movie_routes(), r'B:\未視聴\v.mp4', md)[0] == r'L:\カ\v.mp4'


# ── #203: by_work places films directly under the work folder (no year/month) ──


def test_by_work_layout_omits_year_month():
    name = '金曜ロードショー「ゴジラ-1.0」★早くも地上波初放送2024_11_01_21_00.mp4'
    dst, genre, err = route({'program_title': 'ゴジラ-1.0', 'air_date': '2024-11-01', 'genre': '映画'}, name,
                            layout='by_work')
    assert (dst, genre, err) == (SERIES + r'\ゴジラ-1.0' + '\\' + name, 'movie', None)


def test_by_work_layout_does_not_require_air_date():
    dst, _, err = route({'program_title': 'ゴジラ対ヘドラ', 'air_date': None, 'genre': '映画'}, 'x.mp4',
                        layout='by_work')
    assert err is None and dst == SERIES + r'\ゴジラ対ヘドラ\x.mp4'


def test_by_work_lifts_film_out_of_year_month_folder():
    name = '[映]ゴジラ対ヘドラ[SS]2022-09-04.mp4'
    src = SERIES + r'\ゴジラ対ヘドラ\2022\09' + '\\' + name
    md = {'program_title': 'ゴジラ対ヘドラ', 'air_date': '2022-09-04', 'needs_review': False, 'genre': '映画'}
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_program_year_month', [], movie_grouping=grouping('by_work'))
    assert build_routed_dest_path(routes, src, md)[0] == SERIES + r'\ゴジラ対ヘドラ' + '\\' + name


def test_by_work_keeps_alias_folder_and_moves_unlisted_films_to_title_folder():
    name = 'ゴジラxコング_新たなる帝国(吹替版)2025_02_08_21_00.mp4'
    dst, _, _ = route({'program_title': 'ゴジラxコング', 'air_date': '2025-02-08', 'genre': '映画'}, name,
                      layout='by_work')
    assert dst == SERIES + r'\ゴジラxコング' + '\\' + name
    src = SERIES + r'\その他ゴジラ\ゴジラＶＳビオランテ.mp4'
    md = {'program_title': 'ゴジラVSビオランテ', 'air_date': '2023-10-22', 'needs_review': False, 'genre': '映画'}
    assert build_routed_dest_path(movie_routes('by_work'), src, md)[0] == \
        SERIES + r'\ゴジラVSビオランテ\ゴジラＶＳビオランテ.mp4'


# ── #207: by_work places reviewed films outside the works list in their own work folder ──


@pytest.mark.parametrize('src', [
    SERIES + r'\ゴジラ＃05 [映]ゴジラＶＳビオランテ[SS]2023-10-22.mp4',
    SERIES + r'\ゴジラ\2025\08\シネマ「ゴジラ」2025-08-10.mp4',
])
def test_by_work_unlisted_reviewed_film_gets_title_folder(src):
    md = {'program_title': 'ゴジラVSビオランテ', 'air_date': '2023-10-22', 'needs_review': False, 'genre': '映画'}
    name = PureWindowsPath(src).name
    dst, genre, err = build_routed_dest_path(movie_routes('by_work'), src, md)
    assert (dst, genre, err) == (SERIES + r'\ゴジラVSビオランテ' + '\\' + name, 'movie', None)


def test_by_work_unlisted_title_folder_uses_existing_replacement():
    src = SERIES + r'\x.mp4'
    md = {'program_title': 'メカゴジラの逆襲/特別版', 'air_date': '2023-10-22', 'needs_review': False, 'genre': '映画'}
    assert build_routed_dest_path(movie_routes('by_work'), src, md)[0] == SERIES + r'\メカゴジラの逆襲＿特別版\x.mp4'


@pytest.mark.parametrize('md', [
    # Movie evidence from EPG/frame only, no accepted genre.
    {'program_title': 'キングコング対ゴジラ', 'epg_genre': '映画 - 邦画'},
    # Accepted genre but a frame name as the title.
    {'program_title': '日曜ゴジラ劇場', 'genre': '映画'},
    {'program_title': 'Unknown', 'genre': '映画'},
])
def test_by_work_without_usable_reviewed_title_stays(md):
    src = SERIES + r'\日曜ゴジラ劇場　映画「キングコング対ゴジラ」2023-10-08.mp4'
    md = {'air_date': '2023-10-08', 'needs_review': False, **md}
    assert build_routed_dest_path(movie_routes('by_work'), src, md)[0] == src


def test_by_work_year_month_series_keeps_unlisted_films_in_place():
    src = SERIES + r'\ゴジラＶＳビオランテ.mp4'
    md = {'program_title': 'ゴジラVSビオランテ', 'air_date': '2023-10-22', 'needs_review': False, 'genre': '映画'}
    assert build_routed_dest_path(movie_routes('by_work_year_month'), src, md)[0] == src
