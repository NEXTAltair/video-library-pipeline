from pathlib import PureWindowsPath
import pytest
from video_pipeline.domain.movie_grouping import MovieGrouping
from video_pipeline.domain.path_placement_rules import DriveRoute, DriveRoutes, build_routed_dest_path, load_drive_routes


def policy():
    return MovieGrouping({'default_dest_root': 'L:\\', 'frames': ['金曜ロードショー', '日曜アニメ劇場', '日曜邦画劇場'],
                          'series': [{'folder': 'シリーズ_ガンダム', 'dest_root': r'D:\Anime',
                                      'works': ['機動戦士ガンダムIII めぐりあい宇宙編',
                                                {'title': '機動戦士Zガンダム A New Translation II 恋人たち',
                                                 'aliases': ['機動戦士Ζガンダム A New Translation II 恋人たち']}]}]})


def test_movie_series_group_does_not_replace_canonical_work_identity():
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_title', [], movie_grouping=policy())
    name = '日曜アニメ劇場 機動戦士ガンダムⅢ めぐりあい宇宙編_2026_08_23_19_00.mp4'
    title = '機動戦士ガンダムIII めぐりあい宇宙編'
    md = {'program_title': title, 'air_date': '2026-08-23', 'needs_review': False}
    dst, genre, error = build_routed_dest_path(routes, 'B:\\old\\' + name, md)
    assert dst == str(PureWindowsPath(r'D:\Anime\シリーズ_ガンダム') / name)
    assert genre == 'movie' and error is None
    assert md['program_title'] == title


def test_single_movie_uses_one_frame_folder_not_a_work_folder():
    assert policy().resolve('8番出口', '金曜ロードショー「8番出口」説明_2026_08_28_21_00.mp4') == ('L:', '金曜ロードショー')
    assert policy().resolve('お引越し', 'お引越し【日曜邦画劇場・再】説明.mp4') == ('L:', '日曜邦画劇場')
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_title', [], movie_grouping=policy())
    md = {'program_title': '8番出口', 'air_date': '2026-08-28', 'needs_review': False}
    dst = build_routed_dest_path(routes, 'B:\\old\\金曜ロードショー「8番出口」.mp4', md)[0]
    assert dst == 'L:\\金曜ロードショー\\金曜ロードショー「8番出口」.mp4'


def test_roman_numerals_and_explicit_work_aliases_are_normalized():
    assert policy().resolve('Unknown', '日曜アニメ劇場 機動戦士Ζガンダム A New Translation Ⅱ 恋人たち 説明.mp4') == (r'D:\Anime', 'シリーズ_ガンダム')


def test_mentions_and_frame_exhibitions_do_not_become_movies():
    assert policy().resolve('ユタンポ◆映画', 'ユタンポ★映画「ガンダム」紹介番組.mp4') is None
    assert policy().resolve('金曜ロードショーとジブリ展', '金曜ロードショーとジブリ展の舞台裏.mp4') is None
    assert policy().resolve('機動戦士ガンダムIII めぐりあい宇宙編', '映画の紹介番組.mp4') is None


def test_optional_config_is_relative_to_routes_file_and_missing_file_fails(tmp_path):
    group = tmp_path / 'movies.yaml'
    group.write_text('default_dest_root: "L:\\\\"\nframes: [金曜ロードショー]\nseries: []\n')
    route = tmp_path / 'routes.yaml'
    route.write_text('default_dest: "B:\\\\VideoLibrary"\nmovie_grouping_path: movies.yaml\nroutes: []\n')
    assert load_drive_routes(route).movie_grouping is not None
    group.unlink()
    with pytest.raises(FileNotFoundError):
        load_drive_routes(route)


def test_legacy_routes_without_policy_keep_their_layout():
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_title', [])
    md = {'program_title': 'Film', 'air_date': '2026-01-01', 'needs_review': False}
    assert build_routed_dest_path(routes, r'B:\old\Film.mp4', md)[0] == r'B:\VideoLibrary\Film\Film.mp4'


def test_arbitrary_title_prefix_cannot_claim_an_unrelated_film():
    p = MovieGrouping({'default_dest_root': 'L:\\', 'frames': ['金曜ロードショー'],
                       'series': [{'folder': 'ITシリーズ', 'dest_root': 'L:\\', 'works': ['IT']}]})
    assert p.resolve('It Follows', '金曜ロードショー「It Follows」_2026_08_28.mp4') == ('L:', '金曜ロードショー')
    assert p.resolve('Unknown', '金曜ロードショー It Follows 説明.mp4') == ('L:', '金曜ロードショー')
    assert p.resolve('IT', '金曜ロードショー「IT」.mp4') == ('L:', 'ITシリーズ')


def test_grouping_destinations_remain_in_relocate_and_dedup_inventory(tmp_path):
    from video_pipeline.domain.routing_inventory import default_relocate_roots
    from dedup_cli import configured_roots
    group = tmp_path / 'movies.yaml'
    group.write_text('default_dest_root: "L:\\\\Movies"\nframes: [金曜ロードショー]\nseries:\n  - folder: Known\n    dest_root: "D:\\\\Series"\n    works: [Film]\n')
    route = tmp_path / 'routes.yaml'
    route.write_text('default_dest: "B:\\\\VideoLibrary"\nmovie_grouping_path: movies.yaml\nroutes: []\n')
    roots = default_relocate_roots(r'B:\Source', r'B:\VideoLibrary', r'J:\TVFile', str(route))
    assert r'L:\Movies' in roots and r'D:\Series' in roots
    dedup = configured_roots(r'B:\Source', r'B:\VideoLibrary', str(route))
    assert '/mnt/l/Movies' in dedup and '/mnt/d/Series' in dedup


def test_japanese_resolved_genres_remain_authoritative_for_routes():
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_title', [
        DriveRoute({'genre': 'movie', 'dest_root': 'L:\\', 'layout': 'by_title'}),
        DriveRoute({'genre': 'drama', 'dest_root': r'E:\Dドラマ', 'layout': 'by_title'}),
        DriveRoute({'genre': 'documentary', 'dest_root': r'E:\Documentary', 'layout': 'by_title'}),
    ], movie_grouping=policy())
    for label, expected in [('映画', 'movie'), ('ドラマ', 'drama'), ('ドキュメンタリー・情報', 'documentary')]:
        assert routes.resolve({'genre': label})[0].genre == expected
    md = {'program_title': '機動戦士ガンダムIII めぐりあい宇宙編', 'genre': '映画',
          'air_date': '2026-01-01', 'needs_review': False}
    assert build_routed_dest_path(routes, r'B:\old\Movie.mp4', md)[0] == r'D:\Anime\シリーズ_ガンダム\Movie.mp4'


def test_explicit_movie_metadata_does_not_need_a_redundant_movie_route():
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_title', [], movie_grouping=policy())
    for genre in ['movie', '映画']:
        md = {'program_title': '機動戦士ガンダムIII めぐりあい宇宙編', 'genre': genre,
              'air_date': '2026-01-01', 'needs_review': False}
        assert build_routed_dest_path(routes, r'B:\old\Movie.mp4', md)[0] == r'D:\Anime\シリーズ_ガンダム\Movie.mp4'


def test_windows_absolute_nested_policy_is_normalized_before_join(tmp_path, monkeypatch):
    import video_pipeline.platform.pathscan_common as common
    group = tmp_path / 'movies.yaml'
    group.write_text('frames: []\nseries: []\n')
    original = common.windows_to_wsl_path
    monkeypatch.setattr(common, 'windows_to_wsl_path', lambda path: str(group) if path == r'C:\Rules\movies.yaml' else original(path))
    route = tmp_path / 'routes.yaml'
    route.write_text('default_dest: "B:\\\\VideoLibrary"\nmovie_grouping_path: \'C:\\Rules\\movies.yaml\'\nroutes: []\n')
    assert load_drive_routes(route).movie_grouping is not None
