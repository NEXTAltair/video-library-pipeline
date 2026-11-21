import pytest

from video_pipeline.domain.path_placement_rules import DriveRoute, DriveRoutes, build_routed_dest_path, load_drive_routes

DEST = r'E:\Dドキュメンタリー･情報'


def doc_route(**extra):
    return DriveRoute({
        'genre': 'documentary', 'dest_root': DEST, 'layout': 'by_program_year_month',
        'epg_genre_match': ['ドキュメンタリー/教養 - *', '情報/ワイドショー - *'],
        'layout_rules': [
            {'layout': 'by_title', 'programs': ['NHKスペシャル', 'ガイアの夜明け'],
             'epg_genre_match': ['ドキュメンタリー/教養 - *']},
            {'layout': 'by_program_year_month', 'programs': ['きょうの料理']},
        ],
        **extra,
    })


def md(title, *genres, genre='documentary'):
    return {'program_title': title, 'air_date': '2026-03-04', 'needs_review': False, 'genre': genre,
            'epg_genres': [{'category': c, 'subcategory': s} for c, s in genres]}


def place(m, route=None):
    routes = DriveRoutes(r'B:\VideoLibrary', 'by_program_year_month', [route or doc_route()])
    return build_routed_dest_path(routes, r'B:\未視聴\rec.mp4', m)


def test_listed_program_uses_title_layout_regardless_of_epg_drift():
    # Per-episode EPG drifts (お知らせ/報道); the listed program must not split.
    for genres in [('ドキュメンタリー/教養', 'その他'), ('情報/ワイドショー', '番組紹介・お知らせ')]:
        assert place(md('NHKスペシャル', genres)) == (DEST + r'\NHKスペシャル\rec.mp4', 'documentary', None)
    assert place(md('ガイアの夜明け', ('ニュース/報道', '特集・ドキュメント')))[0] == DEST + r'\ガイアの夜明け\rec.mp4'


def test_listed_program_matches_after_nfkc_casefold():
    assert place(md('ＮＨＫスペシャル'))[0] == DEST + r'\ＮＨＫスペシャル\rec.mp4'


def test_explicit_year_month_listing_beats_primary_epg():
    m = md('きょうの料理', ('ドキュメンタリー/教養', 'その他'))
    assert place(m)[0] == DEST + r'\きょうの料理\2026\03\rec.mp4'


def test_unlisted_program_uses_primary_epg_only():
    assert place(md('ETV特集', ('ドキュメンタリー/教養', 'ドキュメンタリー全般')))[0] == DEST + r'\ETV特集\rec.mp4'
    # Secondary genre must not promote a wide show out of year/month folders.
    m = md('グッド!モーニング', ('情報/ワイドショー', '芸能・ワイドショー'), ('ドキュメンタリー/教養', 'その他'))
    assert place(m)[0] == DEST + r'\グッド!モーニング\2026\03\rec.mp4'


def test_unknown_program_without_epg_keeps_route_layout():
    assert place(md('謎の番組'))[0] == DEST + r'\謎の番組\2026\03\rec.mp4'


def test_flat_epg_genre_string_is_primary_fallback():
    m = {'program_title': 'ETV特集', 'air_date': '2026-03-04', 'needs_review': False,
         'genre': 'documentary', 'epg_genre': 'ドキュメンタリー/教養 - 社会・時事'}
    assert place(m)[0] == DEST + r'\ETV特集\rec.mp4'


def test_route_without_layout_rules_is_unchanged():
    route = DriveRoute({'genre': 'variety', 'dest_root': r'E:\Bバラエティ', 'layout': 'by_program_year_month'})
    assert route.layout_rules == []
    assert route.layout_for(md('NHKスペシャル', ('ドキュメンタリー/教養', 'その他'))) == 'by_program_year_month'


def test_title_layout_ignores_missing_air_date():
    m = md('NHKスペシャル')
    m['air_date'] = ''
    assert place(m) == (DEST + r'\NHKスペシャル\rec.mp4', 'documentary', None)


@pytest.mark.parametrize('rules,msg', [
    ({'programs': ['x']}, 'must be a list'),
    ([{'programs': ['x']}], 'needs a layout'),
    ([{'layout': 'by_decade', 'programs': ['x']}], 'unknown layout'),
    ([{'layout': 'by_title'}], 'needs programs or epg_genre_match'),
    ([{'layout': 'by_title', 'programs': 'NHKスペシャル'}], 'programs must be a list'),
    ([{'layout': 'by_title', 'epg_genre_match': [1]}], 'epg_genre_match must be a list'),
    ([{'layout': 'by_title', 'programs': ['A']}, {'layout': 'by_program_year_month', 'programs': ['a']}],
     'two layouts'),
])
def test_invalid_layout_rules_fail_on_load(rules, msg):
    with pytest.raises(ValueError, match=msg):
        DriveRoute({'genre': 'documentary', 'dest_root': DEST, 'layout_rules': rules})


def test_layout_rules_load_from_yaml(tmp_path):
    p = tmp_path / 'drive_routes.yaml'
    p.write_text(
        'default_dest: B:\\\\VideoLibrary\n'
        'routes:\n'
        '- genre: documentary\n'
        '  dest_root: E:\\\\Doc\n'
        '  layout: by_program_year_month\n'
        '  layout_rules:\n'
        '  - layout: by_series\n'
        '    programs: [NHKスペシャル]\n', encoding='utf-8')
    route = load_drive_routes(p).routes[0]
    assert route.layout_rules[0].layout == 'by_title'
    assert route.layout_for(md('NHKスペシャル')) == 'by_title'
