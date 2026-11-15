import ingest_program_txt
import pytest
from series_name_extractor import extract_series_name


@pytest.mark.parametrize("title, expected", [
    ("ドラマ24孤独のグルメSeason11 5話 横浜上飯田の料理", "孤独のグルメSeason11"),
    ("ドラマ24 孤独のグルメSeason11! 第12話 高砂のかつ煮", "孤独のグルメSeason11"),
    ("日曜劇場「VIVANT」第23話 最終決戦", "VIVANT"),
    ("ドラマ24「別の作品」第2話", "別の作品"),
    ("ドラマ24 孤独のグルメSeason11 #5 横浜の料理", "孤独のグルメSeason11"),
    ("ドラマ24 孤独のグルメSeason11 ＃5 横浜の料理", "孤独のグルメSeason11"),
    ("ドラマ24 孤独のグルメSeason11「横浜の料理」", "孤独のグルメSeason11"),
    ("ドラマ24 孤独のグルメSeason11【横浜の料理】", "孤独のグルメSeason11"),
    ("ドラマ24 孤独のグルメSeason11▽横浜の料理", "孤独のグルメSeason11"),
])
def test_dramatic_frame_is_not_work_identity(title, expected):
    assert extract_series_name(title) == expected


@pytest.mark.parametrize("title", [
    "ドラマ", "日曜劇場", "日曜アニメ劇場", "ドラマ24",
    "ドラマ24孤独のグルメSeason11 5話 横浜上飯田の料理",
    "別の作品 第12話 副題",
])
def test_frame_and_episode_dictionary_entries_are_contaminated(title):
    from broadcast_frame_titles import is_contaminated_program_title
    assert is_contaminated_program_title(title)


def test_dictionary_does_not_pick_generic_drama_inside_documentary(make_db):
    import run_metadata_batches_promptv1 as mod
    con = make_db([], programs=["ドラマ", "日曜劇場"])
    name = "地球ドラマチック ナスカの地上絵_2026_08_15_19_00.mp4"
    result = mod.extract_title_ai_primary(name, mod.strip_suffix(name), mod.HintSet(),
                                        program_dict=mod._ProgramDictionary(con))
    assert result["program_title"] == "地球ドラマチック"


def test_frame_resolution_precedes_polluted_dictionary(make_db):
    import run_metadata_batches_promptv1 as mod
    con = make_db([], programs=["ドラマ24孤独のグルメSeason11 5話 横浜の料理", "日曜劇場"])
    for name, expected in [
        ("ドラマ24孤独のグルメSeason11 5話 横浜の料理_2026_05_02_00_12.mp4", "孤独のグルメSeason11"),
        ("日曜劇場「VIVANT」第23話_2026_11_01_21_00.mp4", "VIVANT"),
    ]:
        result = mod.extract_title_ai_primary(name, mod.strip_suffix(name), mod.HintSet(),
                                            program_dict=mod._ProgramDictionary(con))
        assert result["program_title"] == expected


def test_reviewed_work_without_episode_is_not_requeued_by_recording_suffix():
    import run_metadata_batches_promptv1 as mod
    from broadcast_frame_titles import reviewed_title_conflicts_with_filename
    name = "ドラマ24 孤独のグルメSeason11_2026_05_02_00_12.mp4"
    assert not reviewed_title_conflicts_with_filename("孤独のグルメSeason11", mod.strip_suffix(name))
    name = "ドラマ24 孤独のグルメSeason11「横浜の料理」_2026_05_02_00_12.mp4"
    assert not reviewed_title_conflicts_with_filename("孤独のグルメSeason11", mod.strip_suffix(name))


def test_curated_alias_precedence_is_shared_by_conflict_check():
    import re
    import run_metadata_batches_promptv1 as mod
    from broadcast_frame_titles import reviewed_title_conflicts_with_filename
    base = "ドラマ24 孤独のグルメSeason11 第5話 横浜の料理"
    hints = mod.HintSet(regex_rules=[(re.compile('^ドラマ24'), '孤独のグルメ シーズン11', 'base')])
    canonical = mod._match_from_known_programs(base, None, hints)
    assert canonical == '孤独のグルメ シーズン11'
    assert not reviewed_title_conflicts_with_filename(canonical, base, canonical)


def test_ingest_extract_and_move_share_curated_alias_identity(tmp_path):
    import run_metadata_batches_promptv1 as mod
    from broadcast_frame_titles import reviewed_title_conflicts_with_filename
    hints_file = tmp_path / 'aliases.yaml'
    hints_file.write_text('hints:\n  - canonical_title: 孤独のグルメ シーズン11\n    aliases: [ドラマ24 孤独のグルメSeason11]\n')
    base = 'ドラマ24 孤独のグルメSeason11 第5話「横浜の料理」'
    hints, _ = mod._load_hints_with_status(str(hints_file))
    extracted = mod._match_from_known_programs(base, None, hints)
    ingested = extract_series_name(base, aliases_path=str(hints_file))
    assert extracted == ingested == '孤独のグルメ シーズン11'
    assert not reviewed_title_conflicts_with_filename(ingested, base, extracted)


def test_ingest_and_extract_share_regex_alias_priority(tmp_path):
    import run_metadata_batches_promptv1 as mod
    hints_file = tmp_path / 'aliases.yaml'
    hints_file.write_text('rules:\n  - match: {regex: "^日曜劇場", field: base}\n    set: {program_title: VIVANTシリーズ}\n')
    base = '日曜劇場「VIVANT」第23話'
    hints, _ = mod._load_hints_with_status(str(hints_file))
    assert mod._match_from_known_programs(base, None, hints) == extract_series_name(base, aliases_path=str(hints_file)) == 'VIVANTシリーズ'


@pytest.mark.parametrize('unused_rules', [None, {}, ''])
def test_unused_rules_section_is_optional_for_both_alias_readers(tmp_path, unused_rules):
    import run_metadata_batches_promptv1 as mod
    import yaml
    path = tmp_path / 'aliases.yaml'
    path.write_text(yaml.safe_dump({'rules': unused_rules, 'hints': [{'canonical_title': 'Canonical', 'aliases': ['Alias']}]}, allow_unicode=True))
    hints, _ = mod._load_hints_with_status(str(path))
    assert mod._match_from_known_programs('Alias 第2話', None, hints) == 'Canonical'
    assert extract_series_name('Alias 第2話', aliases_path=str(path)) == 'Canonical'


def test_dictionary_never_identifies_a_work_mentioned_only_in_description(make_db):
    import run_metadata_batches_promptv1 as mod
    con = make_db([], programs=['シン・仮面ライダー', '仮面ライダー'])
    base = '仮面ライダー 第1話「怪奇蜘蛛男」 ドキュメント「シン・仮面ライダー」放送'
    assert mod._match_from_known_programs(base, mod._ProgramDictionary(con), mod.HintSet()) == '仮面ライダー'


def test_short_franchise_alias_cannot_swallow_a_distinct_series(make_db):
    import run_metadata_batches_promptv1 as mod
    from epg_common import normalize_program_key
    full = '機動戦士ガンダム 水星の魔女'
    base = full + ' 第11話「地球の魔女」'
    hints = mod.HintSet(alias_map={'機動戦士ガンダム': '機動戦士ガンダム'})
    con = make_db([], programs=['機動戦士ガンダム', full])
    assert mod._match_from_known_programs(base, mod._ProgramDictionary(con), hints) == full
    assert extract_series_name(base, _alias_map={normalize_program_key('機動戦士ガンダム'): '機動戦士ガンダム'}) == full


def test_dictionary_drops_bracketed_episode_titles_and_recognizes_promotion_label(make_db):
    import run_metadata_batches_promptv1 as mod
    con = make_db([], programs=['Newsモーニングサテライト', 'Newsモーニングサテライト【今日の金融】',
                               'なつかしのアニメ・特撮の旅'])
    dictionary = mod._ProgramDictionary(con)
    assert mod._match_from_known_programs('Newsモーニングサテライト【今日の金融】説明', dictionary, mod.HintSet()) == 'Newsモーニングサテライト'
    assert mod._match_from_known_programs('【銀河無料の日!】なつかしのアニメ・特撮の旅#1 説明', dictionary, mod.HintSet()) == 'なつかしのアニメ・特撮の旅'


def test_extract_series_name_strips_bs11_gundam_hour_prefix():
    title = "BS11ガンダムアワー 機動戦士ガンダム 水星の魔女 第11話「地球の魔女」"

    assert extract_series_name(title) == "機動戦士ガンダム 水星の魔女"


def test_ingest_program_txt_uses_series_title_for_programs_table(tmp_path):
    db_path = tmp_path / "mediaops.sqlite"
    db_path.touch()
    ts_root = tmp_path / "ts"
    ts_root.mkdir()
    program_txt = ts_root / "BS11ガンダムアワー 機動戦士ガンダム 水星の魔女 第11話「地球の魔女」_2026_03_13_19_30.ts.program.txt"
    program_txt.write_text(
        "\n".join(
            [
                "2026/03/13(金) 19:30～20:00",
                "ＢＳ１１イレブン",
                "BS11ガンダムアワー 機動戦士ガンダム 水星の魔女　第11話「地球の魔女」",
                "",
                "改修中のエアリアルを引き取るため、学園を出航する。",
            ]
        ),
        encoding="utf-8",
    )

    rc = ingest_program_txt.main(
        ["--db", str(db_path), "--ts-root", str(ts_root), "--apply"]
    )

    assert rc == 0

    import sqlite3

    con = sqlite3.connect(db_path)
    titles = [row[0] for row in con.execute("SELECT canonical_title FROM programs").fetchall()]
    con.close()

    assert titles == ["機動戦士ガンダム 水星の魔女"]


def test_newsoon_corner_title_is_canonicalized():
    title = "午後LIVE ニュースーン午後3時台時をかけるテレビ チャウシェスク政権の崩壊"

    assert extract_series_name(title) == "午後LIVEニュースーン"


def test_program_dictionary_skips_newsoon_swallowed_corner_title(make_db):
    import run_metadata_batches_promptv1 as mod

    con = make_db(
        [],
        programs=[
            "午後LIVE ニュースーン午後3時台時をかけるテレビ チャウシェスク政権の崩壊",
            "午後LIVEニュースーン",
        ],
    )

    result = mod._match_from_known_programs(
        "午後LIVE ニュースーン午後3時台時をかけるテレビ チャウシェスク政権の崩壊 時をかけるテレビは今から26年前に放送されたルーマ 2026 04 02 15 10.mp4",
        mod._ProgramDictionary(con),
        mod.HintSet(),
    )

    assert result == "午後LIVEニュースーン"


def test_toki_tv_episode_title_is_canonicalized():
    title = "時をかけるテレビ 池上彰 REINA チャウシェスク政権の崩壊"

    assert extract_series_name(title) == "時をかけるテレビ"


def test_overman_king_gainer_episode_title_is_canonicalized():
    title = "OVERMAN キングゲイナー 第3話「炸裂! オーバースキル」"

    assert extract_series_name(title) == "OVERMAN キングゲイナー"


def test_episode_suffix_title_is_treated_as_contaminated():
    from broadcast_frame_titles import is_contaminated_program_title

    assert is_contaminated_program_title("牙狼~語りし者~#23") is True


def test_leading_broadcast_label_preserves_long_dictionary_work(make_db):
    import run_metadata_batches_promptv1 as mod
    full = '機動戦士ガンダム 水星の魔女'
    con = make_db([], programs=['機動戦士ガンダム', full])
    dictionary = mod._ProgramDictionary(con)
    for label in ['【新】', '【再】【字】']:
        assert mod._match_from_known_programs(label + full + ' 第1話「魔女と花嫁」', dictionary, mod.HintSet()) == full


def test_quoted_leading_work_uses_curated_alias_in_both_routes(tmp_path):
    import run_metadata_batches_promptv1 as mod
    hints_file = tmp_path / 'aliases.yaml'
    hints_file.write_text('hints:\n  - canonical_title: Test Program\n    aliases: [Test Alias]\n')
    hints, _ = mod._load_hints_with_status(str(hints_file))
    for base in ['「Test Alias」第1話「Subtitle」', '『Test Alias』第1話「Subtitle」']:
        assert mod._match_from_known_programs(base, None, hints) == 'Test Program'
        assert extract_series_name(base, aliases_path=str(hints_file)) == 'Test Program'
