import ingest_program_txt
import pytest
from series_name_extractor import extract_series_name


@pytest.mark.parametrize("title, expected", [
    ("ドラマ24孤独のグルメSeason11 5話 横浜上飯田の料理", "孤独のグルメSeason11"),
    ("ドラマ24 孤独のグルメSeason11! 第12話 高砂のかつ煮", "孤独のグルメSeason11"),
    ("日曜劇場「VIVANT」第23話 最終決戦", "VIVANT"),
    ("ドラマ24「別の作品」第2話", "別の作品"),
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
