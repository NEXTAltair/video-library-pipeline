import ingest_program_txt
from series_name_extractor import extract_series_name


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
