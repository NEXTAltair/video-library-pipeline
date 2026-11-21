"""Owner series numbering (ゴジラ#03 …) inside curated series folders. #201"""
import pytest

from video_pipeline.domain.series_order import parse_series_order


@pytest.mark.parametrize("name, expected", [
    ("ゴジラ#03 [映][字]キングコング対ゴジラ", ("ゴジラ", 3, "[映][字]キングコング対ゴジラ")),
    ("レジェゴジ#01[映][Ss][字]Godzilla(2014)", ("レジェゴジ", 1, "[映][Ss][字]Godzilla(2014)")),
    ("レジェゴジ＃０２　[映]ゴジラ キング・オブ・モンスターズ", ("レジェゴジ", 2, "[映]ゴジラ キング・オブ・モンスターズ")),
    ("ゴジラ#28 ゴジラ　Ｆｉｎａｌ Ｗａｒｓ", ("ゴジラ", 28, "ゴジラ Final Wars")),
])
def test_parse_owner_series_order(name, expected):
    order = parse_series_order(name)
    assert (order.abbr, order.number, order.rest) == expected
    assert order.metadata_fields() == {"series_abbr": expected[0], "series_order": expected[1]}


@pytest.mark.parametrize("name", [
    "[映]ゴジラ#03 キングコング対ゴジラ",   # not leading
    "番組名 #03 サブタイトル",               # space before # is an episode marker style
    "Show#1.5 special",                     # decimal episode
    "ゴジラ（1954年）◆NECO初",              # no number at all
    "#03 タイトル",                          # no abbreviation
])
def test_non_owner_prefixes_are_not_series_order(name):
    assert parse_series_order(name) is None


def test_review_override_accepts_series_order_fields():
    from video_pipeline.application.reviewed_metadata import review_rows
    row = {"path_id": "g03", "path": "N:\\シリーズ_ゴジラ\\ゴジラ#03 [映]キングコング対ゴジラ2022-09-05-23：40.m4v",
           "program_title": "キングコング対ゴジラ", "air_date": "2022-09-05", "needs_review": False}
    fields = {"series_order": 3, "series_abbr": "ゴジラ", "review_note": "本人の通し番号 ゴジラ#03"}
    reviewed = review_rows([row], [], {"g03": fields})[0]
    assert (reviewed["series_order"], reviewed["series_abbr"]) == (3, "ゴジラ")
    for bad in ("3", -1, True, 3.0):
        with pytest.raises(ValueError):
            review_rows([row], [], {"g03": dict(fields, series_order=bad)})
