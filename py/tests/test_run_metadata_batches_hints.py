import run_metadata_batches_promptv1 as mod
import hashlib
import json
import sys

import pytest


def test_load_hints_missing_file_is_ai_only_and_stdout_clean(tmp_path, capsys):
    hints, status = mod._load_hints_with_status(str(tmp_path / "missing.yaml"))

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "W hints file missing" in captured.err
    assert not hints
    assert status == {
        "hintsPath": str(tmp_path / "missing.yaml"),
        "hintsFilePresent": False,
        "hintsParserAvailable": mod.yaml is not None,
        "hintsLoadable": False,
        "hintsLoaded": False,
        "hintsLoadError": None,
        "hintsAliasCount": 0,
        "hintsRegexRulesCount": 0,
    }


def test_load_hints_valid_json_reports_counts(tmp_path):
    hints_path = tmp_path / "program_aliases.json"
    hints_path.write_text(
        """{
  "hints": [
    {
      "canonical_title": "Test Program",
      "aliases": ["Test Alias"]
    }
  ],
  "rules": [
    {
      "match": { "regex": "^Test", "field": "base" },
      "set": { "program_title": "Test Program" }
    }
  ]
}
""",
        encoding="utf-8",
    )

    hints, status = mod._load_hints_with_status(str(hints_path))

    assert hints.alias_map[mod._normalize_alias_key("Test Alias")] == "Test Program"
    assert status["hintsFilePresent"] is True
    assert status["hintsLoadable"] is True
    assert status["hintsLoaded"] is True
    assert status["hintsLoadError"] is None
    assert status["hintsAliasCount"] == 2
    assert status["hintsRegexRulesCount"] == 1


def test_load_hints_existing_yaml_requires_parser(tmp_path, monkeypatch):
    hints_path = tmp_path / "program_aliases.yaml"
    hints_path.write_text("hints: []\n", encoding="utf-8")
    monkeypatch.setattr(mod, "yaml", None)

    try:
        mod._load_hints_with_status(str(hints_path))
    except mod.HintLoadFailure as exc:
        status = exc.status
    else:
        raise AssertionError("expected HintLoadFailure")

    assert status["hintsFilePresent"] is True
    assert status["hintsParserAvailable"] is False
    assert status["hintsLoadable"] is False
    assert status["hintsLoaded"] is False
    assert "PyYAML is required" in status["hintsLoadError"]


def test_load_hints_invalid_yaml_is_hard_failure(tmp_path, monkeypatch):
    class BrokenYaml:
        @staticmethod
        def safe_load(_file):
            raise ValueError("bad yaml")

    hints_path = tmp_path / "program_aliases.yaml"
    hints_path.write_text("hints: []\n", encoding="utf-8")
    monkeypatch.setattr(mod, "yaml", BrokenYaml)

    try:
        mod._load_hints_with_status(str(hints_path))
    except mod.HintLoadFailure as exc:
        status = exc.status
    else:
        raise AssertionError("expected HintLoadFailure")

    assert status["hintsFilePresent"] is True
    assert status["hintsParserAvailable"] is True
    assert status["hintsLoadable"] is False
    assert status["hintsLoaded"] is False
    assert "bad yaml" in status["hintsLoadError"]


def test_program_dictionary_skips_broadcast_frame_titles(make_db):
    con = make_db(
        [],
        programs=[
            "BS11ガンダムアワー",
            "BS11ガンダムアワー 機動戦士ガンダム 水星の魔女 第9話",
            "機動戦士ガンダム 水星の魔女",
        ],
    )

    program_dict = mod._ProgramDictionary(con)
    result = program_dict.match(
        mod._normalize_title_compare(
            "BS11ガンダムアワー 機動戦士ガンダム 水星の魔女 第9話 「あと一歩、キミに踏み出せたなら」"
        )
    )

    assert result == "機動戦士ガンダム 水星の魔女"


@pytest.mark.parametrize("suffix", ["2024-03-10-09:30", "2024-03-10-09：30", "２０２４-０３-１０-０９：３０"])
def test_hyphen_colon_recording_date_is_used_and_removed_from_title(suffix):
    name = f'爆上戦隊ブンブンジャー「情報屋は認めない」{suffix}.mp4'
    assert mod.extract_air_date(name) == "2024-03-10"
    assert mod.strip_suffix(name) == '爆上戦隊ブンブンジャー「情報屋は認めない」'


def test_output_only_extracts_dates_flags_mtime_and_never_changes_database(tmp_path, monkeypatch, capsys):
    from mediaops_schema import connect_db, create_schema_if_needed
    import upsert_path_metadata_jsonl

    db = tmp_path / "mediaops.sqlite"
    con = connect_db(str(db))
    create_schema_if_needed(con)
    con.close()
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    queue = tmp_path / "queue.jsonl"
    queue.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in [
        {"path_id": "p1", "path": r"N:\Show2024-03-10-09：30.mp4",
         "name": "Show2024-03-10-09：30.mp4", "mtime_utc": "2024-03-31T00:00:00Z"},
        {"path_id": "p2", "path": r"N:\Show.mp4", "name": "Show.mp4",
         "mtime_utc": "2024-03-31T00:00:00Z"},
    ]) + "\n", encoding="utf-8")

    def reject_upsert():
        raise AssertionError("output-only extraction must not call metadata upsert")

    monkeypatch.setattr(upsert_path_metadata_jsonl, "main", reject_upsert)
    monkeypatch.setattr(sys, "argv", ["extract", "--db", str(db), "--queue", str(queue),
                                    "--outdir", str(tmp_path / "out"), "--output-only"])
    assert mod.main() == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["outputOnly"] is True
    assert summary["processed"] == 2
    rows = [json.loads(line) for line in (tmp_path / "out" / "llm_filename_extract_output_0001_0002.jsonl").read_text().splitlines()]
    assert rows[0]["air_date"] == "2024-03-10"
    assert "air_date_from_mtime" not in (rows[0]["needs_review_reason"] or "")
    assert rows[1]["needs_review"] is True
    assert "air_date_from_mtime" in rows[1]["needs_review_reason"]
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
