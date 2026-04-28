import run_metadata_batches_promptv1 as mod


def test_load_hints_missing_file_is_ai_only(tmp_path):
    hints, status = mod._load_hints_with_status(str(tmp_path / "missing.yaml"))

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
