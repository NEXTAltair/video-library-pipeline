import json

from export_program_yaml import default_output_path_for_source, generate_review_yaml


def write_jsonl(path, rows):
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")


def test_default_output_path_uses_program_aliases_prefix_for_extract_outputs(tmp_path) -> None:
    source = tmp_path / "llm_filename_extract_output_0001_0013.jsonl"
    assert default_output_path_for_source(str(source)) == str(tmp_path / "program_aliases_review_0001_0013.yaml")


def test_generate_review_yaml_creates_yaml_and_review_summary(tmp_path) -> None:
    source = tmp_path / "llm_filename_extract_output_0001_0013.jsonl"
    write_jsonl(source, [
        {
            "path_id": "p1",
            "path": r"B:\VideoLibrary\番組A\2026\04\ep1.ts",
            "program_title": "番組A",
            "air_date": "2026-04-07",
            "subtitle": "第1話",
            "needs_review": True,
            "needs_review_reason": "suspicious_program_title",
            "evidence": {"raw": "番組A 2026_04_07.ts"},
        },
        {
            "path_id": "p2",
            "path": r"B:\VideoLibrary\番組B\2026\04\ep2.ts",
            "program_title": "UNKNOWN",
            "air_date": "2026-04-08",
            "subtitle": None,
            "needs_review": True,
            "needs_review_reason": "unknown_program_title",
            "evidence": {"raw": "UNKNOWN.ts"},
        },
    ])

    result = generate_review_yaml(str(source), only_if_reviewable=True)

    assert result["ok"] is True
    assert result["outputPath"] == str(tmp_path / "program_aliases_review_0001_0013.yaml")
    assert result["reviewSummary"]["rowsNeedingReview"] == 2
    assert result["reviewSummary"]["needsReviewFlagRows"] == 2
    yaml_text = (tmp_path / "program_aliases_review_0001_0013.yaml").read_text(encoding="utf-8")
    assert "source_jsonl:" in yaml_text
    assert '"番組A"' in yaml_text
    assert '"UNKNOWN"' not in yaml_text


def test_generate_review_yaml_skips_output_when_no_reviewable_rows(tmp_path) -> None:
    source = tmp_path / "llm_filename_extract_output_0002_0001.jsonl"
    write_jsonl(source, [
        {
            "path_id": "p1",
            "path": r"B:\VideoLibrary\番組A\2026\04\ep1.ts",
            "program_title": "番組A",
            "air_date": "2026-04-07",
            "subtitle": "第1話",
            "needs_review": False,
            "needs_review_reason": "",
            "evidence": {"raw": "番組A 2026_04_07.ts"},
        },
    ])

    result = generate_review_yaml(str(source), only_if_reviewable=True)

    assert result["ok"] is True
    assert result["outputPath"] is None
    assert result["skippedReason"] == "no_reviewable_rows"

