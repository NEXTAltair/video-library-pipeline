import json
from pathlib import Path

from unwatched_pipeline_runner import export_review_yaml_artifacts


def test_export_review_yaml_artifacts_aggregates_single_review_yaml(tmp_path) -> None:
    output_jsonl = tmp_path / "llm_filename_extract_output_0001_0013.jsonl"
    output_jsonl.write_text("", encoding="utf-8")

    def fake_runner(script: Path, args: list[str], cwd: str | None = None) -> str:
        assert script.name == "export_program_yaml.py"
        assert "--source-jsonl" in args
        return json.dumps(
            {
                "ok": True,
                "outputPath": str(tmp_path / "program_aliases_review_0001_0013.yaml"),
                "sourceJsonlPath": str(output_jsonl),
                "reviewSummary": {
                    "rowsNeedingReview": 13,
                    "requiredFieldMissingRows": 1,
                    "invalidAirDateRows": 0,
                    "needsReviewFlagRows": 13,
                    "suspiciousProgramTitleRows": 2,
                    "fieldCounts": {"program_title": 13},
                    "reasonCounts": {"suspicious_program_title": 13},
                },
                "reviewCandidates": [
                    {
                        "path": r"B:\VideoLibrary\番組A\2026\04\ep1.ts",
                        "columns": ["program_title"],
                        "reasons": ["suspicious_program_title"],
                    },
                ],
                "reviewCandidatesTruncated": False,
            },
            ensure_ascii=False,
        )

    result = export_review_yaml_artifacts([str(output_jsonl)], tmp_path / "export_program_yaml.py", cwd=str(tmp_path), runner=fake_runner)

    assert result["ok"] is True
    assert result["reviewYamlPath"] == str(tmp_path / "program_aliases_review_0001_0013.yaml")
    assert result["reviewYamlPaths"] == [str(tmp_path / "program_aliases_review_0001_0013.yaml")]
    assert result["reviewSummary"]["rowsNeedingReview"] == 13
    assert len(result["reviewCandidates"]) == 1


def test_export_review_yaml_artifacts_ignores_non_reviewable_batches(tmp_path) -> None:
    batch1 = tmp_path / "llm_filename_extract_output_0001_0013.jsonl"
    batch2 = tmp_path / "llm_filename_extract_output_0002_0001.jsonl"
    batch1.write_text("", encoding="utf-8")
    batch2.write_text("", encoding="utf-8")

    def fake_runner(script: Path, args: list[str], cwd: str | None = None) -> str:
        source = args[args.index("--source-jsonl") + 1]
        if source.endswith("0001_0013.jsonl"):
            return json.dumps(
                {
                    "ok": True,
                    "outputPath": str(tmp_path / "program_aliases_review_0001_0013.yaml"),
                    "sourceJsonlPath": source,
                    "reviewSummary": {
                        "rowsNeedingReview": 13,
                        "requiredFieldMissingRows": 0,
                        "invalidAirDateRows": 0,
                        "needsReviewFlagRows": 13,
                        "suspiciousProgramTitleRows": 0,
                        "fieldCounts": {"needs_review": 13},
                        "reasonCounts": {"needs_review_flagged": 13},
                    },
                    "reviewCandidates": [],
                    "reviewCandidatesTruncated": False,
                },
                ensure_ascii=False,
            )
        return json.dumps(
            {
                "ok": True,
                "outputPath": None,
                "sourceJsonlPath": source,
                "reviewSummary": {
                    "rowsNeedingReview": 0,
                    "requiredFieldMissingRows": 0,
                    "invalidAirDateRows": 0,
                    "needsReviewFlagRows": 0,
                    "suspiciousProgramTitleRows": 0,
                    "fieldCounts": {},
                    "reasonCounts": {},
                },
                "reviewCandidates": [],
                "reviewCandidatesTruncated": False,
                "skippedReason": "no_reviewable_rows",
            },
            ensure_ascii=False,
        )

    result = export_review_yaml_artifacts(
        [str(batch1), str(batch2)],
        tmp_path / "export_program_yaml.py",
        cwd=str(tmp_path),
        runner=fake_runner,
    )

    assert result["ok"] is True
    assert result["reviewYamlPaths"] == [str(tmp_path / "program_aliases_review_0001_0013.yaml")]
    assert result["reviewSummary"]["rowsNeedingReview"] == 13


def test_export_review_yaml_artifacts_reports_export_failure(tmp_path) -> None:
    batch = tmp_path / "llm_filename_extract_output_0001_0013.jsonl"
    batch.write_text("", encoding="utf-8")

    def fake_runner(script: Path, args: list[str], cwd: str | None = None) -> str:
        return json.dumps({"ok": False, "error": "boom"}, ensure_ascii=False)

    result = export_review_yaml_artifacts([str(batch)], tmp_path / "export_program_yaml.py", cwd=str(tmp_path), runner=fake_runner)

    assert result["ok"] is False
    assert result["error"] == f"failed to export review YAML for {batch}"
