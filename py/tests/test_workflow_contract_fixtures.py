import json
from pathlib import Path


FIXTURES_ROOT = Path(__file__).resolve().parents[2] / "tests" / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_ROOT / name).read_text(encoding="utf-8"))


def test_source_root_review_required_fixture_preserves_run_scoped_refs() -> None:
    payload = load_fixture("v2_workflow_result_source_root_review_required.json")

    assert payload["runId"] == "run_contract_source_review"
    assert payload["flow"] == "source_root"
    assert payload["phase"] == "review_required"
    assert payload["ok"] is False

    artifact_ids = {artifact["id"] for artifact in payload["artifacts"]}
    assert {"metadata_extract_output_0001", "metadata_review_yaml_0001"} <= artifact_ids

    review_gate = payload["gates"][0]
    assert review_gate["id"] == "metadata_review"
    assert review_gate["status"] == "open"
    assert review_gate["artifactIds"] == ["metadata_review_yaml_0001"]
    assert set(review_gate["artifactIds"]) <= artifact_ids

    review_action = payload["nextActions"][0]
    assert review_action["action"] == "review_metadata"
    assert review_action["tool"] == "video_pipeline_resume"
    assert review_action["requiresHumanInput"] is True
    assert review_action["params"] == {
        "runId": "run_contract_source_review",
        "gateId": "metadata_review",
        "artifactIds": ["metadata_review_yaml_0001"],
        "reviewYamlPaths": ["/ops/runs/run_contract_source_review/review/metadata_review_0001.yaml"],
        "resumeAction": "apply_reviewed_metadata",
    }

    review_artifact = next(
        artifact for artifact in payload["artifacts"] if artifact["id"] == "metadata_review_yaml_0001"
    )
    assert review_artifact["inputArtifactIds"] == ["metadata_extract_output_0001"]
    assert review_artifact["metadata"]["sourceJsonlPath"].endswith("llm_filename_extract_output_0001_0001.jsonl")
