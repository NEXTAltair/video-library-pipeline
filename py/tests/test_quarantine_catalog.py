import base64
import gzip
import hashlib
import json
from pathlib import Path
import re

import pytest

from video_pipeline.platform.quarantine_catalog import (
    CATALOG_PREVIEW_MAX_BYTES, create_catalog, render_catalog, verified_population,
)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def verified(tmp_path):
    ops = tmp_path / "ops"
    directory = ops / "duplicates" / "runs" / "dedup_test"
    directory.mkdir(parents=True)
    root = r"B:\_AI_WORK\duplicates\quarantine_v3"
    destination = root + r"\group\repeat\pathid\番組 #1 & <終>.ts"
    move = {"op": "move", "src": r"B:\library\episode.ts", "dst": destination,
            "file_id": "instance", "precondition": {"size_bytes": 25, "mtime_ns": 100}}
    write_json(directory / "plan.json", {"moves": [move]})
    digest = hashlib.sha256((directory / "plan.json").read_bytes()).hexdigest()
    write_json(directory / "manifest.json", {"phase": "complete", "plan_checksum": digest})
    (directory / "move_plan.jsonl").write_text(json.dumps(move) + "\n")
    receipt = {**move, "ok": True}
    (directory / "move_receipts.jsonl").write_text(json.dumps(receipt) + "\n")
    write_json(directory / "intent.json", {"run_id": "dedup_test", "plan_path": str(directory / "move_plan.jsonl"),
               "receipt_path": str(directory / "move_receipts.jsonl"), "operations": 1,
               "checksum": hashlib.sha256((directory / "move_plan.jsonl").read_bytes()).hexdigest()})
    proof = {"ok": True, "delete_ready": True, "directories": {root: {"files": 1, "bytes": 25}},
             "run_summary": [{"run_id": "dedup_test", "manifest_checksum": digest, "moved": 1}]}
    evidence = tmp_path / "evidence"
    write_json(evidence / "proof.json", proof)
    handoff = {"ok": True, "task_scope_complete": True, "proof_path": "/obsolete/artifacts/proof.json",
               "proof_sha256": hashlib.sha256((evidence / "proof.json").read_bytes()).hexdigest(),
               "directories": proof["directories"], "verified_quarantine_files": 1, "verified_quarantine_bytes": 25,
               "unresolved_groups_preserved": 80, "unresolved_files_preserved": 160, "verified_at": "2026-10-08"}
    path = evidence / "handoff.json"
    write_json(path, handoff)
    files = [{"root": root, "path": destination, "size_bytes": 25, "mtime_ns": 100}]
    return path, ops, handoff, files


def test_flat_report_keeps_original_data_and_escapes_names(verified, tmp_path):
    path, ops, handoff, files = verified
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    result = create_catalog(path, ops, tmp_path / "view.html", census=lambda roots: files)
    assert result["files"] == 1 and result["mediaOperations"] == result["databaseWrites"] == 0
    content = (tmp_path / "view.html").read_text()
    assert '番組 #1 &amp; &lt;終&gt;.ts' in content and '<終>' not in content
    assert '80組160本' in content and 'id="search"' in content and 'id="drive"' in content
    assert '%23' in content and '%3C' in content
    assert all(p.read_bytes() == data for p, data in before.items())
    assert result["htmlBytes"] == len(content.encode("utf-8"))


def test_full_recording_population_fits_preview_without_losing_paths(verified):
    _, _, handoff, files = verified
    rows = []
    for index in range(1227):
        token = hashlib.sha256(str(index).encode()).hexdigest()[:32]
        path = (files[0]["root"] + "\\" + token + "\\repeat\\" + token + "\\"
                + f"番組名の長い録画・放送回・出演者・番組説明_第{index}話 #1 & <終>_2026_01_01.ts")
        rows.append({**files[0], "path": path, "size_bytes": 2_000_000_000 + index})
    content = render_catalog(handoff, rows)
    assert len(content.encode("utf-8")) <= CATALOG_PREVIEW_MAX_BYTES
    packed = re.search(r'<script id="catalog-data" type="application/octet-stream">(.*?)</script>', content).group(1)
    decoded = json.loads(gzip.decompress(base64.b64decode(packed)))
    assert len(decoded) == 1227
    assert {path: size for path, size in decoded} == {row["path"]: row["size_bytes"] for row in rows}
    assert '<終>' not in content


@pytest.mark.parametrize("change", ["add", "remove", "duplicate", "size", "time", "drive"])
def test_changed_population_does_not_publish_deletion_view(verified, tmp_path, change):
    path, ops, handoff, files = verified
    altered = [dict(row) for row in files]
    if change == "add": altered.append({**files[0], "path": files[0]["path"] + ".extra"})
    elif change == "remove": altered.clear()
    elif change == "duplicate": altered *= 2
    elif change == "size": altered[0]["size_bytes"] += 1
    elif change == "time": altered[0]["mtime_ns"] += 1
    elif change == "drive": altered[0]["root"] = r"D:\_AI_WORK\duplicates\quarantine_v3"
    with pytest.raises(ValueError):
        create_catalog(path, ops, tmp_path / "bad.html", census=lambda roots: altered)
    assert not (tmp_path / "bad.html").exists()


def test_proof_checksum_is_required_after_archival(verified):
    path, ops, _, _ = verified
    proof = path.parent / "proof.json"
    proof.write_text(proof.read_text() + " ")
    with pytest.raises(ValueError, match="checksum"):
        verified_population(path, ops)


def test_receipt_replay_or_identity_change_blocks_catalogue(verified, tmp_path):
    path, ops, _, files = verified
    receipt = ops / "duplicates/runs/dedup_test/move_receipts.jsonl"
    row = json.loads(receipt.read_text())
    row["file_id"] = "different"
    receipt.write_text(json.dumps(row) + "\n")
    with pytest.raises(ValueError, match="receipt"):
        create_catalog(path, ops, tmp_path / "bad.html", census=lambda roots: files)


@pytest.mark.parametrize("target", ["existing", "run", "quarantine", "case_variant_quarantine", "case_variant_run"])
def test_report_cannot_overwrite_or_pollute_deletable_or_audit_roots(verified, tmp_path, target):
    path, ops, _, files = verified
    output = tmp_path / "view.html"
    if target == "existing": output.write_text("keep")
    elif target == "run": output = ops / "duplicates/runs/dedup_test/view.html"
    elif target == "case_variant_run": output = ops / "duplicates/RUNS/dedup_test/view.html"
    elif target == "case_variant_quarantine": output = Path("/mnt/b/_ai_work/DUPLICATES/QUARANTINE_V3/view.html")
    else: output = Path("/mnt/b/_AI_WORK/duplicates/quarantine_v3/view.html")
    with pytest.raises((ValueError, FileExistsError)):
        create_catalog(path, ops, output, census=lambda roots: files)
    if target == "existing": assert output.read_text() == "keep"
