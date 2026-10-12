import os

import pytest

from video_pipeline.platform import legacy_artifact_archive as archive


def fixture(tmp_path):
    root = tmp_path / "plugin" / "artifacts"
    root.mkdir(parents=True)
    (root / "final.json").write_bytes(b'[{"audit":"unchanged"}]')
    (root / "before.sqlite").write_bytes(b"recovery backup")
    (root / "images").mkdir()
    (root / "images" / "frame.jpg").write_bytes(b"raw frame")
    (root / "review-reference.json").symlink_to("final.json")
    destination = tmp_path / "state" / "audit-archive" / "legacy_001"
    return root, destination


def test_whole_archive_is_lossless_preserves_links_and_can_restore(tmp_path):
    root, destination = fixture(tmp_path)
    original = archive.archive_snapshot(root)
    plan = archive.plan_archive(root, destination)
    assert root.exists() and not destination.exists()
    assert (plan["files"], plan["links"]) == (3, 1)
    result = archive.apply_archive(root, plan["planPath"])
    assert not root.exists()
    assert archive.archive_snapshot(destination) == original
    assert os.readlink(destination / "review-reference.json") == "final.json"
    preview = archive.restore_archive(root, result["receiptPath"])
    assert preview["preview"] and not root.exists()
    archive.restore_archive(root, result["receiptPath"], apply=True)
    assert archive.archive_snapshot(root) == original
    with pytest.raises(ValueError, match="already restored"):
        archive.restore_archive(root, result["receiptPath"], apply=True)


def test_archive_refuses_capture_tree_destinations_stale_data_and_overwrite(tmp_path):
    root, destination = fixture(tmp_path)
    with pytest.raises(ValueError, match="outside"):
        archive.plan_archive(root, root.parent / "archive")
    plan = archive.plan_archive(root, destination)
    (root / "late.json").write_bytes(b"arrived after planning")
    with pytest.raises(ValueError, match="changed"):
        archive.apply_archive(root, plan["planPath"])
    assert root.is_dir() and not destination.exists()
    fresh = archive.plan_archive(root, destination)
    result = archive.apply_archive(root, fresh["planPath"])
    root.mkdir()
    (root / "new-generation.json").write_bytes(b"never overwrite")
    with pytest.raises(ValueError, match="occupied"):
        archive.restore_archive(root, result["receiptPath"], apply=True)
    assert (root / "new-generation.json").read_bytes() == b"never overwrite"


def test_restore_handles_interrupted_rename_and_prohibits_reapply(tmp_path, monkeypatch):
    root, destination = fixture(tmp_path)
    plan = archive.plan_archive(root, destination)
    original_writer = archive.write_json

    def interrupted(path, body):
        if body.get("status") == "archived":
            raise OSError("after rename interruption")
        return original_writer(path, body)

    monkeypatch.setattr(archive, "write_json", interrupted)
    with pytest.raises(OSError, match="interruption"):
        archive.apply_archive(root, plan["planPath"])
    receipt = next((destination.parent / ".maintenance" / "archive-receipts").glob("*.json"))
    monkeypatch.setattr(archive, "write_json", original_writer)
    with pytest.raises(ValueError, match="already started"):
        archive.apply_archive(root, plan["planPath"])
    archive.restore_archive(root, receipt, apply=True)
    assert root.exists() and not destination.exists()


def test_interrupted_restore_can_be_reconciled_without_replaying_a_move(tmp_path, monkeypatch):
    root, destination = fixture(tmp_path)
    plan = archive.plan_archive(root, destination)
    result = archive.apply_archive(root, plan["planPath"])
    writer = archive.write_json

    def interrupted(path, body):
        if body.get("status") == "restored":
            raise OSError("restore receipt interrupted")
        return writer(path, body)

    monkeypatch.setattr(archive, "write_json", interrupted)
    with pytest.raises(OSError, match="receipt interrupted"):
        archive.restore_archive(root, result["receiptPath"], apply=True)
    monkeypatch.setattr(archive, "write_json", writer)
    assert archive.restore_archive(root, result["receiptPath"], apply=True)["alreadyAtOriginal"]
