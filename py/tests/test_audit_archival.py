import gzip
import json
import os

import pytest

from rotate_move_audit_logs import _gzip_file, main


@pytest.mark.parametrize("existing", ["different", "truncated"])
def test_existing_bad_archive_never_erases_original(tmp_path, existing):
    source = tmp_path / "audit.jsonl"
    source.write_bytes(b"original audit\n")
    archive = source.with_suffix(".jsonl.gz")
    if existing == "different":
        archive.write_bytes(gzip.compress(b"other data"))
    else:
        archive.write_bytes(b"partial gzip")
    with pytest.raises((ValueError, OSError, EOFError)):
        _gzip_file(source)
    assert source.read_bytes() == b"original audit\n"


def test_lossless_archive_can_be_restored(tmp_path):
    source = tmp_path / "audit.jsonl"
    original = b'[{"receipt":"verified"}]\n' * 1000
    source.write_bytes(original)
    archived = _gzip_file(source, tmp_path / "archive" / "audit.jsonl.gz")
    assert not source.exists()
    assert gzip.decompress(archived.read_bytes()) == original


def test_default_preview_and_apply_never_prune_old_archives_or_unknown_data(tmp_path, capsys):
    move, llm = tmp_path / "move", tmp_path / "llm"
    move.mkdir(); llm.mkdir()
    source = move / "move_apply_old.jsonl"
    source.write_bytes(b"receipt\n")
    unknown = llm / "review_pending.jsonl"
    unknown.write_bytes(b"human review\n")
    archive = move / "archive"
    archive.mkdir()
    old = archive / "prior.jsonl.gz"
    old.write_bytes(gzip.compress(b"must survive ttl"))
    os.utime(old, (0, 0))
    args = ["--move-dir", str(move), "--llm-dir", str(llm), "--keep-batches", "0", "--ttl-days", "0"]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["preview"]
    assert source.exists() and not (llm / "archive").exists()
    assert main([*args, "--apply"]) == 0
    body = json.loads(capsys.readouterr().out)
    assert body["permanentlyDeleted"] == 0 and body["ttlDeletionDisabled"]
    assert not source.exists()
    assert gzip.decompress((archive / "move_apply_old.jsonl.gz").read_bytes()) == b"receipt\n"
    assert gzip.decompress(old.read_bytes()) == b"must survive ttl"
    assert unknown.read_bytes() == b"human review\n"
