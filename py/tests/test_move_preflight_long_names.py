"""A Windows long-name probe must preserve all ordinary preflight guards."""
import errno
from pathlib import Path

import pytest

from video_pipeline.domain.move_preflight import inspect_move

SRC = "N:\\Library\\" + "録画" * 80 + ".mp4"
DST = "N:\\Show\\" + "録画" * 80 + ".mp4"
MTIME = "2024-03-03T00:30:00.1234567Z"


def too_long(_self, **_kwargs):
    raise OSError(errno.ENAMETOOLONG, "File name too long")


def test_native_probe_preserves_exact_unicode_path_and_dotnet_time(monkeypatch):
    monkeypatch.setattr(Path, "stat", too_long)
    seen = []

    def probe(src, dst):
        seen.append((src, dst))
        return {"ok": True, "source_exists": True, "size_bytes": 42,
                "mtime_utc": MTIME, "destination_exists": False}

    pre, conflict = inspect_move(SRC, DST, {"size": 42, "mtimeUtc": MTIME}, windows_probe=probe)
    assert seen == [(SRC, DST)]
    assert conflict is None and pre["size_bytes"] == 42
    assert pre["mtime_utc"] == "2024-03-03T00:30:00.123456+00:00"


@pytest.mark.parametrize("changes,reason", [
    ({"source_exists": False}, "source_missing"),
    ({"size_bytes": 43}, "source_changed"),
    ({"mtime_utc": "2024-03-04T00:30:00Z"}, "source_changed"),
    ({"destination_exists": True}, "destination_exists"),
])
def test_native_probe_still_excludes_unsafe_moves(monkeypatch, changes, reason):
    monkeypatch.setattr(Path, "stat", too_long)
    observed = {"ok": True, "source_exists": True, "size_bytes": 42,
                "mtime_utc": MTIME, "destination_exists": False, **changes}
    assert inspect_move(SRC, DST, {"size": 42, "mtimeUtc": MTIME}, windows_probe=lambda *_: observed) == (None, reason)


@pytest.mark.parametrize("observed", [{"ok": False}, {"ok": True, "source_exists": True, "size_bytes": 42,
    "mtime_utc": MTIME, "destination_exists": None}])
def test_failed_or_ambiguous_native_probe_never_approves_move(monkeypatch, observed):
    monkeypatch.setattr(Path, "stat", too_long)
    with pytest.raises(RuntimeError):
        inspect_move(SRC, DST, {"size": 42, "mtimeUtc": MTIME}, windows_probe=lambda *_: observed)


def test_unrelated_io_errors_are_not_treated_as_long_name_success(monkeypatch):
    def denied(_self, **_kwargs):
        raise OSError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(Path, "stat", denied)
    with pytest.raises(PermissionError):
        inspect_move(SRC, DST, {}, windows_probe=lambda *_: pytest.fail("not a name-length failure"))
