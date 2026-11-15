import errno
import os
from pathlib import Path
import pytest

from video_pipeline.domain.companion_bundle import capture_bundle, companion_paths, parent_move_succeeded


def fixture(tmp_path):
    root = tmp_path / 'src'
    root.mkdir()
    source = root / 'movie.mp4'
    source.write_bytes(b'video')
    target = tmp_path / 'dst' / 'renamed.mp4'
    def localize(s):
        return str(s).replace('\\', '/')
    return source, target, localize


def test_closed_empty_cohort_and_exact_signature(tmp_path):
    src, dst, localize = fixture(tmp_path)
    os.utime(src, ns=(1700000000123456700, 1700000000123456700))
    bundle = capture_bundle(str(src), str(dst), localize=localize)
    assert bundle['bundle_version'] == 1
    assert bundle['companion_cohort'] == []
    assert [m['kind'] for m in bundle['members']] == ['video']
    sig = bundle['members'][0]['precondition']
    assert sig == {'size_bytes': 5, 'mtime_ns': 1700000000123456700,
                   'mtime_utc': '2023-11-14T22:13:20.1234567+00:00'}


def test_recognized_companions_only_and_renaming(tmp_path):
    src, dst, localize = fixture(tmp_path)
    for suffix in ('.psc', '.vtt'):
        src.with_suffix(suffix).write_bytes(suffix.encode())
    for suffix in ('.program.txt', '.recording.json'):
        Path(str(src) + suffix).write_bytes(suffix.encode())
    Path(str(src) + '.psc').write_bytes(b'not recognized')
    bundle = capture_bundle(str(src), str(dst), localize=localize)
    assert set(bundle['companion_cohort']) == {'psc', 'vtt', 'program_txt', 'recording_json'}
    assert len(bundle['members']) == 5
    assert {m['kind']: Path(localize(m['dst'])).name for m in bundle['members']} == {
        'video': 'renamed.mp4', 'psc': 'renamed.psc', 'vtt': 'renamed.vtt',
        'program_txt': 'renamed.mp4.program.txt', 'recording_json': 'renamed.mp4.recording.json'}
    assert all('precondition' in m for m in bundle['members'])


@pytest.mark.parametrize('side', ['source', 'destination'])
def test_shared_stem_fails_closed(tmp_path, side):
    src, dst, localize = fixture(tmp_path)
    video = src if side == 'source' else dst
    video.parent.mkdir(exist_ok=True)
    video.with_suffix('.mkv').write_bytes(b'other video')
    with pytest.raises(ValueError, match='ambiguous_companion_stem'):
        capture_bundle(str(src), str(dst), localize=localize)


@pytest.mark.parametrize('where', ['source_member', 'destination_parent', 'source_parent'])
def test_reparse_equivalents_fail_closed(tmp_path, where):
    src, dst, localize = fixture(tmp_path)
    if where == 'source_member':
        src.with_suffix('.psc').symlink_to(tmp_path / 'missing')
    elif where == 'destination_parent':
        dst.parent.symlink_to(src.parent, target_is_directory=True)
    else:
        redirect = tmp_path / 'redirect'
        redirect.symlink_to(src.parent, target_is_directory=True)
        src = redirect / src.name
    with pytest.raises(ValueError, match='reparse_point'):
        capture_bundle(str(src), str(dst), localize=localize)


def test_destination_sidecar_collision_even_if_source_absent(tmp_path):
    src, dst, localize = fixture(tmp_path)
    dst.parent.mkdir()
    dst.with_suffix('.vtt').write_text('occupied')
    with pytest.raises(ValueError, match='destination_exists'):
        capture_bundle(str(src), str(dst), localize=localize)


def test_native_video_only_probe_cannot_prove_absence(tmp_path):
    def broken(_):
        raise OSError(errno.ENAMETOOLONG, 'long Windows component')
    with pytest.raises(ValueError, match='native_companion_capture_unavailable'):
        capture_bundle('C:\\movie.mp4', 'C:\\dst\\movie.mp4', localize=broken,
                       windows_probe=lambda *args: {'ok': True, 'source_exists': True})


def test_receipt_compatibility_and_bundle_completion():
    assert parent_move_succeeded({'ok': True})
    assert not parent_move_succeeded({'ok': True, 'bundle_version': 1})
    assert not parent_move_succeeded({'ok': True, 'bundle_version': 1, 'bundle_complete': False})
    assert parent_move_succeeded({'ok': True, 'bundle_version': 1, 'bundle_complete': True})
    assert not parent_move_succeeded({'ok': False, 'bundle_version': 1, 'bundle_complete': True})


def test_posix_companions_supported_without_custom_localizer(tmp_path):
    src, dst, _ = fixture(tmp_path)
    src.with_suffix('.psc').write_bytes(b'psc')
    bundle = capture_bundle(str(src), str(dst))
    assert bundle['companion_cohort'] == ['psc']
    assert bundle['members'][1]['src'] == str(src.with_suffix('.psc'))


def test_incomplete_parent_not_counted_successful(tmp_path):
    import json
    from video_pipeline.domain.move_apply_stats import aggregate_move_apply
    receipt = tmp_path / 'receipt.jsonl'
    receipt.write_text(json.dumps({'op': 'move', 'src': 'a', 'dst': 'b', 'ok': True,
                                   'bundle_version': 1, 'bundle_complete': False}) + '\n')
    stats = aggregate_move_apply(str(receipt))
    assert stats['succeeded'] == 0
    assert stats['failed'] == 1


def test_native_bundle_capture_contract():
    def broken(_):
        raise OSError(errno.ENAMETOOLONG, 'long Windows component')
    src, dst = r'C:\movie.mp4', r'C:\dst\movie.mp4'
    cohort = ['program_txt', 'psc']
    paths_src, paths_dst = companion_paths(src), companion_paths(dst)
    members = [{'kind': 'video', 'src': src, 'dst': dst,
                'precondition': {'size_bytes': 5, 'mtime_utc': '2026-01-01T00:00:00.1234567Z'}}]
    members += [{'kind': k, 'src': paths_src[k], 'dst': paths_dst[k],
                 'precondition': {'size_bytes': 1, 'mtime_utc': '2026-01-01T00:00:00.1234567Z'}} for k in cohort]
    native = {'ok': True, 'bundle_verified': True, 'bundle_version': 1,
              'companion_cohort': cohort, 'members': members}
    bundle = capture_bundle(src, dst, localize=broken, windows_probe=lambda *_: native)
    assert bundle['members'] == members
    native['companion_cohort'] = []
    with pytest.raises(ValueError, match='invalid_native_companion_cohort'):
        capture_bundle(src, dst, localize=broken, windows_probe=lambda *_: native)


def test_database_identity_rejects_incomplete_bundle_before_any_write():
    from video_pipeline.db.file_identity import record_move
    with pytest.raises(ValueError, match='incomplete'):
        record_move(None, {'src': 'a', 'dst': 'b', 'ok': True, 'bundle_version': 1}, 'receipt', 'run')


def test_default_native_probe_reuses_shipped_helpers(monkeypatch):
    import json
    from types import SimpleNamespace
    from video_pipeline.platform import native_companion_probe as native
    seen = []
    monkeypatch.setattr(native, "find_pwsh7_executable", lambda: "pwsh.exe")
    def invoke(command, **kwargs):
        seen.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "bundle_verified": True}), stderr="")
    monkeypatch.setattr(native.subprocess, "run", invoke)
    assert native.read_native_bundle(r"C:\日本語の録画.mp4", r"D:\Show\movie.mp4")["bundle_verified"]
    command, options = seen[0]
    assert "-NonInteractive" in command
    assert "function Get-MoveBundle" in options["input"]
    assert "function Convert-ToLongPathLiteral" in options["input"]
    assert "日本語の録画.mp4" in options["input"]
    assert ". (Join-Path $PSScriptRoot" not in options["input"]
    assert options["timeout"] == 120


def test_capture_does_not_enumerate_whole_library(tmp_path, monkeypatch):
    src, dst, localize = fixture(tmp_path)
    def no_scan(_):
        pytest.fail("bundle capture must use bounded exact sibling probes")
    monkeypatch.setattr(Path, "iterdir", no_scan)
    assert capture_bundle(str(src), str(dst), localize=localize)["companion_cohort"] == []
