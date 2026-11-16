"""All-container discovery is separate from format-specific byte parsing."""
import json
import sys
from pathlib import Path, PureWindowsPath

import pytest

import ingest_inventory_jsonl
import make_metadata_queue_from_inventory as queue
import make_move_plan_from_inventory as planner
from edcb_program_parser import recording_stem_from_program_info
from mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.platform.media_extensions import VIDEO_EXTENSIONS
from video_pipeline.platform.pathscan_common import ensure_exts, scan_files
from video_pipeline.platform import artifact_retention as retention


@pytest.mark.parametrize('ext', VIDEO_EXTENSIONS)
def test_discovery_companion_identity_and_cache_protection(tmp_path, ext):
    media = tmp_path / ('Show_2026_11_01_21_00' + ext.upper())
    media.write_bytes(b'not a container: discovery must not parse MP4 boxes')
    (tmp_path / 'Show.program.txt').write_text('sidecar')
    reserved = tmp_path / '_AI_WORK'
    reserved.mkdir()
    (reserved / media.name).write_bytes(b'excluded')
    files, warnings, errors, _ = scan_files([str(tmp_path)], set(ensure_exts(None)), False, 4096, 0)
    assert not errors and not warnings
    assert [f.name for f in files] == [media.name]
    assert recording_stem_from_program_info(media.name + '.PROGRAM.TXT') == 'Show_2026_11_01_21_00'
    assert ext in retention.MEDIA_SUFFIXES


def test_explicit_override_is_not_broadened():
    assert ensure_exts(['MP4', '.MP4']) == ['.mp4']
    assert ensure_exts(['avi']) == ['.avi']


def test_inventory_ingest_queue_and_source_root_plan_all_video_containers(tmp_path, monkeypatch, capsys):
    db = tmp_path / 'db.sqlite'
    with connect_db(str(db)) as con:
        create_schema_if_needed(con)
    rows = [dict(path=rf'B:\Unwatched\Show_2026_11_01_21_00{ext.upper()}',
                 name='Show_2026_11_01_21_00' + ext.upper(), size=12) for ext in VIDEO_EXTENSIONS]
    rows += [dict(path=r'B:\Unwatched\Show.program.txt', name='Show.program.txt', size=1),
             dict(path=r'C:\Other\Outside.mkv', name='Outside.mkv', size=1)]
    inventory = tmp_path / 'inventory.jsonl'
    inventory.write_text('\n'.join(json.dumps(r) for r in rows) + '\n')
    monkeypatch.setattr(sys, 'argv', ['ingest', '--db', str(db), '--jsonl', str(inventory)])
    assert ingest_inventory_jsonl.main() == 0
    output = tmp_path / 'queue.jsonl'
    monkeypatch.setattr(sys, 'argv', ['queue', '--db', str(db), '--inventory', str(inventory),
                                    '--source-root', r'B:\Unwatched', '--out', str(output)])
    assert queue.main() == 0
    queued = [json.loads(s) for s in output.read_text().splitlines()][1:]
    assert {PureWindowsPath(r['path']).suffix.lower() for r in queued} == set(VIDEO_EXTENSIONS)
    with connect_db(str(db)) as con:
        for row in queued:
            con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,needs_review,human_reviewed) VALUES(?,'human_reviewed','{}','now','Show','2026-11-01',0,1)", (row['path_id'],))
        con.commit()
    # Only the Windows filesystem boundary is simulated. No production media is read or moved.
    monkeypatch.setattr(planner, 'inspect_move', lambda *a, **kw: ({'size_bytes': 12, 'mtime_utc': 'now'}, None))
    plan = tmp_path / 'plan.jsonl'
    monkeypatch.setattr(sys, 'argv', ['plan', '--db', str(db), '--inventory', str(inventory),
                                    '--source-root', r'B:\Unwatched', '--dest-root', r'D:\Library', '--out', str(plan)])
    assert planner.main() == 0
    planned = [json.loads(s) for s in plan.read_text().splitlines()][1:]
    assert len(planned) == len(VIDEO_EXTENSIONS)
    assert {PureWindowsPath(r['dst']).suffix.lower() for r in planned} == set(VIDEO_EXTENSIONS)
    assert all(r['dst'].startswith('D:\\Library\\') for r in planned)


def test_typescript_defaults_match_python_and_bundled_rules_do_not_narrow():
    root = Path(__file__).resolve().parents[2]
    ts = (root / 'src/tools/media-extensions.ts').read_text()
    assert json.loads(ts.split('= ', 1)[1].rstrip(';\n')) == list(VIDEO_EXTENSIONS)
    for tool in ['tool-backfill.ts', 'tool-relocate.ts', 'tool-prepare-relocate-metadata.ts']:
        assert 'default: VIDEO_EXTENSIONS' in (root / 'src/tools' / tool).read_text()
    assert '\nextensions:' not in (root / 'rules/relocate_roots.yaml').read_text()


@pytest.mark.parametrize('module_name', ['backfill_moved_files', 'relocate_existing_files'])
@pytest.mark.parametrize('override', [None, ['.mp4']])
def test_scan_cli_defaults_and_explicit_override(tmp_path, monkeypatch, capsys, module_name, override):
    import importlib
    cli = importlib.import_module(module_name)
    db = tmp_path / 'db.sqlite'
    with connect_db(str(db)) as con:
        create_schema_if_needed(con)
    media = tmp_path / 'media'
    media.mkdir()
    for ext in VIDEO_EXTENSIONS:
        (media / ('Show' + ext.upper())).write_bytes(b'container bytes')
    captured = []
    def local_scan(**kwargs):
        # Real recursive scanner, replacing only the Windows transport boundary.
        captured.extend(sorted(kwargs['exts']))
        kwargs['roots'] = [str(media)]
        kwargs['windows_ops_root'] = None
        return scan_files(**kwargs)
    monkeypatch.setattr(cli, 'scan_files', local_scan)
    argv = [module_name, '--db', str(db), '--windows-ops-root', str(tmp_path / 'ops'),
            '--dest-root', r'D:\Library', '--roots-json', '["B:\\\\Unwatched"]']
    if override:
        argv += ['--extensions-json', json.dumps(override)]
    monkeypatch.setattr(sys, 'argv', argv)
    assert cli.main() == 0
    summary = json.loads(capsys.readouterr().out)
    assert captured == sorted(override or VIDEO_EXTENSIONS)
    assert summary['scannedFiles'] == len(override or VIDEO_EXTENSIONS)
    assert {p.suffix.lower() for p in media.iterdir()} == set(VIDEO_EXTENSIONS)


@pytest.mark.parametrize('ext', VIDEO_EXTENSIONS)
def test_media_never_treated_as_disposable_cache(tmp_path, ext):
    job = retention.create_workspace(tmp_path, 'extension_check', cache_days=0)
    (job / 'cache' / ('source' + ext.upper())).write_bytes(b'recording')
    retention.finish_workspace(job)
    plan = retention.plan_cleanup(tmp_path)
    assert plan['jobs'] == 0
    assert plan['protected']
