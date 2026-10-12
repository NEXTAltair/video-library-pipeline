"""Date/edition preservation, cache freshness and multi-keeper apply boundaries."""
import csv
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path

import pytest

from .test_recording_dedup import library, add_recording
from video_pipeline.domain.date_recording_dedup import plan_by_date, recording_stamp
from video_pipeline.domain.recording_dedup import file_signature, quarantine_root
from video_pipeline.workflows.date_dedup import make_date_plan, validate_date_plan


def evidence(library, entries):
    con,db,root,ops = library
    folder = root.parent/'evidence';folder.mkdir()
    files,probes,rows = [],[],[]
    for i,(name,episodes,duration,subtitle) in enumerate(entries):
        path = root/name
        sig = file_signature(path)
        files.append({'id':i,'path':str(path),'name':name,'size':sig['size_bytes'],
                      'mtimeUtc':sig['mtime_utc'],'title':'Example Show','episodes':episodes,'subtitle':subtitle})
        probes.append({'id':i,'ok':True,'duration':duration,'width':1280,'height':720})
        stamp = recording_stamp(name)
        rows.append({'実パス':str(path),'録画日時_JST(ファイル名末尾)':stamp.strftime('%Y-%m-%d %H:%M'),
                     '放送日による区分':'初回放送終了後' if stamp.year>2020 else '初回放送期間内(本放送候補として保護)'})
    (folder/'candidate-index.json').write_text(json.dumps({'files':files,'same_episode_set_groups':[{'file_ids':list(range(len(files)))}],
        'overlap_title_groups':[],'scope':{'roots':[{'root':str(root),'available':True}]}}))
    (folder/'merged-video-probes.json').write_text(json.dumps(probes))
    (folder/'broadcast-date-sources.json').write_text(json.dumps({'periods':[{'title':'Example Show','start':'2020-01-01','end':'2020-12-31','url':'https://example.test/official'}]}))
    (folder/'batch-duration-anomalies.json').write_text('[]')
    with (folder/'dated-recordings.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    return folder


def recording(library,name,ep='1',size=200,subtitle='Opening'):
    return add_recording(library,name,None,ep=ep,size=size,subtitle=subtitle)


def test_date_protects_originals_and_dedups_only_later_cohort(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('original 2020 06 01 10 00.mp4',[1],1500,'Opening'),
             ('repeat-high 2024 01 01 10 00.mp4',[1],1500,'Opening'),
             ('repeat-low 2024 02 01 10 00.mp4',[1],1500,'Opening')]
    for name,_,_,_ in entries: recording(library,name,size=400 if 'high' in name else 200)
    folder=evidence(library,entries)
    before=library[1].read_bytes()
    plan=plan_by_date(library[0],[library[2]],folder)
    assert len(plan['moves'])==1 and 'repeat-low' in plan['moves'][0]['src']
    assert len(plan['protected_originals'])==1
    assert library[1].read_bytes()==before and plan['video_content_reads']==0
    assert plan['cleanup_complete'] is False  # Previewed moves have not happened.


def test_unknown_period_and_stale_probe_never_move(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('a 2024 01 01 10 00.mp4',[1],1500,'Opening'),('b 2024 02 01 10 00.mp4',[1],1500,'Opening')]
    for name,_,_,_ in entries: recording(library,name)
    folder=evidence(library,entries)
    (library[2]/entries[1][0]).write_bytes(b'changed')
    plan=plan_by_date(library[0],[library[2]],folder)
    assert plan['moves']==[] and 'discovery_or_cached_probe_stale' in plan['defer_reasons']
    (folder/'broadcast-date-sources.json').write_text('{"periods":[]}')
    assert plan_by_date(library[0],[library[2]],folder)['moves']==[]


@pytest.mark.parametrize('label', ['#2530#25', '#25-30#25', '#25〜30#25'])
def test_collapsed_omnibus_range_cannot_be_compared_as_one_episode(library, monkeypatch, label):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy, 'wsl_to_windows_path', str)
    entries = [('single #25 2024 01 01 10 00.mp4', [25], 1800, 'Opening'),
               (f'＜深夜一挙放送＞Example Show {label} 2024 02 01 10 00.mp4',
                [25], 10800, 'Opening')]
    for name, _, _, _ in entries:
        recording(library, name, ep='25')
    plan = plan_by_date(library[0], [library[2]], evidence(library, entries))
    assert plan['moves'] == []
    assert plan['defer_reasons'] == {'filename_omnibus_episode_scope_conflict': 1}
    assert plan['holds'][0]['path'].endswith(entries[1][0])
    assert plan['unresolved_same_episode_groups'] == []
    assert plan['candidate_review_complete'] is False


def test_omnibus_slot_with_matching_single_label_remains_actionable(library, monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy, 'wsl_to_windows_path', str)
    entries = [('single #25 2024 01 01 10 00.mp4', [25], 1800, 'Opening'),
               ('＜深夜一挙放送＞Example Show #25 2024 02 01 10 00.mp4',
                [25], 1800, 'Opening')]
    for name, _, _, _ in entries:
        recording(library, name, ep='25')
    plan = plan_by_date(library[0], [library[2]], evidence(library, entries))
    assert len(plan['moves']) == 1
    assert plan['holds'] == []


@pytest.mark.parametrize('name,episodes,expected', [
    ('新機動戦記ガンダムW #37 ゼロVSエピオン  2026 03 11 15 00.mp4', [37], 'ゼロVSエピオン'),
    ('Example Show #37第37話「ゼロVSエピオン」2025_09_26_19_30.mp4', [37], 'ゼロVSエピオン'),
    ('Example Show #25#25「王と民の戦い」（2023年／全50話）▽出演者2024-05-07-16：30.mp4', [25], '王と民の戦い'),
    ('Example Show #38 ゼロVSエピオン 2026 03 11 15 00.mp4', [37], ''),
    ('Example Show #37#38 ゼロVSエピオン 2026 03 11 15 00.mp4', [37], ''),
    ('Example Show #37 放送時間変更 出演者 2026 03 11 15 00.mp4', [37], ''),
    ('Example Show「ゼロVSエピオン」 #37 2026 03 11 15 00.mp4', [37], ''),
    ('Example Show #37「A」「B」2026 03 11 15 00.mp4', [37], ''),
    ('Example Show #37 ゼロVSエピオン 2026 02 31 15 00.mp4', [37], ''),
    ('Example Show #37 ゼロVSエピオン 2026 03 11 15 00.mp4', [37, 38], ''),
])
def test_literal_filename_subtitle_excludes_other_episode_and_synopsis(name, episodes, expected):
    from video_pipeline.domain.date_recording_dedup import filename_single_episode_subtitle
    assert filename_single_episode_subtitle(name, episodes) == expected


@pytest.mark.parametrize('observed', ['', '#1'])
def test_missing_discovery_label_recovered_from_literal_filename_without_db_edit(library, monkeypatch, observed):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy, 'wsl_to_windows_path', str)
    entries = [('Example Show #1 Opening 2024 01 01 10 00.mp4', [1], 1500, observed),
               ('Example Show #1「Opening」2024 02 01 10 00.mp4', [1], 1500, 'Opening')]
    for name, _, _, _ in entries:
        recording(library, name)
    folder = evidence(library, entries)
    before_db = library[1].read_bytes()
    before_index = (folder / 'candidate-index.json').read_bytes()
    plan = plan_by_date(library[0], [library[2]], folder)
    assert len(plan['moves']) == 1
    repaired = next(item for item in [*plan['keepers'], *plan['drops']] if item['path'].endswith(entries[0][0]))
    assert repaired['subtitle'] == 'Opening'
    assert repaired['discovery_subtitle'] == observed
    assert repaired['filename_subtitle_evidence']['filename'] == entries[0][0]
    assert repaired['filename_subtitle_evidence']['literal_subtitle'] == 'Opening'
    assert len(repaired['filename_subtitle_evidence']['independent_filename_peer_paths']) == 1
    assert library[1].read_bytes() == before_db
    assert (folder / 'candidate-index.json').read_bytes() == before_index


def test_literal_filename_never_overrides_existing_named_contradiction(library, monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy, 'wsl_to_windows_path', str)
    entries = [('Example Show #1 Opening 2024 01 01 10 00.mp4', [1], 1500, 'Different'),
               ('Example Show #1「Opening」2024 02 01 10 00.mp4', [1], 1500, 'Opening')]
    for name, _, _, _ in entries:
        recording(library, name)
    plan = plan_by_date(library[0], [library[2]], evidence(library, entries))
    assert plan['moves'] == []
    assert {item['subtitle'] for item in [*plan['keepers'], *plan['holds']]} == {'Different', 'Opening'}
    assert plan['defer_reasons'] == {'filename_named_subtitle_conflict': 1}
    assert all('filename_subtitle_evidence' not in item for item in plan['keepers'])


def test_peer_metadata_alone_cannot_supply_a_missing_filename_label(library, monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy, 'wsl_to_windows_path', str)
    entries = [('Example Show #1 Opening 2024 01 01 10 00.mp4', [1], 1500, ''),
               ('Example Show #1 2024 02 01 10 00.mp4', [1], 1500, 'Opening')]
    for name, _, _, _ in entries:
        recording(library, name)
    plan = plan_by_date(library[0], [library[2]], evidence(library, entries))
    assert plan['moves'] == []
    assert plan['unresolved_same_episode_files'] == 2
    first = next(item for item in plan['keepers'] if item['path'].endswith(entries[0][0]))
    assert first['subtitle'] == ''
    assert 'filename_subtitle_evidence' not in first
    assert first['unconfirmed_filename_subtitle_candidate']['literal_subtitle'] == 'Opening'


@pytest.mark.parametrize('drift', [False, True])
def test_dotnet_timestamp_presentation_keeps_freshness_guard(library, monkeypatch, drift):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy, 'wsl_to_windows_path', str)
    entries = [('a 2024 01 01 10 00.mp4', [1], 1500, 'Opening'),
               ('b 2024 02 01 10 00.mp4', [1], 1500, 'Opening')]
    for name, _, _, _ in entries:
        recording(library, name)
    folder = evidence(library, entries)
    index_path = folder / 'candidate-index.json'
    index = json.loads(index_path.read_text())
    for item in index['files']:
        stamp = datetime.fromisoformat(item['mtimeUtc'])
        item['mtimeUtc'] = stamp.strftime('%Y-%m-%dT%H:%M:%S.%f') + '7+00:00'
    index_path.write_text(json.dumps(index))
    if drift:
        source = library[2] / entries[1][0]
        stat = source.stat()
        os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000))
    plan = plan_by_date(library[0], [library[2]], folder)
    if drift:
        assert plan['moves'] == []
        assert 'discovery_or_cached_probe_stale' in plan['defer_reasons']
    else:
        assert len(plan['moves']) == 1


def test_omnibus_needs_every_retained_episode_and_valid_duration(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('one 2024 01 01 10 00.mp4',[1],1500,'Opening'),('two 2024 02 01 10 00.mp4',[2],1500,'Second'),('batch 2024 03 01 10 00.mp4',[1,2],3000,'')]
    for name,eps,_,sub in entries: recording(library,name,ep=str(eps[0]) if len(eps)==1 else '',subtitle=sub)
    folder=evidence(library,entries)
    plan=plan_by_date(library[0],[library[2]],folder)
    assert len(plan['moves'])==1 and len(plan['moves'][0]['keeps'])==2
    probes=json.loads((folder/'merged-video-probes.json').read_text());probes[-1]['duration']=1800
    (folder/'merged-video-probes.json').write_text(json.dumps(probes))
    assert plan_by_date(library[0],[library[2]],folder)['moves']==[]


def test_second_coverage_keeper_change_blocks_apply_preflight(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('one 2024 01 01 10 00.mp4',[1],1500,'Opening'),('two 2024 02 01 10 00.mp4',[2],1500,'Second'),('batch 2024 03 01 10 00.mp4',[1,2],3000,'')]
    for name,eps,_,sub in entries: recording(library,name,ep=str(eps[0]) if len(eps)==1 else '',subtitle=sub)
    folder=evidence(library,entries)
    plan,directory=make_date_plan(library[1],library[3],[library[2]],folder)
    (library[2]/entries[1][0]).write_bytes(b'changed')
    with pytest.raises(ValueError,match='recording changed'):
        validate_date_plan(library[1],directory)


def test_unregistered_physical_recordings_are_planned_without_db_write(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('a 2024 01 01 10 00.mp4',[1],1500,'Opening'),('b 2024 02 01 10 00.mp4',[1],1500,'Opening')]
    for name,_,_,_ in entries:
        p=library[2]/name;p.write_bytes(bytes(200));os.utime(p,(1700000000,1700000000))
    folder=evidence(library,entries)
    plan=plan_by_date(library[0],[library[2]],folder)
    assert len(plan['moves'])==1
    assert library[0].execute('SELECT count(*) FROM paths').fetchone()[0]==0


def test_filename_date_not_production_year_and_24hour_rollover():
    assert recording_stamp('Show (1982年) 2024 12 31 24 30.mp4').isoformat()=='2025-01-01T00:30:00'
    assert recording_stamp('Show 1982年 全50話.mp4') is None
    assert recording_stamp('Show 2024 02 30 10 00.mp4') is None


def test_length_mismatch_remains_visible_as_unresolved_not_clean(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('S3-02 short 2026 01 03 14 20.mp4',[2],3306.2256,''),
             ('S3-02 long 2026 05 01 14 15.mp4',[2],3907.062711,''),
             ('S3-02 long copy 2026 06 01 14 15.mp4',[2],3907.062711,'')]
    for name,_,_,_ in entries:recording(library,name,ep='2',subtitle='')
    before=library[1].read_bytes()
    plan=plan_by_date(library[0],[library[2]],evidence(library,entries))
    assert len(plan['moves'])==1  # The proven equal-length copy is still eligible.
    groups=plan['unresolved_same_episode_groups']
    assert len(groups)==1 and groups[0]['episodes']==[2]
    assert groups[0]['reasons']==['duration_or_edition_mismatch']
    assert len(groups[0]['candidates'])==plan['unresolved_same_episode_files']==2
    assert plan['decisions']['keep']==0 and plan['decisions']['review']==2
    assert groups[0]['content_equivalence']=='not_verified'
    assert plan['cleanup_complete'] is False
    assert library[1].read_bytes()==before


def test_conflicting_subtitles_reported_without_merging_seasons(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('a 2024 01 01 10 00.mp4',[1],1500,'First'),
             ('b 2024 02 01 10 00.mp4',[1],1500,'Other'),
             ('c 2024 03 01 10 00.mp4',[1],1500,'First')]
    for name,_,_,sub in entries:recording(library,name,subtitle=sub)
    folder=evidence(library,entries)
    index=json.loads((folder/'candidate-index.json').read_text())
    index['files'][-1]['title']='Example Show Season 2'
    (folder/'candidate-index.json').write_text(json.dumps(index))
    plan=plan_by_date(library[0],[library[2]],folder)
    assert plan['moves']==[]
    assert len(plan['unresolved_same_episode_groups'])==1
    group=plan['unresolved_same_episode_groups'][0]
    assert group['reasons']==['subtitle_conflict_or_missing']
    assert {Path(c['path']).name for c in group['candidates']}=={entries[0][0],entries[1][0]}


@pytest.mark.parametrize('label,expected_moves',[('#2',1),('第2話',1),('＃０２',1),
    ('#3',0),('#2 Rescue',0),('Rescue',0)])
def test_bare_matching_episode_marker_is_not_a_subtitle_conflict(library,monkeypatch,label,expected_moves):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('a 2026 01 01 10 00.mp4',[2],1800,label),('b 2026 02 01 10 00.mp4',[2],1800,'')]
    for name,_,_,_ in entries:recording(library,name,ep='2',subtitle='')
    plan=plan_by_date(library[0],[library[2]],evidence(library,entries))
    assert len(plan['moves'])==expected_moves
    assert bool(plan['unresolved_same_episode_groups'])==(not expected_moves)


def coverage_review(folder, plan, *, source_index=0, keeper_index=1):
    source, keeper = plan['keepers'][source_index], plan['keepers'][keeper_index]
    proof = folder/'full-content-review.txt'
    proof.write_text('Full audio comparison and visual review of every unmatched interval; keeper includes ending.')
    def observation(item):
        return {'path':item['path'],'signature':item['signature'],
                'duration_seconds':item['quality']['duration']}
    review = {'review_status':'verified_program_coverage','reviewed_by':'test reviewer',
        'program_title':source['title'],'episodes':source['episodes'],'edition':source['edition'],
        'broadcast_kind':'repeat_by_sourced_date','source':observation(source),'keeper':observation(keeper),
        'full_source_audio_compared':True,'source_program_content_covered':True,'keeper_program_complete':True,
        'unmatched_source_intervals':[{'start':2900,'end':3000,'classification':'commercial_or_promotion',
            'review_notes':'Every unmatched interval reviewed as non-program content.'}],
        'review_notes':'Complete retained episode contains all source program scenes.',
        'evidence_files':[{'path':proof.name,'sha256':hashlib.sha256(proof.read_bytes()).hexdigest()}]}
    document = {'schema_version':1,'reviews':[review]}
    (folder/'content-coverage-reviews.json').write_text(json.dumps(document))
    return document


def length_mismatch_fixture(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('short 2026 01 01 10 00.mp4',[2],3306,''),
             ('complete 2026 02 01 10 00.mp4',[2],3907,'')]
    for name,_,_,_ in entries:recording(library,name,ep='2',subtitle='')
    folder=evidence(library,entries)
    initial=plan_by_date(library[0],[library[2]],folder)
    assert len(initial['unresolved_same_episode_groups'])==1 and not initial['moves']
    return folder,initial


def test_explicit_content_coverage_resolves_only_reviewed_duration_pair(library,monkeypatch):
    folder,initial=length_mismatch_fixture(library,monkeypatch)
    coverage_review(folder,initial)
    before=library[1].read_bytes()
    plan=plan_by_date(library[0],[library[2]],folder)
    assert len(plan['moves'])==1 and 'short' in plan['moves'][0]['src']
    assert plan['moves'][0]['reason']=='explicitly_reviewed_program_content_covered_by_complete_keeper'
    assert not plan['unresolved_same_episode_groups'] and plan['candidate_review_complete']
    assert 'content-coverage-reviews.json' in plan['evidence_checksums']
    assert 'full-content-review.txt' in plan['evidence_checksums']
    assert library[1].read_bytes()==before


@pytest.mark.parametrize('change,error',[
    ('partial_audio','incomplete content'),('unreviewed_interval','unreviewed source'),
    ('stale_signature','recording changed'),('wrong_season','preservation identity'),
    ('wrong_episode','preservation identity'),('evidence_changed','evidence changed'),
    ('escaped_reference','escaped directory'),('incomplete_keeper','incomplete content'),
])
def test_coverage_review_fails_closed(library,monkeypatch,change,error):
    folder,initial=length_mismatch_fixture(library,monkeypatch)
    body=coverage_review(folder,initial);review=body['reviews'][0]
    if change=='partial_audio':review['full_source_audio_compared']=False
    if change=='unreviewed_interval':review['unmatched_source_intervals'][0]['classification']='unknown'
    if change=='stale_signature':review['source']['signature']['size_bytes']+=1
    if change=='wrong_season':review['program_title']='Example Show season 99'
    if change=='wrong_episode':review['episodes']=[3]
    if change=='evidence_changed':(folder/'full-content-review.txt').write_text('changed')
    if change=='escaped_reference':review['evidence_files'][0]['path']='../outside'
    if change=='incomplete_keeper':review['keeper_program_complete']=False
    (folder/'content-coverage-reviews.json').write_text(json.dumps(body))
    with pytest.raises(ValueError,match=error):plan_by_date(library[0],[library[2]],folder)


def test_changed_visual_coverage_proof_blocks_exact_plan_apply(library,monkeypatch):
    folder,initial=length_mismatch_fixture(library,monkeypatch)
    coverage_review(folder,initial)
    _,directory=make_date_plan(library[1],library[3],[library[2]],folder)
    (folder/'full-content-review.txt').write_text('replacement')
    with pytest.raises(ValueError,match='date evidence changed'):
        validate_date_plan(library[1],directory)


@pytest.mark.parametrize('source_subtitle,keeper_subtitle,explicit,allowed',[
    ('','Opening',False,False), ('','Opening',True,True),
    ('Opening','',True,True), ('Opening','Different episode',True,False),
])
def test_content_review_missing_subtitle_requires_exact_explicit_reconciliation(
        library,monkeypatch,source_subtitle,keeper_subtitle,explicit,allowed):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('short 2026 01 01 10 00.mp4',[2],3306,source_subtitle),
             ('complete 2026 02 01 10 00.mp4',[2],3907,keeper_subtitle)]
    for name,_,_,_ in entries:recording(library,name,ep='2',subtitle='')
    folder=evidence(library,entries)
    initial=plan_by_date(library[0],[library[2]],folder)
    body=coverage_review(folder,initial)
    review=body['reviews'][0]
    if explicit:
        review['subtitle_discrepancy_review']={
            'source_subtitle':source_subtitle,'keeper_subtitle':keeper_subtitle,
            'missing_label_only':True,'same_episode_verified_from_full_content':True,
            'review_notes':'Full narrative and episode ending match despite absent filename label.'}
    (folder/'content-coverage-reviews.json').write_text(json.dumps(body))
    if allowed:
        plan=plan_by_date(library[0],[library[2]],folder)
        assert len(plan['moves'])==1 and not plan['unresolved_same_episode_groups']
    else:
        with pytest.raises(ValueError,match='subtitle preservation identity'):
            plan_by_date(library[0],[library[2]],folder)


def test_coverage_review_cannot_override_original_period_protection(library,monkeypatch):
    folder,initial=length_mismatch_fixture(library,monkeypatch)
    coverage_review(folder,initial)
    sources=json.loads((folder/'broadcast-date-sources.json').read_text())
    sources['periods'][0]['end']='2026-01-31'
    (folder/'broadcast-date-sources.json').write_text(json.dumps(sources))
    with pytest.raises(ValueError,match='not applicable to current protected/held'):
        plan_by_date(library[0],[library[2]],folder)


def test_whole_drive_reserved_quarantine_but_never_inside_media_subroot():
    source=Path('/mnt/n/特撮/episode.mp4')
    assert quarantine_root(source,[Path('/mnt/n')])==Path('/mnt/n/_AI_WORK/duplicates/quarantine_v3')
    with pytest.raises(ValueError):
        quarantine_root(source,[Path('/mnt/n'),Path('/mnt/n/_AI_WORK')])


def test_native_eio_fallback_never_accepts_missing_or_reparse(monkeypatch):
    from video_pipeline.domain.recording_dedup import within_roots
    from video_pipeline.platform.native_file_signatures import native_path_key
    source=Path('/mnt/z/series/long-name.mp4');real_stat=Path.stat;real_resolve=Path.resolve
    def broken_stat(p,*args,**kwargs):
        if p==source:raise OSError(5,'DrvFS EIO')
        return real_stat(p,*args,**kwargs)
    def broken_resolve(p,*args,**kwargs):
        if p==source:raise OSError(5,'DrvFS EIO')
        return real_resolve(p,*args,**kwargs)
    monkeypatch.setattr(Path,'stat',broken_stat);monkeypatch.setattr(Path,'resolve',broken_resolve)
    key=native_path_key(source);sig={'size_bytes':200,'mtime_ns':1700000000000000000,'mtime_utc':'2023-11-14T22:13:20+00:00'}
    cache={key:{'exists':True,'signature':sig}}
    assert file_signature(source,native_cache=cache)==sig
    assert within_roots(source,[Path('/mnt/z')],native_cache=cache)
    for unsafe in [{'exists':False},{'exists':True,'error':'native_reparse_ancestor','signature':sig}]:
        with pytest.raises(ValueError):file_signature(source,native_cache={key:unsafe})
        assert not within_roots(source,[Path('/mnt/z')],native_cache={key:unsafe})


def test_explicit_base_alias_preserves_full_filename_season(library,monkeypatch):
    import video_pipeline.domain.date_recording_dedup as policy
    monkeypatch.setattr(policy,'wsl_to_windows_path',str)
    entries=[('Example Show Season 1 high 2024 01 01 10 00.mp4',[1],1500,'Opening'),('Example Show Season 1 low 2024 02 01 10 00.mp4',[1],1500,'Opening')]
    for name,_,_,_ in entries:recording(library,name)
    folder=evidence(library,entries)
    index=json.loads((folder/'candidate-index.json').read_text())
    for f in index['files']:f['title']='Example Show Season 1'
    (folder/'candidate-index.json').write_text(json.dumps(index))
    period=json.loads((folder/'broadcast-date-sources.json').read_text());period['periods'][0].update(title='Example Show Season 1',metadata_title_aliases=['Example Show'])
    (folder/'broadcast-date-sources.json').write_text(json.dumps(period))
    plan=plan_by_date(library[0],[library[2]],folder)
    assert len(plan['moves'])==1 and all(k['title']=='Example Show Season 1' for k in plan['keepers'])
    period['periods'][0]['metadata_title_aliases']=[];(folder/'broadcast-date-sources.json').write_text(json.dumps(period))
    assert plan_by_date(library[0],[library[2]],folder)['moves']==[]
