from copy import deepcopy
import json
import subprocess
import sys
from pathlib import Path

import pytest
from video_pipeline.domain.program_genre_policy import prepare_program_genre_review


def cohort(genres, title='番組', placement='default'):
    return [{'path_id': str(i), 'program_title': title, 'genre': g,
             'air_date': f'2026-11-{i % 28 + 1:02d}', 'episode_no': i,
             'franchise': 'シリーズ', 'source_genre': placement,
             'src': f'N:\\シリーズ_牙狼\\x{i}.mp4', 'size_bytes': 1,
             'destination_work_folder': 'N:\\シリーズ_牙狼\\番組', 'layout': 'by_title'}
            for i, g in enumerate(genres)]


def evidence(genre, **extra):
    return {'genre': genre, 'reference': 'https://broadcaster.example/program', 'note': 'checked category', **extra}


@pytest.mark.parametrize('a,b,expected', [(1742,17,True),(86,2,True),(51,3,True),
    (36,8,True),(46,1,True),(42,1,True),(5,2,True),(4,1,True),(4,3,False),
    (64,37,True),(3,2,False),(2,1,False),(1,1,False),(2,0,True),(1,0,False)])
def test_threshold_and_examples(a,b,expected):
    report = prepare_program_genre_review(cohort(['news']*a + ['documentary']*b))
    assert report['programs'][0]['consensus'] is expected
    assert len(report['row_overrides']) == (b if expected else 0)


def test_unknown_votes_count_against_threshold():
    report = prepare_program_genre_review(cohort(['news','news',None,None]))
    assert not report['programs'][0]['consensus']


def test_identity_and_existing_series_layout_are_not_edited():
    rows = cohort(['news']*5 + ['drama'])
    before = deepcopy(rows)
    report = prepare_program_genre_review(rows)
    assert rows == before
    assert report['row_overrides']['5']['genre'] == 'ニュース・報道'
    assert set(report['row_overrides']['5']) == {'genre','review_note'}
    assert report['routing_changes'] == report['db_writes'] == report['physical_moves'] == 0


def test_source_change_requires_majority_and_checked_official_classification():
    rows = cohort(['news']*5 + ['documentary'], placement='documentary')
    assert not prepare_program_genre_review(rows)['row_overrides']
    bad = {'official_classifications': {'番組': evidence('news')}}
    assert not prepare_program_genre_review(rows,bad)['row_overrides']
    good = {'official_classifications': {'番組': evidence('news', checked=True)}}
    assert len(prepare_program_genre_review(rows,good)['row_overrides']) == 1
    good['official_classifications']['番組']['genre'] = 'drama'
    assert not prepare_program_genre_review(rows,good)['row_overrides']


def test_explicit_target_wins_but_cannot_override_source_majority_guard():
    rows = cohort(['documentary']*5+['news'], placement='documentary')
    policy = {'explicit_rules': {'番組': evidence('news')},
              'official_classifications': {'番組': evidence('news',checked=True)}}
    report = prepare_program_genre_review(rows,policy)
    assert report['programs'][0]['target_genre'] == 'news'
    assert not report['row_overrides']
    assert all('source_change_without_matching_majority' in r['reasons'] for r in report['held_rows'])
    for row in rows:
        row['source_genre'] = 'default'
    assert len(prepare_program_genre_review(rows,policy)['row_overrides']) == 5


def test_explicit_human_scope_is_per_path_not_blanket():
    rows = cohort(['news']*5+['documentary'],placement='documentary')
    policy = {'approved_source_changes': {'5': {'reference': 'owner message 2026-11-16', 'note': 'もぎたて20承認'}}}
    report = prepare_program_genre_review(rows,policy)
    assert set(report['row_overrides']) == {'5'}


def test_alias_canonicalizes_only_exact_normalized_titles():
    rows = cohort(['news']*4,title='モーサテ')
    rows[1]['program_title'] = 'Newsモーニングサテライト'
    rows.append({**rows[0], 'path_id':'x','program_title':'モーサテ別番組'})
    policy = {'aliases': [{'canonical_title':'モーサテ','aliases':['Newsモーニングサテライト'],
                          'reference':'https://www.tv-tokyo.co.jp/nms/', 'note':'same official show'}]}
    report = prepare_program_genre_review(rows,policy)
    assert len(report['programs']) == 2
    assert report['row_overrides']['1']['program_title'] == 'モーサテ'
    assert 'x' not in report['row_overrides']


@pytest.mark.parametrize('flags', [{'zero_corrupt':True},{'godzilla_title':True},
                                    {'bond007_title':True}])
def test_protected_rows_held(flags):
    rows = cohort(['news']*5+['drama'])
    rows[-1]['flags'] = flags
    assert '5' not in prepare_program_genre_review(rows)['row_overrides']


def test_movie_genre_is_not_rewritten():
    rows = cohort(['anime']*5+['movie'],title='東京ゴッドファーザーズ')
    assert not prepare_program_genre_review(rows)['row_overrides']


def test_unknown_source_is_not_assumed_default():
    rows = cohort(['news']*5+['drama'])
    del rows[-1]['source_genre']
    assert '5' not in prepare_program_genre_review(rows)['row_overrides']


@pytest.mark.parametrize('policy',[{'aliases':[{'canonical_title':'x','aliases':['y']}]},
    {'explicit_rules':{'x': {'genre':'news'}}}, {'official_classifications':{'x':evidence('invalid')}}])
def test_unbacked_policy_rejected(policy):
    with pytest.raises(ValueError):
        prepare_program_genre_review(cohort(['news']*2),policy)


def test_duplicate_identity_rejected():
    rows = cohort(['news']*2)
    rows[1]['path_id'] = rows[0]['path_id']
    with pytest.raises(ValueError):
        prepare_program_genre_review(rows)


def test_cli_only_writes_new_draft_outputs(tmp_path):
    source = tmp_path/'rows.json'
    source.write_text(json.dumps({'rows':cohort(['news']*5+['drama'])}))
    report, review = tmp_path/'report.json', tmp_path/'review.yaml'
    script = Path(__file__).resolve().parents[1]/'prepare_program_genre_review.py'
    cmd = [sys.executable,str(script),'--input',str(source),'--report',str(report),'--review-yaml',str(review)]
    assert subprocess.run(cmd,capture_output=True).returncode == 0
    assert json.loads(report.read_text())['row_overrides']['5']['genre'] == 'ニュース・報道'
    before = review.read_bytes()
    assert subprocess.run(cmd,capture_output=True).returncode != 0
    assert review.read_bytes() == before


def test_movie_and_syllabary_movement_hold_does_not_preserve_obvious_wrong_metadata():
    rows = cohort(['documentary']*5+['movie'])
    rows[-1]['flags'] = {'L_syllabary':True,'movie_route_or_standalone':True}
    report = prepare_program_genre_review(rows)
    assert report['row_overrides']['5']['genre'] == 'ドキュメンタリー・情報'
    last = report['programs'][0]['row_decisions'][-1]
    assert last['movement_holds'] == ['movie_policy_pending','syllabary_policy_pending']
    assert last['eligible_for_review']


def test_template_preserves_source_cohort_and_independent_fields(tmp_path):
    import yaml
    source = tmp_path/'rows.jsonl'
    rows = cohort(['news']*5+['drama'])
    source.write_text('\n'.join(json.dumps(r) for r in rows))
    template = tmp_path/'template.yaml'
    body = {'hints': [],'cohort':{'path_id_count':6},'source_jsonl':str(source),
            'row_overrides':{'5':{'air_date':'2020-01-01','review_note':'verified date'}}}
    template.write_text(yaml.safe_dump(body))
    report, review = tmp_path/'report.json', tmp_path/'review.yaml'
    script = Path(__file__).resolve().parents[1]/'prepare_program_genre_review.py'
    cmd = [sys.executable,str(script),'--input',str(source),'--review-template',str(template),
           '--report',str(report),'--review-yaml',str(review)]
    assert subprocess.run(cmd,capture_output=True).returncode == 0
    result = yaml.safe_load(review.read_text())
    assert result['source_jsonl'] == str(source) and result['cohort'] == body['cohort']
    assert result['row_overrides']['5']['air_date'] == '2020-01-01'
    assert 'verified date' in result['row_overrides']['5']['review_note']
    assert yaml.safe_load(template.read_text()) == body


def test_new_acceptance_guard_checks_normalized_titles_across_batches():
    from video_pipeline.domain.program_genre_policy import validate_program_genre_consistency
    rows = cohort(['news','documentary'], title='Ａ番組')
    rows[1]['program_title'] = 'A 番組'
    with pytest.raises(ValueError,match='program-level genre split remains'):
        validate_program_genre_consistency(rows)
    rows[1]['genre'] = 'ニュース・報道'
    validate_program_genre_consistency(rows)


def test_new_run_acceptance_rejects_split_before_db_change(tmp_path):
    import sqlite3
    from contextlib import closing
    import yaml
    from tests.test_reviewed_metadata_application import fixture, db_state
    from video_pipeline.application.reviewed_metadata import apply_reviewed_metadata
    store, db, cfg = fixture(tmp_path)
    run = store.read_run(cfg.run_id)
    for i, aid in enumerate(cfg.artifact_ids):
        draft = Path(run.artifacts[aid].path)
        body = yaml.safe_load(draft.read_text())
        body['row_overrides'] = {f'p{i}': {'program_title':'Unified',
            'genre':'news' if i == 0 else 'documentary', 'needs_review':False,
            'needs_review_reason':'', 'review_note':'test unresolved split across batches'}}
        draft.write_text(yaml.safe_dump(body))
    before = db_state(db)
    with pytest.raises(ValueError,match='program-level genre split remains'):
        apply_reviewed_metadata(store,cfg)
    assert db_state(db) == before
    assert not list(db.parent.glob('*.bak_pre_apply_reviewed_metadata*'))


def test_acceptance_guard_checks_epg_resolved_values_too():
    from video_pipeline.domain.program_genre_policy import validate_program_genre_consistency
    rows = cohort([None,None])
    rows[0]['epg_genres'] = [{'category':'アニメ'}]
    rows[1]['epg_genres'] = [{'category':'ニュース'}]
    with pytest.raises(ValueError,match='program-level genre split remains'):
        validate_program_genre_consistency(rows)


def test_single_record_jsonl_cli_does_not_assume_report_shape(tmp_path):
    source = tmp_path/'one.jsonl'
    source.write_text(json.dumps(cohort(['news'])[0])+'\n')
    report, review = tmp_path/'report.json', tmp_path/'review.yaml'
    script = Path(__file__).resolve().parents[1]/'prepare_program_genre_review.py'
    result = subprocess.run([sys.executable,str(script),'--input',str(source),
        '--report',str(report),'--review-yaml',str(review)],capture_output=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(report.read_text())['rows'] == 1
