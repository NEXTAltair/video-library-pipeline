"""A subset is non-applying and cannot launder a held row or a stale plan."""
import copy
import json
from pathlib import Path
import pytest
from tests.test_relocate_workflow import RelocateWorkflowHarness
from video_pipeline.workflows import RelocateApplyConfig, WorkflowStore
from video_pipeline.workflows.store import sha256_file
from video_pipeline.workflows.relocate_subset import partition_plan
from workflow_cli import _next_actions_for_run


def setup(tmp_path):
    h=RelocateWorkflowHarness(tmp_path,run_id='subset');h.dry_run()
    store=WorkflowStore(Path(h.cfg.windows_ops_root));run=store.read_run('subset');a=run.artifacts['relocate_plan']
    rows=[dict(path_id=p,src=rf'B:\Library\Show\{p}.mp4',dst=rf'E:\Docs\Show\{p}.mp4',
        status='planned',program_title='Show',genre_route='documentary',air_date='2025-11-01',
        bundle_version=1,members=[{'kind':'video','src':p,'dst':p+'-new'},{'kind':'subtitle','src':p+'.srt','dst':p+'-new.srt'}],
        companion_cohort=[p+'.srt'],precondition={'size_bytes':10}) for p in ['held','a','b']]
    Path(a.path).write_text(''.join(json.dumps(x)+'\n' for x in rows));a.sha256=sha256_file(Path(a.path));a.metadata['summary']['plannedMoves']=3;store.write_run(run)
    run=store.read_run('subset');cfg=RelocateApplyConfig(windows_ops_root=h.cfg.windows_ops_root,run_id='subset',
        run_revision=run.revision,selected_path_ids=['a','b'],held_path_ids=['held'])
    return h,store,cfg,rows


def test_subset_is_immutable_owned_bundle_preserving_and_non_applying(tmp_path):
    h,store,cfg,rows=setup(tmp_path);run=store.read_run('subset');before=Path(run.artifacts['relocate_plan'].path).read_bytes();db=Path(h.cfg.db).read_bytes();n=len(h.calls)
    assert any(a.action=='prepare_relocate_subset' for a in _next_actions_for_run(run))
    result=h.service().resume(cfg,action='prepare_relocate_subset');assert result.ok,result.to_dict()
    run=store.read_run('subset');child=run.artifacts[result.next_actions[0].params['artifactId']]
    assert [json.loads(x) for x in Path(child.path).read_text().splitlines()]==rows[1:]
    assert Path(run.artifacts['relocate_plan'].path).read_bytes()==before
    assert child.input_artifact_ids==['relocate_plan'] and child.metadata['subsetSelection']['heldPathIds']==['held']
    assert child.metadata['subsetSelection']['originalSummary']['plannedMoves']==3
    assert len(h.calls)==n and Path(h.cfg.db).read_bytes()==db
    assert not h.service().resume(cfg,action='prepare_relocate_subset').ok
    assert not h.service().apply(cfg).ok  # original plan no longer current
    assert not any(a.action=='prepare_relocate_subset' for a in _next_actions_for_run(run))


@pytest.mark.parametrize('bad',['unknown','duplicate','empty','type','held','missing','whitespace','badplanid','duplicateplan','checksum','foreign','outside','stale','revision','started','receipts','internal','complete','reviewgate','rowhold'])
def test_subset_failclosed_without_artifacts_or_side_effects(tmp_path,bad):
    h,store,cfg,rows=setup(tmp_path);run=store.read_run('subset');a=run.artifacts['relocate_plan'];path=Path(a.path)
    if bad=='unknown':cfg.selected_path_ids=['a','unknown']
    if bad=='duplicate':cfg.selected_path_ids=['a','a','b']
    if bad=='empty':cfg.selected_path_ids=[]
    if bad=='type':cfg.selected_path_ids=['a',123]
    if bad=='held':cfg.selected_path_ids=['held','a','b']
    if bad=='missing':cfg.selected_path_ids=['a']
    if bad=='whitespace':cfg.held_path_ids=[' held']
    if bad=='badplanid':rows[0]['path_id']=None
    if bad=='duplicateplan':rows[0]['path_id']='a'
    if bad=='rowhold':rows[1]['hold_reason']='quarantine'
    if bad in ['badplanid','duplicateplan','rowhold']:
        path.write_text(''.join(json.dumps(x)+'\n' for x in rows));a.sha256=sha256_file(path)
    if bad=='checksum':path.write_text('tampered')
    if bad=='foreign':cfg.artifact_id='foreign'
    if bad=='outside':
        outside=tmp_path/'outside';outside.write_bytes(path.read_bytes());a.path=str(outside)
    if bad=='stale':
        store.register_artifact('subset',artifact_id='relocate_plan_0002',artifact_type='relocate_plan',path=path,producer='test');run=store.read_run('subset')
    if bad=='revision':cfg.run_revision=-1
    if bad=='internal':run.artifacts['relocate_internal_move_plan']=copy.deepcopy(a)
    if bad=='complete':run.phase='complete';run.status='complete'
    if bad in ['started','receipts']:
        p=store.run_dir('subset')/'apply'/('relocate_apply_state.json' if bad=='started' else 'receipts');p.parent.mkdir(exist_ok=True)
        if bad=='started':p.write_text('{}')
        else:p.mkdir()
    store.write_run(run)
    if bad=='reviewgate':store.create_review_gate('subset',gate_type='metadata_review',artifact_ids=['relocate_plan'],gate_id='block')
    if bad!='revision':cfg.run_revision=store.read_run('subset').revision
    original=store.read_run('subset').to_dict();n=len(h.calls);db=Path(h.cfg.db).read_bytes()
    result=h.service().resume(cfg,action='prepare_relocate_subset');assert not result.ok,(bad,result.to_dict())
    assert store.read_run('subset').to_dict()==original and len(h.calls)==n and Path(h.cfg.db).read_bytes()==db


def test_partition_does_not_revote_genre_or_drop_unknown_denominator():
    rows=[{'path_id':p,'src':p,'dst':p+'new','status':'planned','genre':g} for p,g in [('movie','movie'),('doc','doc'),('unknown',None)]]
    assert partition_plan(rows,['doc'],['movie','unknown'])==[rows[1]]

@pytest.mark.parametrize('changed',['parent','child','lineage','shared_companion'])
def test_subset_lineage_and_shared_member_failclosed(tmp_path,changed):
    h,store,cfg,rows=setup(tmp_path)
    if changed=='shared_companion':
        rows[1]['members'][1]['src']=rows[0]['members'][1]['src']
        a=store.read_run('subset').artifacts['relocate_plan'];p=Path(a.path);p.write_text(''.join(json.dumps(x)+'\n' for x in rows));run=store.read_run('subset');run.artifacts[a.id].sha256=sha256_file(p);store.write_run(run);cfg.run_revision=store.read_run('subset').revision
        assert not h.service().resume(cfg,action='prepare_relocate_subset').ok
        return
    result=h.service().resume(cfg,action='prepare_relocate_subset');assert result.ok
    aid=result.next_actions[0].params['artifactId'];run=store.read_run('subset')
    if changed=='parent':Path(run.artifacts['relocate_plan'].path).write_text('changed')
    if changed=='child':
        p=Path(run.artifacts[aid].path);p.write_text(json.dumps(rows[0])+'\n');run.artifacts[aid].sha256=sha256_file(p)
    if changed=='lineage':run.artifacts[aid].input_artifact_ids=[]
    store.write_run(run)
    n=len(h.calls)
    assert not h.service().apply(RelocateApplyConfig(windows_ops_root=cfg.windows_ops_root,run_id='subset',artifact_id=aid)).ok
    assert len(h.calls)==n


def test_public_cli_partition_returns_only_reviewed_subset_apply_action(tmp_path):
    from workflow_cli import build_parser, _cmd_resume
    h,store,cfg,rows=setup(tmp_path)
    args=build_parser().parse_args(['resume','--windows-ops-root',cfg.windows_ops_root,
        '--run-id','subset','--action','prepare_relocate_subset','--artifact-id','relocate_plan',
        '--run-revision',str(cfg.run_revision),'--selected-path-ids-json','["a","b"]',
        '--held-path-ids-json','["held"]'])
    result=_cmd_resume(args);assert result['ok'],result
    assert len(result['nextActions'])==1
    assert result['nextActions'][0]['params']['resumeAction']=='apply_relocate_move_plan'
    assert result['nextActions'][0]['params']['artifactId']!='relocate_plan'
    assert result['nextActions'][0]['requiresHumanInput']
