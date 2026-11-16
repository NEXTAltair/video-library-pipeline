"""Partial plans may prepare a review, never apply or accept skipped rows."""
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from tests.test_relocate_workflow import RelocateWorkflowHarness
from video_pipeline.workflows import RelocateApplyConfig, RelocateWorkflowService, WorkflowStore
from video_pipeline.workflows.store import sha256_file
from workflow_cli import _next_actions_for_run


class MixedHarness(RelocateWorkflowHarness):
    def __init__(self, tmp_path, *, unregistered=256):
        super().__init__(tmp_path, run_id="mixed47")
        self.unregistered = unregistered

    def python_runner(self, script, args, cwd=None):
        result = json.loads(super().python_runner(script, args, cwd))
        if script.name == "relocate_existing_files.py":
            plan = Path(result['planPath'])
            rows = [dict(path_id=f'p{i}', src=rf'B:\Library\Show\video{i}.mp4',
                         dst=rf'E:\Docs\Show\video{i}.mp4', status='planned') for i in range(47)]
            rows += [dict(path_id=f'm{i}', src=rf'B:\Library\Show\missing{i}.mp4',
                          status='skipped', reason='missing_metadata') for i in range(138)]
            rows += [dict(src=rf'B:\Library\Show\unregistered{i}.m4v', status='skipped',
                          reason='unregistered_path') for i in range(self.unregistered)]
            plan.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            queue = plan.parent.parent / 'llm' / 'mixed_missing.jsonl'
            queue.write_text(''.join(json.dumps(dict(path_id=f'm{i}', path=rf'B:\Library\Show\missing{i}.mp4'))+'\n' for i in range(138)))
            result.update(plannedMoves=47, metadataQueuePlannedCount=138,
                          metadataMissingSkipped=138, metadataQueuePath=str(queue),
                          unregisteredSkipped=self.unregistered)
        elif script.name == "run_metadata_batches_promptv1.py":
            queue = Path(args[args.index('--queue')+1])
            rows = [json.loads(l) for l in queue.read_text().splitlines() if l.strip()]
            Path(result['latestOutputJsonlPath']).write_text(''.join(json.dumps({
                **r, 'program_title':'Show', 'genre':'ドキュメンタリー・情報',
                'air_date':'2026-01-01', 'needs_review':False})+'\n' for r in rows))
        elif script.name == 'export_program_yaml.py':
            source = args[args.index('--source-jsonl')+1]
            Path(result['outputPath']).write_text(yaml.safe_dump({'source_jsonl':source,'hints':[], 'row_overrides':{}}))
        return json.dumps(result)

    def setup(self):
        result = self.dry_run()
        store = WorkflowStore(Path(self.cfg.windows_ops_root))
        self.store = store
        # A pre-existing unrelated blocker must never be approved by preparation.
        store.create_review_gate(self.cfg.run_id, gate_type='relocate_metadata_review',
                                 artifact_ids=['relocate_metadata_queue'], gate_id='unrelated')
        return result

    def cfg_from(self, params):
        return RelocateApplyConfig(windows_ops_root=self.cfg.windows_ops_root,
                                   run_id=params['runId'], artifact_id=params['artifactId'])


def snapshot(harness):
    run = harness.store.read_run(harness.cfg.run_id)
    return {aid:Path(a.path).read_bytes() for aid,a in run.artifacts.items()}, Path(harness.cfg.db).read_bytes()


@pytest.mark.parametrize('unregistered',[256,0])
def test_mixed_returned_action_prepares_only_47_and_preserves_other_blockers(tmp_path, unregistered):
    h=MixedHarness(tmp_path,unregistered=unregistered);result=h.setup()
    assert result.phase=='review_required'
    action=next(a for a in result.next_actions if a.action=='review_planned_metadata')
    assert action.to_dict() in [a.to_dict() for a in _next_actions_for_run(h.store.read_run(h.cfg.run_id))]
    original,db=snapshot(h)
    assert not h.service().apply(h.cfg_from(action.params)).ok
    reviewed=h.service().resume(h.cfg_from(action.params),action=action.params['resumeAction'])
    assert reviewed.phase=='review_required'
    assert any(a.action=='review_metadata' for a in reviewed.next_actions), reviewed.to_dict()
    run=h.store.read_run(h.cfg.run_id)
    q=next(a for a in run.artifacts.values() if a.metadata.get('refreshReviewed'))
    rows=[json.loads(l) for l in Path(q.path).read_text().splitlines()]
    assert {r['path_id'] for r in rows}=={f'p{i}' for i in range(47)}
    assert q.input_artifact_ids==['relocate_plan']
    assert all(Path(run.artifacts[aid].path).read_bytes()==raw for aid,raw in original.items())
    assert Path(h.cfg.db).read_bytes()==db and run.review_gates['unrelated'].status=='open'
    assert not h.service().apply(h.cfg_from(action.params)).ok
    assert not any(a.action=='review_planned_metadata' for a in _next_actions_for_run(run))
    n=len(h.calls);artifacts=len(run.artifacts)
    rejected=h.service().resume(h.cfg_from(action.params),action=action.params['resumeAction'])
    assert not rejected.ok and len(h.calls)==n
    assert len(h.store.read_run(h.cfg.run_id).artifacts)==artifacts


@pytest.mark.parametrize('bad',['checksum','foreign','path_escape','duplicate','missing_id','invalid_id',
                               'invalid_src','stale','internal_apply','state_file','applying','complete'])
def test_mixed_review_rejects_before_extraction_or_new_queue(tmp_path,bad):
    h=MixedHarness(tmp_path);result=h.setup();cfg=h.cfg_from(next(a.params for a in result.next_actions if a.action=='review_planned_metadata'))
    run=h.store.read_run(h.cfg.run_id);a=run.artifacts['relocate_plan'];path=Path(a.path)
    if bad=='checksum':path.write_text('changed')
    if bad=='foreign':cfg.artifact_id='foreign'
    if bad=='path_escape':
        outside=tmp_path/'outside.jsonl';outside.write_bytes(path.read_bytes());a.path=str(outside)
    if bad in {'duplicate','missing_id','invalid_id','invalid_src'}:
        rows=[json.loads(l) for l in path.read_text().splitlines()]
        if bad=='duplicate':rows[1]['path_id']=rows[0]['path_id']
        if bad=='missing_id':rows[0].pop('path_id')
        if bad=='invalid_id':rows[0]['path_id']=1
        if bad=='invalid_src':rows[0]['src']=1
        path.write_text(''.join(json.dumps(r)+'\n' for r in rows));a.sha256=sha256_file(path)
    if bad=='stale':
        h.store.register_artifact(h.cfg.run_id,artifact_id='relocate_plan_0002',artifact_type='relocate_plan',path=path,producer='test',metadata=a.metadata)
        run=h.store.read_run(h.cfg.run_id)
    if bad=='internal_apply':run.artifacts['relocate_internal_move_plan']=a
    if bad=='state_file':
        p=h.store.run_dir(h.cfg.run_id)/'apply/relocate_apply_state.json';p.parent.mkdir(exist_ok=True);p.write_text('{}')
    if bad in {'applying','complete'}:run.phase=bad
    if bad=='complete':run.status='complete'
    h.store.write_run(run);n=len(h.calls)
    rejected=h.service().resume(cfg,action='prepare_planned_metadata')
    assert not rejected.ok and len(h.calls)==n
    assert not any(x.metadata.get('refreshReviewed') for x in h.store.read_run(h.cfg.run_id).artifacts.values())
    assert h.store.read_run(h.cfg.run_id).review_gates['unrelated'].status=='open'


def test_mixed_acceptance_commits_only_registered47_and_rechecks_gaps(tmp_path):
    from mediaops_schema import connect_db, create_schema_if_needed
    h=MixedHarness(tmp_path);h.setup();db=Path(h.cfg.db);db.unlink()
    con=connect_db(str(db));create_schema_if_needed(con)
    for i in range(47):
        pid=f'p{i}';path=rf'B:\Library\Show\video{i}.mp4'
        con.execute('INSERT INTO paths(path_id,path,created_at,updated_at) VALUES(?,?,?,?)',(pid,path,'before','before'))
        data=json.dumps({'program_title':'Show','genre':'ドキュメンタリー・情報','air_date':'2026-01-01','franchise':'Keep','episode_no':'1','subtitle':'Keep','needs_review':False})
        con.execute('INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,human_reviewed) VALUES(?,?,?,?,?,?,?)',(pid,'human_reviewed',data,'before','Show','2026-01-01',1))
    for i in range(138):
        con.execute('INSERT INTO paths(path_id,path,created_at,updated_at) VALUES(?,?,?,?)',
                    (f'm{i}',rf'B:\Library\Show\missing{i}.mp4','before','before'))
    untouched=[tuple(r) for r in con.execute("select * from paths where path_id like 'm%' order by path_id").fetchall()]
    con.commit();con.close()
    run=h.store.read_run(h.cfg.run_id);cfg=RelocateApplyConfig(windows_ops_root=h.cfg.windows_ops_root,run_id=h.cfg.run_id)
    reviewed=h.service().resume(cfg,action='prepare_planned_metadata')
    assert any(a.action=='review_metadata' for a in reviewed.next_actions), reviewed.to_dict()
    action=next(a for a in reviewed.next_actions if a.action=='review_metadata');params=action.params
    accepted=h.service().resume(RelocateApplyConfig(windows_ops_root=h.cfg.windows_ops_root,
        run_id=h.cfg.run_id,gate_id=params['gateId'],artifact_ids=params['artifactIds'],review_yaml_paths=params['reviewYamlPaths']),action=params['resumeAction'])
    assert accepted.phase=='review_required'
    con=sqlite3.connect(db)
    assert con.execute('select count(*) from path_metadata').fetchone()[0]==47
    assert con.execute('select count(*) from paths').fetchone()[0]==185
    assert con.execute("select * from paths where path_id like 'm%' order by path_id").fetchall()==untouched
    assert con.execute("select count(*) from path_metadata where source='agent_accepted' and human_reviewed=0").fetchone()[0]==47
    assert h.store.read_run(h.cfg.run_id).review_gates['unrelated'].status=='open'
    assert not h.service().apply(cfg).ok


@pytest.mark.parametrize('corruption',['extra','missing','duplicate','changed_path'])
def test_planned_extraction_cannot_widen_shrink_or_remap_cohort(tmp_path,corruption):
    h=MixedHarness(tmp_path);result=h.setup()
    action=next(a for a in result.next_actions if a.action=='review_planned_metadata')
    original,db=snapshot(h)
    def runner(script,args,cwd=None):
        raw=h.python_runner(script,args,cwd)
        if script.name=='run_metadata_batches_promptv1.py':
            p=Path(json.loads(raw)['latestOutputJsonlPath']);rows=[json.loads(l) for l in p.read_text().splitlines()]
            if corruption=='extra':rows.append({**rows[0],'path_id':'m0'})
            if corruption=='missing':rows.pop()
            if corruption=='duplicate':rows.append(rows[0])
            if corruption=='changed_path':rows[0]['path']=r'B:\Different\video.mp4'
            p.write_text(''.join(json.dumps(r)+'\n' for r in rows))
        return raw
    service=RelocateWorkflowService(py_root=tmp_path,python_runner=runner)
    result=service.resume(h.cfg_from(action.params),action=action.params['resumeAction'])
    assert not result.ok
    run=h.store.read_run(h.cfg.run_id)
    assert not any(g.type=='metadata_review' for g in run.review_gates.values())
    assert all(Path(run.artifacts[aid].path).read_bytes()==raw for aid,raw in original.items())
    assert run.review_gates['unrelated'].status=='open' and Path(h.cfg.db).read_bytes()==db


@pytest.mark.parametrize('corruption',['checksum','cohort_with_updated_digest','parent_plan_checksum'])
def test_planned_queue_retry_rechecks_lineage_and_immutable_cohort(tmp_path,corruption):
    h=MixedHarness(tmp_path);h.setup()
    def fail(script,args,cwd=None):
        if script.name=='run_metadata_batches_promptv1.py':raise RuntimeError('temporary extraction error')
        return h.python_runner(script,args,cwd)
    cfg=RelocateApplyConfig(windows_ops_root=h.cfg.windows_ops_root,run_id=h.cfg.run_id)
    failed=RelocateWorkflowService(py_root=tmp_path,python_runner=fail).resume(cfg,action='prepare_planned_metadata')
    assert not failed.ok
    run=h.store.read_run(h.cfg.run_id)
    queue=next(a for a in run.artifacts.values() if a.metadata.get('refreshReviewed'))
    if corruption=='parent_plan_checksum':Path(run.artifacts['relocate_plan'].path).write_text('changed')
    else:
        path=Path(queue.path);rows=[json.loads(l) for l in path.read_text().splitlines()];rows[0]['path_id']='m0'
        path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
        if corruption=='cohort_with_updated_digest':queue.sha256=sha256_file(path);h.store.write_run(run)
    n=len(h.calls)
    result=h.service().resume(cfg,action='prepare_relocate_metadata')
    assert not result.ok and len(h.calls)==n
    assert not any(g.type=='metadata_review' for g in h.store.read_run(h.cfg.run_id).review_gates.values())
    assert h.store.read_run(h.cfg.run_id).review_gates['unrelated'].status=='open'
