import json
import subprocess
import sys
from pathlib import Path

import pytest
from tests.test_relocate_chunked_apply import Env
from video_pipeline.workflows import chunked_apply, receipt_coverage
from mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.db.file_identity import ensure_identity_schema, ensure_location, observe_file


def setup(tmp_path, count=40):
    e=Env(tmp_path,count);run=e.store.run_dir(e.run_id);log=run/'apply/receipts/log.jsonl';log.parent.mkdir(parents=True)
    state={'chunks':[{'localLog':str(log),'dbUpdated':True}],'dbSummaries':[],'db':str(e.ops/'db/mediaops.sqlite')}
    rows=[{**r,'op':'move','ok':True,'bundle_version':1,'bundle_complete':True,'members':[{'kind':'video','src':r['src'],'dst':r['dst'],'precondition':r['precondition']}]} for r in e.rows]
    return e,run,log,state,rows


def write(log, rows):log.write_text('\n'.join(json.dumps(r) for r in rows)+'\n')


def real_db(e, rows):
    db=Path(e.ops/'db/mediaops.sqlite');db.unlink();c=connect_db(str(db));create_schema_if_needed(c);ensure_identity_schema(c);c.execute("insert into runs(run_id,kind,started_at) values('seed','inventory','2026-01-01')")
    ids={}
    for row in rows:
        pid=ensure_location(c,row['src']);ids[row['src']]=observe_file(c,pid,row['precondition']['size_bytes'],'2026-01-01','seed')
    c.commit();c.close();return db,ids


def runner(script,args,cwd=None):
    p=subprocess.run([sys.executable,str(Path(__file__).parents[1]/script.name),*args],capture_output=True,text=True)
    if p.returncode:raise RuntimeError(p.stderr)
    return p.stdout


def test_legacy40physical36db_adds_only_four_and_keeps_identity(tmp_path):
    e,run,log,state,rows=setup(tmp_path);db,ids=real_db(e,rows);service=e.service();service.python_runner=runner
    write(log,rows[:36]);runner(Path('update_db_paths_from_move_apply.py'),['--db',str(db),'--applied',str(log)])
    write(log,rows);service._flush_db(run,state)
    assert state['dbSummaries'][-1]['updated']==4 and state['dbSummaries'][-1]['already_applied']==36
    with connect_db(str(db)) as c:
        assert c.execute('select count(*) from events').fetchone()[0]==40
        for row in rows:
            p=c.execute('select fp.file_id from file_paths fp join paths p using(path_id) where p.path=? and fp.is_current=1',(row['dst'],)).fetchone();assert p[0]==ids[row['src']]
            assert c.execute('select count(*) from file_paths fp join paths p using(path_id) where p.path=? and fp.is_current=1',(row['src'],)).fetchone()[0]==0
    assert len(state['chunks'][0]['dbReceiptCoverage'])==40
    assert all(len(x['members'])==1 for x in state['chunks'][0]['dbReceiptCoverage'].values())
    previous=list(state['dbSummaries']);service._flush_db(run,state);assert state['dbSummaries']==previous


def test_append_during_flush_is_not_acknowledged_until_next_call(tmp_path):
    e,run,log,state,rows=setup(tmp_path,2);write(log,rows[:1]);service=e.service();seen=[]
    def update(st,frozen):
        seen.append(receipt_coverage.successful_receipts(frozen));write(log,rows)
        return {'updated':len(seen[-1])}
    service._db_update=update;service._flush_db(run,state);assert len(state['chunks'][0]['dbReceiptCoverage'])==1
    service._flush_db(run,state);assert [len(x) for x in seen]==[1,1]
    assert len(state['chunks'][0]['dbReceiptCoverage'])==2


def test_partial_member_torn_tail_duplicate_and_failed_parent_not_covered(tmp_path):
    e,run,log,state,rows=setup(tmp_path,2)
    write(log,[rows[0],rows[0],{**rows[1],'bundle_complete':False},{'op':'move_member','ok':True}]);log.write_text(log.read_text()+'{"op":')
    e.service()._flush_db(run,state);assert len(state['chunks'][0]['dbReceiptCoverage'])==1
    write(log,rows);e.service()._flush_db(run,state);assert len(state['chunks'][0]['dbReceiptCoverage'])==2


def test_transaction_failure_rolls_back_and_coverage_does_not_advance(tmp_path):
    e,run,log,state,rows=setup(tmp_path,2);db,_=real_db(e,rows);rows[1]['file_id']='wrong';write(log,rows);service=e.service();service.python_runner=runner
    with pytest.raises(RuntimeError):service._flush_db(run,state)
    assert 'dbReceiptCoverage' not in state['chunks'][0]
    with connect_db(str(db)) as c:assert c.execute('select count(*) from events').fetchone()[0]==0
    rows[1].pop('file_id');write(log,rows);service._flush_db(run,state);assert state['dbSummaries'][-1]['updated']==2


def test_db_commit_then_state_save_failure_replays_without_duplicate_events(tmp_path,monkeypatch):
    e,run,log,state,rows=setup(tmp_path,2);db,_=real_db(e,rows);write(log,rows);service=e.service();service.python_runner=runner;save=chunked_apply.save_state
    monkeypatch.setattr(chunked_apply,'save_state',lambda *_:(_ for _ in ()).throw(OSError('injected state save failure')))
    with pytest.raises(OSError):service._flush_db(run,state)
    assert 'dbReceiptCoverage' not in state['chunks'][0] and state['dbSummaries']==[]
    monkeypatch.setattr(chunked_apply,'save_state',save);service._flush_db(run,state)
    assert state['dbSummaries'][-1]['updated']==0 and state['dbSummaries'][-1]['already_applied']==2
    with connect_db(str(db)) as c:assert c.execute('select count(*) from events').fetchone()[0]==2


def test_covered_receipt_mutation_is_fail_closed(tmp_path):
    e,run,log,state,rows=setup(tmp_path,1);write(log,rows);service=e.service();service._flush_db(run,state)
    rows[0]['dst']='N:\\wrong.mp4';write(log,rows)
    with pytest.raises(ValueError,match='removed or changed'):service._flush_db(run,state)


def test_invalid_duplicate_member_cannot_commit_before_coverage_validation(tmp_path):
    e,run,log,state,rows=setup(tmp_path,1);rows[0]['members']*=2;write(log,rows)
    service=e.service();service._db_update=lambda *_:pytest.fail('malformed coverage DB commit')
    with pytest.raises(ValueError,match='duplicate receipt member'):service._flush_db(run,state)


def test_public_status_exposes_reconcile_for_legacy_uncovered_receipts(tmp_path):
    from types import SimpleNamespace
    from workflow_cli import _cmd_status
    from video_pipeline.workflows import WorkflowPhase
    e,run,log,state,rows=setup(tmp_path,2);write(log,rows)
    state['internalPlan']=str(run/'plan/relocate_plan.jsonl');state['chunks'][0]['log']=str(log);state['inFlight']=None;state['settlement']=None
    e.store.transition_run(e.run_id,WorkflowPhase.APPLYING);chunked_apply.save_state(run,state,'seed')
    args=SimpleNamespace(windows_ops_root=str(e.ops),run_id=e.run_id,limit=5,include_artifacts=False,hints_path='')
    assert _cmd_status(args)['run']['nextActions'][0]['params']['resumeAction']=='reconcile_relocate_apply'
    e.service()._flush_db(run,state)
    assert _cmd_status(args)['run']['nextActions'][0]['params']['resumeAction']=='continue_relocate_apply'
