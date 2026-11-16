"""Issue186: use disposable files, never production media."""
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from tests.test_move_bundle_powershell import fixture
from tests.test_relocate_chunked_apply import Env, resume
from video_pipeline.workflows import chunked_apply
from video_pipeline.workflows.store import sha256_file

ROOT = Path(__file__).resolve().parents[2]


def native_env(tmp_path, companions=True):
    if not shutil.which("pwsh"):
        pytest.skip("PowerShell unavailable")
    scripts, op, stem = fixture(tmp_path, companions=companions)
    op['bundle_version'] = 1
    shutil.copy(ROOT/'assets/windows-scripts/reconcile_move_bundle.ps1', scripts)
    (tmp_path/'dst').mkdir()
    for member in op['members']:
        src = tmp_path/member['src'][3:].replace('\\','/')
        dst = tmp_path/member['dst'][3:].replace('\\','/')
        shutil.copy2(src, dst)
    plan = tmp_path/'plan.jsonl'; plan.write_text(json.dumps(op)+'\n')
    log = tmp_path/'receipt.jsonl'
    log.write_text(json.dumps({'op':'move_bundle_intent', **{k:op[k] for k in ('src','dst','members','path_id')}})+'\n')
    return scripts, op, plan, log


def invoke(env, mode='inspect', **kwargs):
    scripts, op, plan, log = env
    args = {'PlanJsonl':str(plan),'LogJsonl':str(log),'PlanSha256':sha256_file(plan),
            'LogSha256':sha256_file(log) if log.exists() else 'absent','Src':op['src'],'Mode':mode, **kwargs}
    result = subprocess.run(['pwsh','-NoProfile','-File',str(scripts/'reconcile_move_bundle.ps1'),
                             *(v for k,x in args.items() for v in ('-'+k,x))],
                            env={**os.environ,'BUNDLE_ROOT':str(plan.parent)},capture_output=True,text=True)
    return result, json.loads(result.stdout)


@pytest.mark.skipif(not shutil.which('pwsh'), reason='PowerShell unavailable')
def test_identical_copies_inspect_finalize_and_replay(tmp_path):
    e=native_env(tmp_path)
    before=e[3].read_bytes()
    r,info=invoke(e); assert r.returncode==0 and info['state']=='ready'
    assert e[3].read_bytes()==before and len(list((tmp_path/'src').iterdir()))==5
    r,info=invoke(e,'finalize'); assert r.returncode==0 and info['state']=='complete'
    records=[json.loads(l) for l in e[3].read_text().splitlines()]
    assert [r['kind'] for r in records if r['op']=='move_member']==['program_txt','recording_json','psc','vtt','video']
    assert not list((tmp_path/'src').iterdir())
    assert records[-1]['bundle_complete'] and records[-1]['bundle_version']==1
    count=len(records)
    r,info=invoke(e,'finalize'); assert r.returncode==0 and info['state']=='complete'
    assert len(e[3].read_text().splitlines())==count


@pytest.mark.parametrize('failure', ['partial','missing_intent','checksum','path','source_changed','missing_proof','mixed','new_companion'])
def test_fail_closed_no_deletion(tmp_path, failure):
    e=native_env(tmp_path,companions=False);scripts,op,plan,log=e
    src=next((tmp_path/'src').iterdir());dst=next((tmp_path/'dst').iterdir());kw={}
    if failure=='partial':
        st=dst.stat();dst.write_bytes(b'x'*st.st_size);os.utime(dst,ns=(st.st_atime_ns,st.st_mtime_ns))
    elif failure=='missing_intent':log.write_text('{}\n')
    elif failure=='checksum':kw['PlanSha256']='0'*64
    elif failure=='path':op['members'][0]['dst']='X:\\unowned.mp4';plan.write_text(json.dumps(op)+'\n')
    elif failure=='source_changed':src.write_bytes(b'changed')
    elif failure=='missing_proof':src.unlink()
    elif failure=='mixed':
        # A moved companion plus an unstarted video is not a safe finalize.
        dst.unlink();op['companion_cohort']=['psc'];m={'kind':'psc','src':op['src'][:-4]+'.psc','dst':op['dst'][:-4]+'.psc','precondition':op['members'][0]['precondition']};op['members'].append(m);plan.write_text(json.dumps(op)+'\n')
    else:(tmp_path/'dst'/(dst.stem+'.psc')).write_text('unowned')
    before={p:str(p.stat().st_size) for p in tmp_path.rglob('*') if p.is_file()}
    r,info=invoke(e,'finalize',**kw);assert r.returncode!=0 and not info['ok']
    assert before=={p:str(p.stat().st_size) for p in tmp_path.rglob('*') if p.is_file()}


def bundle_env(tmp_path):
    e=Env(tmp_path,1)
    row=e.rows[0];row.update(bundle_version=1,companion_cohort=[],members=[{'kind':'video','src':row['src'],'dst':row['dst'],'precondition':{'size_bytes':1000,'mtime_utc':'2026-01-01T00:00:00Z'}}])
    plan=e.ops/'runs'/e.run_id/'plan/relocate_plan.jsonl';plan.write_text(json.dumps(row)+'\n')
    run=e.store.read_run(e.run_id);run.artifacts['relocate_plan'].sha256=sha256_file(plan);e.store.write_run(run)
    e.on_chunk=lambda *_:0
    from tests.test_relocate_chunked_apply import Killed
    with pytest.raises(Killed):resume(e,'apply_relocate_move_plan')
    state=chunked_apply.load_state(e.store.run_dir(e.run_id));log=Path(state['chunks'][0]['localLog'])
    log.write_text(json.dumps({'op':'move_bundle_intent', **{k:row[k] for k in ('src','dst','path_id','members')}})+'\n')
    return e,state,log


def test_public_reconcile_settle_and_db_boundary(tmp_path):
    e,state,log=bundle_env(tmp_path);service=e.service();calls=[]
    def ps(script,args):
        assert script.endswith('reconcile_move_bundle.ps1');mode=args[args.index('-Mode')+1];calls.append(mode)
        if mode=='finalize':
            with log.open('a') as f:f.write(json.dumps({**e.rows[0],'op':'move','ok':True,'bundle_complete':True})+'\n')
        return {'ok':True,'state':'ready' if mode=='inspect' else 'complete'}
    service.powershell_runner=ps
    r=resume(e,'reconcile_relocate_apply',service);assert r['ok'] and not e.db_rows
    assert r['nextActions'][0]['params']['resumeAction']=='settle_interrupted_move'
    r=resume(e,'settle_interrupted_move',service);assert r['ok'] and len(e.db_rows)==1
    assert calls==['inspect','finalize']
    r=resume(e,'settle_interrupted_move',service);assert not r['ok'] and calls==['inspect','finalize']
    r=resume(e,'continue_relocate_apply',service);assert r['phase']=='complete'


@pytest.mark.parametrize('corrupt',['checksum','intent','chunk'])
def test_ownership_failures_never_probe_or_commit_db(tmp_path,corrupt):
    e,state,log=bundle_env(tmp_path)
    if corrupt=='checksum':Path(state['internalPlan']).write_text('{}\n')
    elif corrupt=='intent':
        r=json.loads(log.read_text());r['dst']='N:\\Wrong.mp4';log.write_text(json.dumps(r)+'\n')
    else:
        p=Path(state['chunks'][0]['localPlan']);p.write_text(json.dumps({**e.rows[0],'dst':'N:\\Wrong.mp4'})+'\n')
    service=e.service();service.powershell_runner=lambda *_:pytest.fail('unowned native call')
    r=resume(e,'reconcile_relocate_apply',service);assert not r['ok'] and not e.db_rows


def test_moved_companions_and_identical_video_finalize_in_order(tmp_path):
    e=native_env(tmp_path);_,op,_,log=e
    with log.open('a') as out:
        for m in op['members']:
            if m['kind']=='video': continue
            (tmp_path/m['src'][3:].replace('\\','/')).unlink()
            out.write(json.dumps({'op':'move_member','ok':True,'path_id':op['path_id'],**m})+'\n')
    r,info=invoke(e,'finalize');assert r.returncode==0 and info['state']=='complete'
    assert not list((tmp_path/'src').iterdir())


def test_durable_hash_proof_recovers_crash_after_source_delete(tmp_path):
    e=native_env(tmp_path,companions=False);_,op,_,log=e;m=op['members'][0]
    dst=next((tmp_path/'dst').iterdir());next((tmp_path/'src').iterdir()).unlink()
    with log.open('a') as out:out.write(json.dumps({'op':'bundle_member_verified','path_id':op['path_id'],**m,'sha256':hashlib.sha256(dst.read_bytes()).hexdigest()})+'\n')
    r,info=invoke(e,'finalize');assert r.returncode==0 and info['state']=='complete'


def test_unstarted_bundle_can_continue_without_forged_receipt(tmp_path):
    e=native_env(tmp_path)
    for p in (tmp_path/'dst').iterdir():p.unlink()
    before=e[3].read_bytes();r,info=invoke(e);assert r.returncode==0 and info['state']=='not_started'
    assert e[3].read_bytes()==before


def test_hash_mismatch_same_size_timestamp_is_not_deleted(tmp_path):
    e=native_env(tmp_path,companions=False);dst=next((tmp_path/'dst').iterdir());st=dst.stat()
    dst.write_bytes(b'x'*st.st_size);os.utime(dst,ns=(st.st_atime_ns,st.st_mtime_ns))
    r,info=invoke(e,'finalize');assert r.returncode==1 and info['error']=='recovery_partial_copy_fail_closed'
    assert next((tmp_path/'src').iterdir()).exists() and dst.exists()


def test_settle_replay_after_parent_receipt_before_db_commit(tmp_path):
    e,state,log=bundle_env(tmp_path);service=e.service()
    def ps(script,args):
        mode=args[args.index('-Mode')+1]
        if mode=='finalize' and not any(json.loads(x).get('bundle_complete') for x in log.read_text().splitlines()):
            with log.open('a') as f:f.write(json.dumps({**e.rows[0],'op':'move','ok':True,'bundle_complete':True})+'\n')
        return {'ok':True,'state':'ready' if mode=='inspect' else 'complete'}
    service.powershell_runner=ps
    assert resume(e,'reconcile_relocate_apply',service)['ok']
    original=e._py
    def fail(script,args,cwd=None):
        if script.name=='update_db_paths_from_move_apply.py':raise RuntimeError('injected DB commit failure')
        return original(script,args,cwd)
    service.python_runner=fail
    assert not resume(e,'settle_interrupted_move',service)['ok']
    # State saved before DB flush: continue flushes the existing complete receipt,
    # never moves/reconciles the already completed file a second time.
    service.python_runner=original
    assert resume(e,'continue_relocate_apply',service)['phase']=='complete'
    assert len(e.db_rows)==1 and len([json.loads(x) for x in log.read_text().splitlines() if json.loads(x).get('bundle_complete')])==1


@pytest.mark.parametrize('field',['path_id','src','dst','members'])
def test_intent_tampering_blocks_recovery_before_delete(tmp_path,field):
    e=native_env(tmp_path,companions=False);intent=json.loads(e[3].read_text())
    if field=='members':intent[field][0]['precondition']['size_bytes']+=1
    else:intent[field]='X:\\wrong'
    e[3].write_text(json.dumps(intent)+'\n')
    r,info=invoke(e,'finalize');assert r.returncode!=0 and not info['ok']
    assert len(list((tmp_path/'src').iterdir()))==1


def test_templates_and_bootstrap_ship_recovery_script():
    name='reconcile_move_bundle.ps1'
    assert (ROOT/'templates/windows-scripts'/name).read_bytes()==(ROOT/'assets/windows-scripts'/name).read_bytes()
    assert f'"{name}"' in (ROOT/'src/platform/windows-scripts-bootstrap.ts').read_text()


def test_same_member_source_destination_is_never_deleted(tmp_path):
    e=native_env(tmp_path);_,op,plan,log=e
    op['dst']=op['src'][:-4]+'.mkv'
    for m in op['members']:
        m['dst']=m['src'] if m['kind'] in ('psc','vtt') else m['src'].replace('.mp4','.mkv')
    plan.write_text(json.dumps(op)+'\n')
    log.write_text(json.dumps({'op':'move_bundle_intent',**{k:op[k] for k in ('src','dst','path_id','members')}})+'\n')
    r,info=invoke(e,'finalize');assert r.returncode!=0 and not info['ok']
    assert len(list((tmp_path/'src').iterdir()))==5


def test_partly_moved_companion_bundle_is_fail_closed_without_deletion(tmp_path):
    e=native_env(tmp_path);_,op,_,log=e
    with log.open('a') as out:
        for m in op['members']:
            if m['kind']=='video':
                (tmp_path/m['dst'][3:].replace('\\','/')).unlink()
            else:
                (tmp_path/m['src'][3:].replace('\\','/')).unlink()
                out.write(json.dumps({'op':'move_member','ok':True,'path_id':op['path_id'],**m})+'\n')
    before=log.read_bytes()
    r,info=invoke(e,'finalize');assert r.returncode==1 and info['error']=='recovery_mixed_partial_bundle_fail_closed'
    assert log.read_bytes()==before and len(list((tmp_path/'src').iterdir()))==1 and len(list((tmp_path/'dst').iterdir()))==4


@pytest.mark.parametrize("rename,apply_override", [(False,False),(True,False),(True,True)])
def test_public_recovery_accepts_real_mover_intent_companion_order(tmp_path, rename, apply_override):
    from tests.test_move_bundle_powershell import run
    from video_pipeline.domain.companion_bundle import capture_bundle
    native=tmp_path/'native';native.mkdir()
    scripts,op,_=fixture(native)
    op.update(capture_bundle(op['src'],op['dst'],localize=lambda p:str(native/p[3:].replace('\\','/'))))
    assert op['members'][0]['kind']=='video'  # real Python planner order
    if rename:
        (native/'dst').mkdir()
        (native/op['dst'][3:].replace('\\','/')).write_bytes(b'collision')
    parent,records=run(native,scripts,op,*(['-OnDstExists','rename_suffix'] if rename else []))
    assert ('__dup1' in parent['dst']) == rename
    intent=next(r for r in records if r.get('op')=='move_bundle_intent')
    assert intent['members'][-1]['kind']=='video'  # actual shipped mover order
    assert parent['ok']
    e,state,log=bundle_env(tmp_path/'workflow')
    # Crash after the last durable member receipt, before the parent receipt.
    log.write_text('\n'.join(json.dumps(r) for r in records if r.get('op')!='move')+'\n')
    internal=Path(state['internalPlan']);internal.write_text(json.dumps(op)+'\n')
    Path(state['chunks'][0]['localPlan']).write_text(json.dumps(op)+'\n')
    manifest=e.store.read_run(e.run_id);manifest.artifacts['relocate_internal_move_plan'].sha256=sha256_file(internal)
    if rename:
        state['onDstExists']='rename_suffix'
        if not apply_override:manifest.config_snapshot['onDstExists']='rename_suffix'
        chunked_apply.save_state(e.store.run_dir(e.run_id),state,'now')
    e.store.write_run(manifest)
    shutil.copy(ROOT/'assets/windows-scripts/reconcile_move_bundle.ps1',scripts)
    service=e.service()
    def ps(script,args):
        result=subprocess.run(['pwsh','-NoProfile','-File',str(scripts/'reconcile_move_bundle.ps1'),*args],env={**os.environ,'BUNDLE_ROOT':str(native)},capture_output=True,text=True)
        assert result.returncode==0,(result.stdout,result.stderr)
        return json.loads(result.stdout)
    service.powershell_runner=ps
    result=resume(e,'reconcile_relocate_apply',service)
    assert result['ok'] and result['nextActions'][0]['params']['resumeAction']=='settle_interrupted_move',result['diagnostics']
    assert resume(e,'settle_interrupted_move',service)['ok']
    assert len(e.db_rows)==1
    recovered=[json.loads(line) for line in log.read_text().splitlines() if json.loads(line).get('bundle_complete')]
    assert len(recovered)==1 and recovered[0]['dst']==parent['dst']
    if rename:
        assert (native/op['dst'][3:].replace('\\','/')).read_bytes()==b'collision'


def test_duplicate_intent_member_kind_is_rejected(tmp_path):
    e,state,log=bundle_env(tmp_path);intent=json.loads(log.read_text());intent['members'].append(intent['members'][0]);log.write_text(json.dumps(intent)+'\n')
    service=e.service();service.powershell_runner=lambda *_:pytest.fail('duplicate kind native probe')
    assert not resume(e,'reconcile_relocate_apply',service)['ok']


def test_intent_timestamp_mismatch_at_ntfs_tick_precision_is_rejected(tmp_path):
    e,state,log=bundle_env(tmp_path);intent=json.loads(log.read_text())
    intent['members'][0]['precondition']['mtime_utc']='2026-01-01T00:00:00.0000001Z'
    log.write_text(json.dumps(intent)+'\n')
    service=e.service();service.powershell_runner=lambda *_:pytest.fail('100ns mismatch native probe')
    result=resume(e,'reconcile_relocate_apply',service)
    assert not result['ok'] and not e.db_rows


@pytest.mark.parametrize('case',['error_policy','other_parent','zero','too_large','other_stem','other_extension','wrong_member','policy_tamper'])
def test_suffix_resolution_is_bounded_by_owned_policy_and_identity(tmp_path,case):
    e,state,log=bundle_env(tmp_path);row=e.rows[0];intent=json.loads(log.read_text())
    dst=row['dst'][:-4]+'__dup1.mp4'
    if case=='other_parent':dst=dst.replace('Show','Other')
    elif case=='zero':dst=dst.replace('__dup1','__dup0')
    elif case=='too_large':dst=dst.replace('__dup1','__dup10001')
    elif case=='other_stem':dst=dst.replace('f0000','f9999')
    elif case=='other_extension':dst=dst[:-4]+'.mkv'
    intent['dst']=dst;intent['members'][0]['dst']=dst
    if case=='wrong_member':intent['members'][0]['dst']=row['dst']
    if case!='error_policy':
        state['onDstExists']='rename_suffix';chunked_apply.save_state(e.store.run_dir(e.run_id),state,'now')
        if case!='policy_tamper':
            manifest=e.store.read_run(e.run_id);manifest.config_snapshot['onDstExists']='rename_suffix';e.store.write_run(manifest)
    log.write_text(json.dumps(intent)+'\n')
    service=e.service();service.powershell_runner=lambda *_:pytest.fail('unowned suffix native call')
    assert not resume(e,'reconcile_relocate_apply',service)['ok'] and not e.db_rows


def test_actual_mover_policy_header_must_match_state(tmp_path):
    e,state,log=bundle_env(tmp_path)
    log.write_text(json.dumps({'_meta':{'kind':'move_apply','on_dst_exists':'rename_suffix'}})+'\n'+log.read_text())
    service=e.service();service.powershell_runner=lambda *_:pytest.fail('policy mismatch native call')
    assert not resume(e,'reconcile_relocate_apply',service)['ok']


@pytest.mark.parametrize('moved',[False,True])
def test_native_missing_log_only_admits_untouched_source_bundle(tmp_path,moved):
    e=native_env(tmp_path,companions=False);e[3].unlink()
    if moved:next((tmp_path/'src').iterdir()).unlink()
    else:
        for p in (tmp_path/'dst').iterdir():p.unlink()
    r,info=invoke(e)
    if moved:assert r.returncode==1 and info['error']=='recovery_missing_intent'
    else:assert r.returncode==0 and info['state']=='not_started'
    assert not e[3].exists()


@pytest.mark.parametrize('override',[False,True])
def test_public_missing_log_returns_continue_only_after_readonly_inspection(tmp_path,override):
    e,state,log=bundle_env(tmp_path);log.unlink()
    if override:
        state['onDstExists']='rename_suffix';chunked_apply.save_state(e.store.run_dir(e.run_id),state,'now')
    service=e.service();calls=[]
    def ps(script,args):
        assert args[args.index('-Mode')+1]=='inspect' and args[args.index('-LogSha256')+1]=='absent'
        calls.append(script);return {'ok':True,'state':'not_started'}
    service.powershell_runner=ps
    result=resume(e,'reconcile_relocate_apply',service)
    assert result['ok'] and result['nextActions'][0]['params']['resumeAction']=='continue_relocate_apply'
    assert len(calls)==1 and not log.exists() and not e.db_rows


def test_effective_apply_db_override_is_used_for_recovery_flush(tmp_path):
    e,state,log=bundle_env(tmp_path)
    state['db']=str(e.ops/'db'/'override.sqlite')
    chunked_apply.save_state(e.store.run_dir(e.run_id),state,'now')
    service=e.service();calls=[]
    service.powershell_runner=lambda *_:{'ok':True,'state':'ready'}
    assert resume(e,'reconcile_relocate_apply',service)['ok']
    def ps(script,args):
        with log.open('a') as f:f.write(json.dumps({**e.rows[0],'op':'move','ok':True,'bundle_complete':True})+'\n')
        return {'ok':True,'state':'complete'}
    service.powershell_runner=ps
    old=e._py
    def py(script,args,cwd=None):
        if script.name=='update_db_paths_from_move_apply.py':calls.append(args[args.index('--db')+1])
        return old(script,args,cwd)
    service.python_runner=py
    assert resume(e,'settle_interrupted_move',service)['ok']
    assert calls==[state['db']]


def test_completed_parents_flush_even_when_next_native_bundle_blocks(tmp_path):
    e,state,log=bundle_env(tmp_path)
    first={**e.rows[0], 'path_id':'prior', 'src':'B:\\Old\\prior.mp4','dst':'N:\\Show\\prior.mp4'}
    original=Path(state['internalPlan']);original.write_text(json.dumps(first)+'\n'+json.dumps(e.rows[0])+'\n')
    Path(state['chunks'][0]['localPlan']).write_text(original.read_text())
    manifest=e.store.read_run(e.run_id);manifest.artifacts['relocate_internal_move_plan'].sha256=sha256_file(original);e.store.write_run(manifest)
    log.write_text(json.dumps({**first,'op':'move','ok':True,'bundle_complete':True})+'\n'+log.read_text())
    service=e.service();service.powershell_runner=lambda *_:{'ok':False,'error':'recovery_mixed_partial_bundle_fail_closed'}
    result=resume(e,'reconcile_relocate_apply',service)
    assert not result['ok'] and e.db_rows=={first['src']}
    assert chunked_apply.load_state(e.store.run_dir(e.run_id))['inFlight'] is not None


@pytest.mark.parametrize('side',['src','dst'])
def test_late_companion_prevents_recovery_parent_success(tmp_path,side):
    e=native_env(tmp_path,companions=False);scripts,op,_,log=e
    # Inject a newly written sidecar after the initial cohort scan, during the
    # final video signature check. The shipped recovery algorithm is unchanged.
    with (scripts/'_move_bundle.ps1').open('a') as f:
        f.write('''
$script:oldSignature=${function:Assert-BundleSignature}
function Assert-BundleSignature($Path,$Expected) {
  & $script:oldSignature $Path $Expected
  $script:sigCalls++
  if ($script:sigCalls -eq 5) {
    [IO.File]::WriteAllText((Convert-ToLongPathLiteral ($env:LATE_BASE + '.program.txt')), 'late')
  }
}
''')
    os.environ['LATE_BASE']=op[side]
    try:r,info=invoke(e,'finalize')
    finally:os.environ.pop('LATE_BASE',None)
    assert r.returncode==1 and info['error']=='recovery_final_cohort_changed'
    assert not any(json.loads(l).get('bundle_complete') for l in log.read_text().splitlines())
