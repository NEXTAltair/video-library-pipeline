import json
from pathlib import Path

import pytest

import video_pipeline.platform.native_media_input as inputs


@pytest.mark.parametrize('path,expected',[
    ('/mnt/n/'+'日'*90+'.mp4',True),
    ('/mnt/n/'+'a'*254+'.mp4',True),
    ('/mnt/n/short.mp4',False),
    ('/tmp/'+'日'*90+'.mp4',False),
])
def test_native_alias_is_only_for_unaddressable_wsl_ntfs_components(path,expected):
    assert inputs.requires_native_alias(path) is expected


def test_regular_input_does_not_touch_filesystem_or_native_bridge(tmp_path,monkeypatch):
    def forbidden(*args,**kwargs):raise AssertionError('unexpected native access')
    monkeypatch.setattr(inputs,'read_native_signatures',forbidden)
    path=tmp_path/'ordinary.mp4'
    with inputs.readable_media_input(path) as actual:
        assert actual==path and not path.exists()


@pytest.mark.parametrize('body_fails',[False,True])
def test_generated_alias_cleanup_on_success_or_decoder_failure(tmp_path,monkeypatch,body_fails):
    original=tmp_path/'source.mp4';original.write_bytes(b'keep original')
    signature={'size_bytes':13,'mtime_ns':1}
    monkeypatch.setattr(inputs,'requires_native_alias',lambda p:True)
    monkeypatch.setattr(inputs,'read_native_signatures',lambda paths:{inputs.native_path_key(original):{
        'exists':True,'signature':signature}})
    real_path=inputs.Path
    monkeypatch.setattr(inputs,'Path',lambda p: tmp_path if str(p)=='/mnt' else real_path(p))
    monkeypatch.setattr(inputs,'wsl_to_windows_path',str)
    actions=[]
    def operation(request,directory):
        actions.append(request.copy())
        if request['action']=='create':
            Path(request['alias']).write_bytes(b'keep original')
            return {'ok':True,'source_file_id':'native-id','alias_file_id':'native-id'}
        assert request['file_id']=='native-id'
        Path(request['alias']).unlink()
        return {'ok':True,'generated_alias_removed':True,'original_preserved':True}
    monkeypatch.setattr(inputs,'_native_alias_operation',operation)
    receipts=[]
    def run():
        with inputs.readable_media_input(original,receipts=receipts) as readable:
            assert readable.read_bytes()==original.read_bytes()
            if body_fails:raise ValueError('decoder failed')
    if body_fails:
        with pytest.raises(ValueError,match='decoder failed'):run()
    else:run()
    assert original.read_bytes()==b'keep original'
    assert [r['action'] for r in actions]==['create','cleanup']
    assert json.loads(Path(receipts[0]).read_text())['cleanup']['original_preserved']


def test_unsafe_original_never_creates_alias(tmp_path,monkeypatch):
    monkeypatch.setattr(inputs,'requires_native_alias',lambda p:True)
    source=tmp_path/'missing.mp4'
    monkeypatch.setattr(inputs,'read_native_signatures',lambda paths:{inputs.native_path_key(source):{
        'exists':False,'error':'native_reparse_ancestor'}})
    with pytest.raises(ValueError,match='Unsafe native media input'):
        with inputs.readable_media_input(source):pass
    assert not list(tmp_path.iterdir())


def test_failed_cleanup_preserves_alias_and_reports_failure(tmp_path,monkeypatch):
    source=tmp_path/'source.mp4';source.write_bytes(b'original')
    monkeypatch.setattr(inputs,'requires_native_alias',lambda p:True)
    monkeypatch.setattr(inputs,'read_native_signatures',lambda paths:{inputs.native_path_key(source):{
        'exists':True,'signature':{'size_bytes':8,'mtime_ns':1}}})
    real_path=inputs.Path
    monkeypatch.setattr(inputs,'Path',lambda p:tmp_path if str(p)=='/mnt' else real_path(p))
    monkeypatch.setattr(inputs,'wsl_to_windows_path',str)
    def operation(request,directory):
        if request['action']=='create':
            Path(request['alias']).write_bytes(b'original')
            return {'ok':True,'source_file_id':'old-id','alias_file_id':'old-id'}
        raise ValueError('alias_cleanup_identity_mismatch')
    monkeypatch.setattr(inputs,'_native_alias_operation',operation)
    receipts=[]
    with pytest.raises(ValueError,match='identity_mismatch'):
        with inputs.readable_media_input(source,receipts=receipts) as alias:
            assert alias.exists()
    receipt=json.loads(Path(receipts[0]).read_text())
    assert receipt['cleanup'] is None and 'identity_mismatch' in receipt['cleanup_error']
    assert Path(receipt['alias_path']).read_bytes()==source.read_bytes()==b'original'
