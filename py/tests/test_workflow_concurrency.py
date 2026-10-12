from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import time

import pytest

from video_pipeline.platform.writer_lock import workflow_writer
from video_pipeline.workflows.store import WorkflowStore
from video_pipeline.platform.artifact_retention import maintenance_lock


def register(root, index):
    store = WorkflowStore(root)
    path = store.run_dir("run_concurrent") / "metadata" / f"{index}.jsonl"
    path.write_text(f'{{"index":{index}}}\n')
    return store.register_artifact("run_concurrent", artifact_type="test", artifact_id=f"artifact_{index}",
                                   path=path, producer="concurrency-test").id


@pytest.mark.parametrize("executor", [ThreadPoolExecutor, ProcessPoolExecutor])
def test_concurrent_registration_preserves_every_successful_write(tmp_path, executor):
    store = WorkflowStore(tmp_path)
    store.init_run("source_root", run_id="run_concurrent")
    with executor(max_workers=4) as pool:
        successful = [f.result(timeout=15) for f in [pool.submit(register, tmp_path, i) for i in range(24)]]
    run = store.read_run("run_concurrent")
    assert set(run.artifact_ids) == set(successful)
    assert len(run.artifact_ids) == 24
    assert run.revision == 25


def test_stale_direct_write_is_a_conflict_not_a_lost_update(tmp_path):
    store = WorkflowStore(tmp_path)
    store.init_run("source_root", run_id="run_concurrent")
    stale = store.read_run("run_concurrent")
    register(tmp_path, 1)
    before = store.manifest_path(stale.run_id).read_bytes()
    stale.phase = "failed"
    with pytest.raises(ValueError, match="revision conflict"):
        store.write_run(stale)
    assert store.manifest_path(stale.run_id).read_bytes() == before


def hold_scope(root, ready):
    with workflow_writer(root):
        ready.set()
        time.sleep(1)


def test_lock_is_reentrant_shared_by_other_store_and_maintenance_and_recovers_after_kill(tmp_path):
    store = WorkflowStore(tmp_path)
    with workflow_writer(tmp_path):
        store.init_run("source_root", run_id="run_concurrent")
        register(tmp_path, 0)
    job_root = tmp_path / "artifacts" / "job"
    job_root.mkdir(parents=True)
    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    process = ctx.Process(target=hold_scope, args=(tmp_path, ready))
    process.start()
    try:
        assert ready.wait(5)
        with pytest.raises(TimeoutError):
            with workflow_writer(tmp_path, timeout=0.1):
                pass
        # OS lock must also prevent maintenance while V2 owns the ops root.
        with ThreadPoolExecutor() as pool:
            future = pool.submit(lambda: _maintenance(job_root))
            time.sleep(0.05)
            assert not future.done()
            process.terminate()
            process.join(5)
            assert future.result(timeout=5)
        register(tmp_path, 1)
        assert len(store.read_run("run_concurrent").artifact_ids) == 2
    finally:
        if process.is_alive():
            process.terminate()
        process.join(5)


def _maintenance(root):
    with maintenance_lock(root):
        return True
