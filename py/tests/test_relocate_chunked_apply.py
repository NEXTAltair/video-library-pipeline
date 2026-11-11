"""Issue #138: chunked, resumable and recoverable relocate apply."""
import json
import sqlite3
from pathlib import Path

import pytest

from video_pipeline.workflows import (
    RelocateApplyConfig,
    RelocateWorkflowService,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowStore,
)
from workflow_cli import _cmd_status, _with_current_next_actions


class Killed(BaseException):
    """A dead process: not catchable by `except Exception`, like a deadline kill."""


class Env:
    """Fake Windows side: files exist as (src, dst) pairs; receipts mirror real ps1 output."""

    def __init__(self, tmp_path: Path, rows: int, *, chunk_rows: int = 10, budget: float = 3600.0):
        self.ops = tmp_path / "ops"
        (self.ops / "scripts").mkdir(parents=True)
        db = self.ops / "db" / "mediaops.sqlite"
        db.parent.mkdir(parents=True)
        db.write_text("db", encoding="utf-8")
        self.run_id = "run_chunked"
        self.rows = [
            {"path_id": f"p{i}", "src": rf"B:\Old\f{i:04d}.mp4", "dst": rf"N:\Show\f{i:04d}.mp4",
             "status": "planned", "precondition": {"size_bytes": 1000 + i}}
            for i in range(rows)
        ]
        store = WorkflowStore(self.ops)
        store.init_run(WorkflowFlow.RELOCATE, run_id=self.run_id,
                       config_snapshot={"windowsOpsRoot": str(self.ops), "db": str(db)})
        plan = self.ops / "runs" / self.run_id / "plan" / "relocate_plan.jsonl"
        plan.write_text("\n".join([json.dumps({"_meta": {}}), *(json.dumps(r) for r in self.rows)]) + "\n",
                        encoding="utf-8")
        store.register_artifact(self.run_id, artifact_type="relocate_plan", path=plan, producer="t",
                                artifact_id="relocate_plan")
        store.transition_run(self.run_id, WorkflowPhase.PLAN_READY)
        self.store = store
        self.chunk_rows, self.budget = chunk_rows, budget
        self.moved_calls: list[str] = []      # every src handed to the mover, in order
        self.db_rows: set[str] = set()        # srcs recorded in the fake DB
        self.on_chunk = None                  # callable(chunk_number, rows) -> kill after n rows or None
        self.chunk_calls = 0
        self.fail_srcs: dict[str, str] = {}
        self.probe = {}                       # src -> response for settle inspect
        self.settle_calls: list[tuple[str, str]] = []
        self.cfg = RelocateApplyConfig(windows_ops_root=str(self.ops), run_id=self.run_id)

    def service(self) -> RelocateWorkflowService:
        return RelocateWorkflowService(python_runner=self._py, powershell_runner=self._ps, py_root=self.ops,
                                       chunk_rows=self.chunk_rows, budget_seconds=self.budget)

    def _py(self, script: Path, args: list[str], _cwd=None) -> str:
        if script.name == "backup_mediaops_db.py":
            if args[args.index("--action") + 1] == "rotate":
                return json.dumps({"ok": True})
            path = self.ops / "db" / "backup.sqlite"
            with sqlite3.connect(path) as con:
                con.execute("CREATE TABLE t (id INTEGER)")
            return json.dumps({"ok": True, "backup_path": str(path)})
        if script.name == "update_db_paths_from_move_apply.py":
            log = Path(args[args.index("--applied") + 1])
            before = len(self.db_rows)
            for line in log.read_text(encoding="utf-8").splitlines():
                rec = json.loads(line)
                if rec.get("op") == "move" and rec.get("ok"):
                    self.db_rows.add(rec["src"])
            added = len(self.db_rows) - before
            return json.dumps({"updated": added, "events": added, "already_applied": 0})
        return json.dumps({"ok": True})

    def _ps(self, script: str, args: list[str]) -> dict:
        if script.endswith("settle_interrupted_move.ps1"):
            src, mode = args[args.index("-Src") + 1], args[args.index("-Mode") + 1]
            self.settle_calls.append((src, mode))
            info = dict(self.probe[src])
            if mode != "inspect":
                info["action"] = "source_deleted" if mode == "finalize" else "destination_deleted"
            return {"ok": True, "mode": mode, **info}
        assert script.endswith("apply_move_plan.ps1")
        plan = Path(args[args.index("-PlanJsonl") + 1])
        out = Path(args[args.index("-OutJsonl") + 1])
        assert not out.exists(), "ps1 refuses to overwrite a receipt log"
        rows = [json.loads(l) for l in plan.read_text(encoding="utf-8").splitlines()]
        rows = [r for r in rows if "_meta" not in r]
        self.chunk_calls += 1
        kill_after = self.on_chunk(self.chunk_calls, rows) if self.on_chunk else None
        lines = [json.dumps({"_meta": {"kind": "move_apply"}})]
        for n, row in enumerate(rows):
            if kill_after is not None and n == kill_after:
                out.write_text("\n".join(lines) + "\n", encoding="utf-8")
                raise Killed()
            self.moved_calls.append(row["src"])
            if row["src"] in self.fail_srcs:
                lines.append(json.dumps({"op": "move", "path_id": row["path_id"], "src": row["src"],
                                         "dst": row["dst"], "ok": False, "error": self.fail_srcs[row["src"]]}))
                continue
            lines.append(json.dumps({"op": "move", "path_id": row["path_id"], "src": row["src"],
                                     "dst": row["dst"], "ok": True}))
        out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {"out_jsonl": str(out)}


def resume(env: Env, action: str, service=None) -> dict:
    return (service or env.service()).resume(env.cfg, action=action).to_dict()


def next_params(payload: dict) -> dict:
    assert len(payload["nextActions"]) == 1
    return payload["nextActions"][0]["params"]


def test_small_plan_completes_in_one_call_with_per_chunk_db_updates(tmp_path):
    env = Env(tmp_path, rows=25)
    payload = resume(env, "apply_relocate_move_plan")
    assert payload["ok"] and payload["phase"] == "complete" and payload["outcome"] == "relocate_apply_complete"
    assert env.chunk_calls == 3
    assert len(env.moved_calls) == len(set(env.moved_calls)) == 25
    assert len(env.db_rows) == 25
    manifest = json.loads(env.store.manifest_path(env.run_id).read_text(encoding="utf-8"))
    assert manifest["artifactIds"] == ["relocate_plan", "relocate_db_backup", "relocate_internal_move_plan",
                                       "relocate_move_apply_log", "relocate_db_update", "relocate_move_apply_stats"]


def test_budget_returns_progress_and_blocks_a_second_apply(tmp_path):
    env = Env(tmp_path, rows=35, budget=0)
    first = resume(env, "apply_relocate_move_plan")
    assert first["ok"] and first["phase"] == "applying" and first["outcome"] == "relocate_apply_in_progress"
    assert next_params(first) == {"runId": env.run_id, "resumeAction": "continue_relocate_apply"}
    assert len(env.moved_calls) == 10 and len(env.db_rows) == 10  # recorded before the call returned

    status = _cmd_status(type("A", (), {"windows_ops_root": str(env.ops), "run_id": env.run_id, "limit": 5,
                                        "include_artifacts": False, "hints_path": ""})())["run"]
    assert status["applyProgress"]["done"] == 10 and status["applyProgress"]["total"] == 35
    assert status["nextActions"][0]["params"]["resumeAction"] == "continue_relocate_apply"

    before = env.store.manifest_path(env.run_id).read_bytes()
    again = resume(env, "apply_relocate_move_plan")
    assert not again["ok"] and again["phase"] == "applying"
    assert again["diagnostics"][-1]["code"] == "relocate_apply_already_started"
    assert env.store.manifest_path(env.run_id).read_bytes() == before
    assert len(env.moved_calls) == 10

    for _ in range(10):
        payload = resume(env, "continue_relocate_apply")
        if payload["phase"] == "complete":
            break
    assert payload["outcome"] == "relocate_apply_complete"
    assert len(env.moved_calls) == len(set(env.moved_calls)) == 35 and len(env.db_rows) == 35


def kill_chunk(env: Env, chunk: int, after: int) -> None:
    env.on_chunk = lambda number, rows: after if number == chunk else None


def run_until_killed(env: Env) -> None:
    with pytest.raises(Killed):
        resume(env, "apply_relocate_move_plan")


def test_killed_call_is_detected_and_blocks_continue_until_reconciled(tmp_path):
    env = Env(tmp_path, rows=30)
    kill_chunk(env, chunk=2, after=3)           # 10 + 3 moved, 4th row of chunk 2 is mid-move
    run_until_killed(env)
    assert len(env.db_rows) == 10               # chunk 2 receipts not recorded yet (process died)

    fresh = env.service()                       # a new process
    status = _cmd_status(type("A", (), {"windows_ops_root": str(env.ops), "run_id": env.run_id, "limit": 5,
                                        "include_artifacts": False, "hints_path": ""})())["run"]
    assert status["phase"] == "applying" and status["applyProgress"]["state"] == "interrupted"
    assert status["nextActions"][0]["params"]["resumeAction"] == "reconcile_relocate_apply"

    before = env.store.manifest_path(env.run_id).read_bytes()
    for action in ("continue_relocate_apply", "apply_relocate_move_plan"):
        rejected = resume(env, action, fresh)
        assert not rejected["ok"] and rejected["phase"] == "applying"
    assert env.store.manifest_path(env.run_id).read_bytes() == before
    assert len(env.moved_calls) == 13


def finish(env: Env) -> dict:
    env.on_chunk = None
    for _ in range(20):
        payload = resume(env, "continue_relocate_apply")
        if payload["phase"] == "complete":
            return payload
    raise AssertionError(payload)


def killed_at_row_4(tmp_path, probe: dict) -> tuple[Env, str]:
    env = Env(tmp_path, rows=30)
    kill_chunk(env, chunk=2, after=3)
    run_until_killed(env)
    candidate = env.rows[13]["src"]             # chunk 2 = rows 10..19, fourth row is index 13
    env.probe[candidate] = probe
    return env, candidate


def test_reconcile_not_started_resumes_without_double_moves(tmp_path):
    env, cand = killed_at_row_4(tmp_path, {"source_exists": True, "destination_exists": False})
    payload = resume(env, "reconcile_relocate_apply", env.service())
    assert payload["ok"] and payload["outcome"] == "relocate_apply_reconciled"
    assert len(env.db_rows) == 13               # the 3 receipts of the interrupted chunk are recorded now
    assert next_params(payload)["resumeAction"] == "continue_relocate_apply"
    done = finish(env)
    assert done["outcome"] == "relocate_apply_complete"
    assert len(env.moved_calls) == len(set(env.moved_calls)) == 30 and len(env.db_rows) == 30


def test_reconcile_moved_but_unrecorded_is_recorded_not_repeated(tmp_path):
    env, cand = killed_at_row_4(tmp_path, {"source_exists": False, "destination_exists": True,
                                           "destination_size": 1013})
    payload = resume(env, "reconcile_relocate_apply", env.service())
    assert payload["ok"] and cand in env.db_rows
    finish(env)
    assert cand not in env.moved_calls          # never handed to the mover again
    assert len(env.db_rows) == 30


def test_partial_copy_requires_settlement_then_discards_and_redoes_the_file(tmp_path):
    env, cand = killed_at_row_4(tmp_path, {"source_exists": True, "destination_exists": True,
                                           "source_size": 1013, "destination_size": 400, "identical": False})
    payload = resume(env, "reconcile_relocate_apply", env.service())
    assert payload["outcome"] == "relocate_apply_settlement_required"
    action = payload["nextActions"][0]
    assert action["params"]["resumeAction"] == "settle_interrupted_move" and action["requiresHumanInput"] is True
    assert env.settle_calls == [(cand, "inspect")]          # reconcile itself deleted nothing

    rejected = resume(env, "continue_relocate_apply", env.service())
    assert not rejected["ok"] and rejected["diagnostics"][-1]["code"] == "relocate_apply_settlement_required"

    settled = resume(env, "settle_interrupted_move", env.service())
    assert settled["ok"] and settled["outcome"] == "relocate_apply_settled"
    assert env.settle_calls[-1] == (cand, "discard")
    finish(env)
    assert env.moved_calls.count(cand) == 1                 # redone exactly once
    assert len(env.db_rows) == 30


def test_complete_copy_with_source_remaining_is_finalized_not_repeated(tmp_path):
    env, cand = killed_at_row_4(tmp_path, {"source_exists": True, "destination_exists": True,
                                           "source_size": 1013, "destination_size": 1013, "identical": True})
    payload = resume(env, "reconcile_relocate_apply", env.service())
    assert payload["outcome"] == "relocate_apply_settlement_required"
    resume(env, "settle_interrupted_move", env.service())
    assert env.settle_calls[-1] == (cand, "finalize")
    assert cand in env.db_rows
    finish(env)
    assert cand not in env.moved_calls and len(env.db_rows) == 30


def test_lost_file_blocks_reconcile_and_keeps_the_run_applying(tmp_path):
    env, cand = killed_at_row_4(tmp_path, {"source_exists": False, "destination_exists": False})
    payload = resume(env, "reconcile_relocate_apply", env.service())
    assert not payload["ok"] and payload["outcome"] == "relocate_apply_reconcile_blocked"
    assert payload["phase"] == "applying"
    assert payload["nextActions"][0]["params"]["resumeAction"] == "reconcile_relocate_apply"


def test_hundreds_of_files_with_budget_limits_and_two_kills_move_each_file_once(tmp_path):
    env = Env(tmp_path, rows=320, chunk_rows=25, budget=0)
    kills = {3: 5, 9: 0}                        # chunk number -> rows moved before the kill
    env.on_chunk = lambda number, rows: kills.get(number)
    payload = None
    for _ in range(80):
        try:
            payload = resume(env, "apply_relocate_move_plan" if payload is None else payload["nextActions"][0]["params"]["resumeAction"])
        except Killed:
            run = env.store.read_run(env.run_id)
            assert run.phase == "applying"
            row = None
            from video_pipeline.workflows import chunked_apply
            state = chunked_apply.load_state(env.store.run_dir(env.run_id))
            row = chunked_apply.candidate_row(state, state["inFlight"]["chunk"])
            env.probe[row["src"]] = {"source_exists": True, "destination_exists": False}
            payload = {"nextActions": [{"params": {"resumeAction": "reconcile_relocate_apply"}}]}
            continue
        if payload["phase"] == "complete":
            break
    assert payload["outcome"] == "relocate_apply_complete"
    assert len(env.moved_calls) == len(set(env.moved_calls)) == 320
    assert len(env.db_rows) == 320
    final = env.store.read_run(env.run_id)
    assert final.status == "complete"


def test_partial_failure_keeps_the_successes_and_offers_a_replan(tmp_path):
    # #139: one failed move must not turn a mostly successful run into a failed one.
    env = Env(tmp_path, rows=12)
    env.fail_srcs = {env.rows[3]["src"]: "dst_exists", env.rows[7]["src"]: "src_not_found"}
    payload = resume(env, "apply_relocate_move_plan")
    assert payload["ok"] is False and payload["outcome"] == "relocate_apply_partial"
    assert payload["phase"] == "complete"
    assert len(env.db_rows) == 10                                   # the successes are recorded
    diag = payload["diagnostics"][-1]
    assert diag["code"] == "relocate_apply_partial"
    assert diag["details"]["errorBreakdown"] == {"dst_exists": 1, "src_not_found": 1}
    action = payload["nextActions"][0]
    assert action["tool"] == "video_pipeline_start" and action["params"] == {"flow": "relocate"}
    assert env.store.read_run(env.run_id).status == "complete"


def test_total_failure_still_fails_the_run(tmp_path):
    env = Env(tmp_path, rows=3)
    env.fail_srcs = {r["src"]: "dst_exists" for r in env.rows}
    payload = resume(env, "apply_relocate_move_plan")
    assert payload["phase"] == "failed" and payload["outcome"] == "relocate_apply_failed"


def legacy_interrupted_env(tmp_path: Path, rows: int = 14, done: int = 5) -> Env:
    """A plan_ready run whose apply died under the old single-call code: internal plan and backup
    artifacts exist, a receipt log sits in ops/move, the DB has none of it."""
    env = Env(tmp_path, rows=rows)
    apply_dir = env.ops / "runs" / env.run_id / "apply"
    apply_dir.mkdir(exist_ok=True)
    internal = apply_dir / "relocate_internal_move_plan.jsonl"
    internal.write_text("\n".join([json.dumps({"_meta": {}}), *(json.dumps({k: r[k] for k in ("path_id", "src", "dst", "precondition")}) for r in env.rows)]) + "\n", encoding="utf-8")
    backup = env.ops / "db" / "backup.sqlite"
    backup.write_text("b", encoding="utf-8")
    env.store.register_artifact(env.run_id, artifact_type="relocate_db_backup", path=backup, producer="t", artifact_id="relocate_db_backup")
    env.store.register_artifact(env.run_id, artifact_type="relocate_internal_move_plan", path=internal, producer="t", artifact_id="relocate_internal_move_plan")
    move_dir = env.ops / "move"
    move_dir.mkdir()
    lines = [json.dumps({"_meta": {"kind": "move_apply", "plan": str(internal), "dry_run": False, "on_dst_exists": "error"}})]
    lines += [json.dumps({"op": "move", "path_id": r["path_id"], "src": r["src"], "dst": r["dst"], "ok": True}) for r in env.rows[:done]]
    (move_dir / "move_apply_20261110_000000_000_1.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    # an unrelated log of another plan must not be adopted
    other = json.dumps({"_meta": {"kind": "move_apply", "plan": str(env.ops / "elsewhere.jsonl"), "dry_run": False}})
    (move_dir / "move_apply_20261110_000000_000_2.jsonl").write_text(other + "\n", encoding="utf-8")
    return env


def test_legacy_interrupted_apply_is_recovered_through_public_actions(tmp_path):
    env = legacy_interrupted_env(tmp_path)
    cand = env.rows[5]["src"]
    env.probe[cand] = {"source_exists": True, "destination_exists": True,
                       "source_size": 1005, "destination_size": 1005, "identical": False}

    status = _cmd_status(type("A", (), {"windows_ops_root": str(env.ops), "run_id": env.run_id, "limit": 5,
                                        "include_artifacts": False, "hints_path": ""})())["run"]
    assert status["phase"] == "plan_ready"
    assert [a["params"]["resumeAction"] for a in status["nextActions"]] == ["reconcile_relocate_apply"]

    before = env.store.manifest_path(env.run_id).read_bytes()
    again = resume(env, "apply_relocate_move_plan")                # re-applying would collide; refused
    assert not again["ok"] and again["diagnostics"][-1]["code"] == "relocate_apply_already_started"
    shown = _with_current_next_actions(env.store, dict(again))     # what the CLI returns to the caller
    assert [a["params"]["resumeAction"] for a in shown["nextActions"]] == ["reconcile_relocate_apply"]
    assert env.store.manifest_path(env.run_id).read_bytes() == before

    adopted = resume(env, "reconcile_relocate_apply")
    assert adopted["outcome"] == "relocate_apply_settlement_required" and adopted["phase"] == "applying"
    assert len(env.db_rows) == 5                                   # the logged moves reach the DB now
    assert env.settle_calls == [(cand, "inspect")]

    settled = resume(env, "settle_interrupted_move")
    assert settled["outcome"] == "relocate_apply_settled" and env.settle_calls[-1] == (cand, "discard")
    done = finish(env)
    assert done["outcome"] == "relocate_apply_complete"
    assert env.moved_calls == [r["src"] for r in env.rows[5:]]     # the 5 logged files never move again
    assert len(env.db_rows) == 14
