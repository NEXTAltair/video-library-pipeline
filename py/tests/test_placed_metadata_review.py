"""#209: already_correct rows get a read-only metadata review/acceptance path, no moves."""
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from tests.test_relocate_workflow import RelocateWorkflowHarness
from video_pipeline.workflows import RelocateApplyConfig, WorkflowStore
from workflow_cli import _next_actions_for_run

SERIES = {"g05": ("ゴジラ", 5), "g14": ("ゴジラ", 14)}
PLACED = ["g05", "g14", "o1"]


def src(pid):
    return rf"B:\Library\Godzilla\{pid}.mp4"


class PlacedHarness(RelocateWorkflowHarness):
    def __init__(self, tmp_path, *, planned=False):
        super().__init__(tmp_path, run_id="placed209")
        self.planned = planned
        self.overrides = {}
        self.moves_after_accept = set()

    def python_runner(self, script, args, cwd=None):
        result = json.loads(super().python_runner(script, args, cwd))
        if script.name == "relocate_existing_files.py":
            plan = Path(result["planPath"])
            rows = []
            for pid in PLACED:
                row = dict(path_id=pid, src=src(pid), dst=src(pid), status="skipped", reason="already_correct",
                           mtimeUtc="2026-01-01T00:00:00Z")
                if pid in SERIES:
                    row.update(series_abbr=SERIES[pid][0], series_order=SERIES[pid][1])
                if pid in self.moves_after_accept:
                    row.update(status="planned", reason="recompute_destination", dst=rf"B:\Elsewhere\{pid}.mp4")
                rows.append(row)
            if self.planned:
                rows.append(dict(path_id="p1", src=r"B:\Library\Show\p1.mp4", dst=r"B:\Library\Show2\p1.mp4",
                                 status="planned"))
            plan.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
            result.update(alreadyCorrect=len(PLACED), plannedMoves=1 if self.planned else 0)
        elif script.name == "run_metadata_batches_promptv1.py":
            queue = Path(args[args.index("--queue") + 1])
            rows = [json.loads(l) for l in queue.read_text(encoding="utf-8").splitlines() if l.strip()]
            Path(result["latestOutputJsonlPath"]).write_text("".join(json.dumps({
                "path_id": r["path_id"], "path": r["path"], "program_title": "Godzilla",
                "genre": "ドキュメンタリー・情報", "air_date": "2026-01-01", "needs_review": False},
                ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
        elif script.name == "export_program_yaml.py":
            source = args[args.index("--source-jsonl") + 1]
            Path(result["outputPath"]).write_text(yaml.safe_dump(
                {"source_jsonl": source, "hints": [], "row_overrides": self.overrides}, allow_unicode=True),
                encoding="utf-8")
        return json.dumps(result)

    def store(self):
        return WorkflowStore(Path(self.cfg.windows_ops_root))

    def run(self):
        return self.store().read_run(self.cfg.run_id)

    def resume(self, params, **extra):
        cfg = RelocateApplyConfig(windows_ops_root=self.cfg.windows_ops_root, run_id=params["runId"],
                                  artifact_id=params.get("artifactId", "relocate_plan"),
                                  gate_id=params.get("gateId", ""), artifact_ids=params.get("artifactIds", []),
                                  review_yaml_paths=params.get("reviewYamlPaths", []), **extra)
        return self.service().resume(cfg, action=params["resumeAction"])

    def seed_db(self):
        from mediaops_schema import connect_db, create_schema_if_needed
        db = Path(self.cfg.db)
        db.unlink()
        con = connect_db(str(db))
        create_schema_if_needed(con)
        for pid in PLACED:
            con.execute("INSERT INTO paths(path_id,path,created_at,updated_at) VALUES(?,?,?,?)",
                        (pid, src(pid), "before", "before"))
            data = json.dumps({"program_title": "Godzilla", "genre": "ドキュメンタリー・情報",
                               "air_date": "2026-01-01", "needs_review": False})
            con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,"
                        "human_reviewed) VALUES(?,?,?,?,?,?,?)",
                        (pid, "human_reviewed", data, "before", "Godzilla", "2026-01-01", 1))
        con.commit()
        con.close()


def action(result, name):
    return next(a for a in result.next_actions if a.action == name)


def series_of(db, pid):
    con = sqlite3.connect(db)
    rows = con.execute("SELECT data_json FROM path_metadata WHERE path_id=? ORDER BY rowid", (pid,)).fetchall()
    data = json.loads(rows[-1][0])
    return data.get("series_abbr"), data.get("series_order")


def test_complete_already_correct_run_offers_placed_review_and_prepares_read_only(tmp_path):
    h = PlacedHarness(tmp_path)
    start = h.dry_run()
    assert (start.ok, start.phase, start.outcome) == (True, "complete", "relocate_already_correct")
    offered = action(start, "review_placed_metadata")
    assert offered.params == {"runId": "placed209", "artifactId": "relocate_plan",
                              "resumeAction": "prepare_placed_metadata"}
    assert offered.to_dict() in [a.to_dict() for a in _next_actions_for_run(h.run())]
    db = Path(h.cfg.db).read_bytes()

    prepared = h.resume(offered.params)
    assert prepared.phase == "complete" and prepared.outcome == "relocate_placed_metadata_review_required"
    review = action(prepared, "review_placed_metadata_yaml")
    assert review.params["resumeAction"] == "apply_placed_metadata"
    assert not any(a.action == "review_placed_metadata" for a in prepared.next_actions)
    run = h.run()
    queue = next(a for a in run.artifacts.values() if a.type == "relocate_placed_metadata_queue")
    queued = [json.loads(l)["path_id"] for l in Path(queue.path).read_text().splitlines()]
    # Default cohort: owner-numbered rows (<略称>#NN in a series folder).
    assert queued == ["g05", "g14"] and queue.metadata["scope"] == "series_order"
    assert queue.metadata["seriesOrderSuggestions"]["g14"] == {"series_abbr": "ゴジラ", "series_order": 14}
    extract_args = next(a for name, a in h.calls if name == "run_metadata_batches_promptv1.py")
    assert "--output-only" in extract_args and "--ignore-human-reviewed" not in extract_args
    assert run.review_gates[review.params["gateId"]].type == "placed_metadata_review"
    assert Path(h.cfg.db).read_bytes() == db
    assert not any("--apply" in a for _, a in h.calls)
    # Repeating preparation while the gate is open is rejected without extraction.
    n = len(h.calls)
    assert not h.resume(offered.params).ok and len(h.calls) == n


def test_accept_series_order_row_overrides_without_moves(tmp_path):
    h = PlacedHarness(tmp_path)
    h.seed_db()
    h.overrides = {"g05": {"series_order": 5, "series_abbr": "ゴジラ", "review_note": "本人の通し番号 ゴジラ#05"},
                   "g14": {"series_order": 14, "series_abbr": "ゴジラ", "review_note": "本人の通し番号 ゴジラ#14"}}
    start = h.dry_run()
    prepared = h.resume(action(start, "review_placed_metadata").params)
    review = action(prepared, "review_placed_metadata_yaml")
    planner_calls = sum(1 for name, _ in h.calls if name == "relocate_existing_files.py")

    accepted = h.resume(review.params)
    assert accepted.ok, accepted.to_dict()
    assert (accepted.phase, accepted.outcome) == ("complete", "relocate_placed_metadata_accepted")
    assert series_of(h.cfg.db, "g05") == ("ゴジラ", 5)
    assert series_of(h.cfg.db, "g14") == ("ゴジラ", 14)
    run = h.run()
    assert run.review_gates[review.params["gateId"]].status == "approved"
    recheck = next(a for a in run.artifacts.values() if a.type == "relocate_placed_metadata_recheck")
    assert recheck.metadata["recheck"]["cohortNotAlreadyCorrect"] == []
    assert recheck.metadata["recheck"]["cohortAlreadyCorrect"] == 2
    assert sum(1 for name, _ in h.calls if name == "relocate_existing_files.py") == planner_calls + 1
    assert not any("--apply" in a for _, a in h.calls)
    # One acceptance per plan: nothing placed is offered again.
    assert not any(a.action.startswith("review_placed") for a in _next_actions_for_run(run))
    assert not any(a.action.startswith("review_placed") for a in accepted.next_actions)


def test_placed_acceptance_requires_review_note(tmp_path):
    h = PlacedHarness(tmp_path)
    h.seed_db()
    h.overrides = {"g05": {"series_order": 5, "series_abbr": "ゴジラ"}}
    prepared = h.resume(action(h.dry_run(), "review_placed_metadata").params)
    review = action(prepared, "review_placed_metadata_yaml")
    db = Path(h.cfg.db).read_bytes()
    rejected = h.resume(review.params)
    assert not rejected.ok and rejected.outcome == "placed_metadata_rejected"
    assert "review_note" in rejected.diagnostics[-1].message
    assert Path(h.cfg.db).read_bytes() == db
    assert h.run().review_gates[review.params["gateId"]].status == "open"


def test_recheck_reports_rows_that_would_move(tmp_path):
    h = PlacedHarness(tmp_path)
    h.seed_db()
    h.overrides = {"g05": {"series_order": 5, "series_abbr": "ゴジラ", "review_note": "通し番号"}}
    review = action(h.resume(action(h.dry_run(), "review_placed_metadata").params), "review_placed_metadata_yaml")
    h.moves_after_accept = {"g14"}
    accepted = h.resume(review.params)
    assert not accepted.ok and accepted.outcome == "relocate_placed_metadata_accepted_moves_pending"
    assert accepted.phase == "complete"
    assert accepted.diagnostics[-1].details["recheck"]["cohortNotAlreadyCorrect"] == ["g14"]
    assert series_of(h.cfg.db, "g05") == ("ゴジラ", 5)


def test_selected_path_ids_narrow_cohort_and_reject_non_placed(tmp_path):
    h = PlacedHarness(tmp_path)
    params = action(h.dry_run(), "review_placed_metadata").params
    n = len(h.calls)
    bad = h.resume(params, selected_path_ids=["g05", "nope"])
    assert not bad.ok and len(h.calls) == n
    assert not any(a.type == "relocate_placed_metadata_queue" for a in h.run().artifacts.values())
    good = h.resume(params, selected_path_ids=["o1"])
    assert good.outcome == "relocate_placed_metadata_review_required"
    queue = next(a for a in h.run().artifacts.values() if a.type == "relocate_placed_metadata_queue")
    assert [json.loads(l)["path_id"] for l in Path(queue.path).read_text().splitlines()] == ["o1"]
    assert queue.metadata["scope"] == "selected"


def test_plan_ready_run_offers_both_reviews_and_placed_gate_does_not_block_apply(tmp_path):
    h = PlacedHarness(tmp_path, planned=True)
    start = h.dry_run()
    assert start.phase == "plan_ready"
    names = [a.action for a in start.next_actions]
    assert {"review_plan", "review_planned_metadata", "review_placed_metadata"} <= set(names)
    assert {"review_plan", "review_placed_metadata"} <= {a.action for a in _next_actions_for_run(h.run())}
    prepared = h.resume(action(start, "review_placed_metadata").params)
    assert prepared.phase == "plan_ready"
    from video_pipeline.workflows import RelocateWorkflowService  # noqa: F401
    plan = h.service()._validate_apply_plan(h.store(), h.cfg.run_id, "relocate_plan")
    assert plan.id == "relocate_plan"


@pytest.mark.parametrize("state", ["blocked", "superseded", "applied_complete"])
def test_placed_review_not_offered_on_dead_or_applied_runs(tmp_path, state):
    h = PlacedHarness(tmp_path)
    h.dry_run()
    store = h.store()
    run = store.read_run(h.cfg.run_id)
    if state == "blocked":
        run.phase, run.status = "blocked", "blocked"
    if state == "superseded":
        run.resolution = {"type": "superseded"}
    if state == "applied_complete":
        run.artifacts["relocate_internal_move_plan"] = run.artifacts["relocate_plan"]
    store.write_run(run)
    run = store.read_run(h.cfg.run_id)
    assert not any(a.action.startswith("review_placed") for a in _next_actions_for_run(run))
    n = len(h.calls)
    rejected = h.resume({"runId": h.cfg.run_id, "artifactId": "relocate_plan",
                         "resumeAction": "prepare_placed_metadata"})
    assert not rejected.ok and len(h.calls) == n
