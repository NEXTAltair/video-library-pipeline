---
name: "video-library-pipeline-move-review"
description: "Review, supersede, or apply a V2 run-scoped move plan through video_pipeline_resume."
metadata: {"openclaw":{"emoji":"📦","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Move Plan Review

Read `video-library-pipeline` if not already loaded in this turn. Apply its
Continuation and Authorization Contract throughout this branch.

## Rule

- Use only V2 public tools.
- Apply/move must be run-scoped: `runId` + plan `artifactId`.
- Require explicit user approval covering the current inspected move scope. Reuse existing scope-based approval after comparing the current plan; a new turn/run/artifact does not itself require another question. Exact-artifact approval remains exact-artifact only.
- Never pass a filesystem `planPath` guessed from a previous run or latest file.
- Never apply a stale plan. If source or destination state may have changed, create a fresh same-flow dry-run and compare it first.

## Tool Sequence

1. Ensure the current run is `phase == "plan_ready"`.
   - If needed, call `video_pipeline_status {"runId":"<runId>", "includeArtifacts":true}`.
2. Inspect the plan artifact referenced by the plan-review action:
   ```json
   video_pipeline_inspect_artifact {
     "runId": "<runId>",
     "artifactId": "<artifactId>",
     "includeContentPreview": true
   }
   ```
3. Check freshness before proposing apply:
   - Compare plan age with the current date and subsequent runs.
   - If filesystem or metadata state may differ, start a fresh dry-run for the same flow and safe parent root.
   - Compare source existence, scanned count, planned count, metadata queue count, and destination examples.
4. If the old snapshot is invalid, do not apply it. Supersede it with the newer same-flow run:
   ```json
   video_pipeline_resume {
     "runId": "<old-run-id>",
     "resumeAction": "supersede_run",
     "supersededByRunId": "<new-run-id>",
     "reason": "<specific comparison evidence>"
   }
   ```
   This changes workflow state only and performs no physical move.
5. If the plan is still current, summarize it for the user:
   - `runId`
   - plan artifact ID and path
   - source/destination examples from preview when available
   - diagnostics or gates still attached to the run
6. Check existing approval against the whole current plan scope: operation, source recordings/titles/roots, destinations, collision policy and exclusions. Keep held, quarantined and title-mismatched recordings protected. If the plan is within the approved scope, continue without asking again. If approval is absent, narrower than the plan, or explicitly tied to an old artifact, finish the concrete review and ask once for the uncovered scope. Establish coverage using supported artifact inspection (`previewBytes` up to 65536) and run-scoped routing/diagnostic summaries; a few preview rows alone are not proof of whole-plan coverage. If the public surface cannot expose necessary evidence, diagnose and repair that inspection gap under the shared contract rather than asking the user to waive it.
7. Once covered by approval and all freshness/gate checks pass, call the exact current resume params returned by the workflow:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "artifactId": "<artifactId>",
     "resumeAction": "<resumeAction from nextActions>"
   }
   ```
8. Verify apply receipts and current state. Inspect failed/truncated responses before any retry; never repeat verified moves. Continue any remaining authorized stage, then report the requested outcome and protected exclusions.

## Completion Criteria

- `phase == "complete"` with `outcome == "workflow_run_superseded"` means the stale snapshot was safely retired without moving files.
- `phase == "complete"` after apply means the V2 workflow is complete.
- `phase == "blocked"` or `phase == "failed"` invokes the shared recovery contract: reconcile partial moves/DB updates, resolve bounded authorized causes and refresh the plan through public V2. Preserve failed-run evidence; never blindly retry apply or weaken guards.
- Distinguish physical move completion from metadata review, supersession, or DB-only operations.
