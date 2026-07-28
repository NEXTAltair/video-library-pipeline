---
name: "video-library-pipeline-move-review"
description: "Review, supersede, or apply a V2 run-scoped move plan through video_pipeline_resume."
metadata: {"openclaw":{"emoji":"📦","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Move Plan Review

## Rule

- Use only V2 public tools.
- Apply/move must be run-scoped: `runId` + plan `artifactId`.
- Require explicit user confirmation before resuming an action that applies a plan.
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
6. Ask for explicit approval to apply the current plan.
7. After approval, call the exact resume params returned by the workflow:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "artifactId": "<artifactId>",
     "resumeAction": "<resumeAction from nextActions>"
   }
   ```
8. Report final `phase`, `outcome`, diagnostics, and apply artifacts.

## Completion Criteria

- `phase == "complete"` with `outcome == "workflow_run_superseded"` means the stale snapshot was safely retired without moving files.
- `phase == "complete"` after apply means the V2 workflow is complete.
- `phase == "blocked"` or `phase == "failed"` means stop and report diagnostics.
- Distinguish physical move completion from metadata review, supersession, or DB-only operations.
