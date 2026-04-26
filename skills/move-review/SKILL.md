---
name: video-library-pipeline-move-review
description: Review and apply a V2 run-scoped move plan through video_pipeline_resume.
metadata: {"openclaw":{"emoji":"📦","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Move Plan Review

## Rule

- Use only V2 public tools.
- Apply/move must be run-scoped: `runId` + plan `artifactId`.
- Require explicit user confirmation before resuming an action that applies a plan.
- Never pass a filesystem `planPath` guessed from a previous run or latest file.

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
3. Summarize the plan for the user:
   - `runId`
   - plan artifact ID and path
   - source/destination examples from preview when available
   - diagnostics or gates still attached to the run
4. Ask for explicit approval to apply the plan.
5. After approval, call the exact resume params returned by the workflow:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "artifactId": "<artifactId>",
     "resumeAction": "<resumeAction from nextActions>"
   }
   ```
6. Report final `phase`, `outcome`, diagnostics, and apply artifacts.

## Completion Criteria

- `phase == "complete"` means the V2 workflow is complete.
- `phase == "blocked"` or `phase == "failed"` means stop and report diagnostics.
- Distinguish physical move completion from metadata review or DB-only operations.
