---
name: "video-library-pipeline-inventory-review"
description: "Start a V2 sourceRoot workflow and handle run-scoped inventory, metadata, empty plans, and review gates."
metadata: {"openclaw":{"emoji":"📋","requires":{"plugins":["video-library-pipeline"]}}}
---

# SourceRoot V2 Run Start

## Rule

- Use only the V2 public tools.
- Start with `video_pipeline_start`; do not call hidden legacy tools.
- Do not infer latest files from `windowsOpsRoot/llm` or `windowsOpsRoot/move`.
- Stop for human review whenever the returned `nextActions` or `followUpToolCalls` require it.
- If `complete_empty_plan` is returned with `requiresHumanInput: false`, call its exact resume params directly. The plugin revalidates the artifact checksum, recorded `planned=0`, and absence of plan operations before completing the run.

## Tool Sequence

1. Call:
   ```json
   video_pipeline_start {
     "flow": "source_root"
   }
   ```
   Optional parameters:
   - `runId` when the operator needs a stable explicit ID
   - `maxFilesPerRun`
   - `allowNeedsReview`
   - `driveRoutesPath`
2. Read `WorkflowResult`:
   - `runId`
   - `phase`
   - `outcome`
   - `artifacts`
   - `gates`
   - `nextActions`
   - `followUpToolCalls`
   - `diagnostics`
3. If `nextActions` returns `complete_empty_plan` with `requiresHumanInput: false`, call the exact `video_pipeline_resume` params and verify `phase == "complete"`.
4. If `phase == "review_required"`, hand off to `skills/extract-review/SKILL.md` with the full result.
5. If `phase == "plan_ready"` with a human review action, hand off to `skills/move-review/SKILL.md` with the returned plan action.
6. If `phase == "blocked"` or `phase == "failed"`, report diagnostics and stop.

## Human Review Checklist

- Confirm the `runId` is included in every follow-up.
- Confirm all referenced artifacts belong to the same run.
- If a `ReviewGate` is open, inspect its artifact IDs before resuming.
- Do not continue from free-text `nextStep`; use structured `nextActions` or `followUpToolCalls`.
- Do not ask for approval when the plugin returns `complete_empty_plan` with `requiresHumanInput: false`; it performs no move operations.

## Handoff

- To metadata review: pass `runId`, open gate details, and artifact IDs from `gates`.
- To move review: pass the `followUpToolCalls` entry whose reason/action is plan review.
- Empty sourceRoot plans do not hand off to move review; complete them through their returned resume action.
