---
name: video-library-pipeline-inventory-review
description: Start a V2 sourceRoot workflow and handle run-scoped inventory, metadata, and review gates.
metadata: {"openclaw":{"emoji":"📋","requires":{"plugins":["video-library-pipeline"]}}}
---

# SourceRoot V2 Run Start

## Rule

- Use only the V2 public tools.
- Start with `video_pipeline_start`; do not call hidden legacy tools.
- Do not infer latest files from `windowsOpsRoot/llm` or `windowsOpsRoot/move`.
- Stop for human review whenever the returned `nextActions` or `followUpToolCalls` require it.

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
3. If `phase == "review_required"`, hand off to `skills/extract-review/SKILL.md` with the full result.
4. If `phase == "plan_ready"`, hand off to `skills/move-review/SKILL.md` with the returned plan action.
5. If `phase == "blocked"` or `phase == "failed"`, report diagnostics and stop.

## Human Review Checklist

- Confirm the `runId` is included in every follow-up.
- Confirm all referenced artifacts belong to the same run.
- If a `ReviewGate` is open, inspect its artifact IDs before resuming.
- Do not continue from free-text `nextStep`; use structured `nextActions` or `followUpToolCalls`.

## Handoff

- To metadata review: pass `runId`, open gate details, and artifact IDs from `gates`.
- To move review: pass the `followUpToolCalls` entry whose reason/action is plan review.
