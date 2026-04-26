---
name: video-library-pipeline-relocate-review
description: Start and review a V2 relocate workflow for existing library files.
metadata: {"openclaw":{"emoji":"📁","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Relocate Existing Files

## Rule

- Use only V2 public tools.
- Start with `video_pipeline_start {"flow":"relocate"}`.
- Do not call hidden legacy relocation tools directly.
- Destination layout is determined by workflow logic and drive routes. Do not ask the user to choose drives or folder taxonomy.
- Relocate metadata follow-up actions `prepare_relocate_metadata` and `review_relocate_metadata` do not currently advance the V2 workflow. Do not replay them as progress actions; report the #125 workflow gap.

## Roots Rule

- When the user names a program/title to reorganize, scan the parent library root rather than a single possibly-contaminated folder.
- Example: for a title under `B:\VideoLibrary`, pass `roots:["B:\\VideoLibrary"]`.
- Use narrower roots only when the operator explicitly provides a safe parent subtree.

## Tool Sequence

1. Start a relocate run:
   ```json
   video_pipeline_start {
     "flow": "relocate",
     "roots": ["B:\\VideoLibrary"],
     "queueMissingMetadata": true,
     "writeMetadataQueueOnDryRun": true,
     "scanErrorPolicy": "warn",
     "scanRetryCount": 2
   }
   ```
2. Read `WorkflowResult`:
   - `runId`
   - `phase`
   - `outcome`
   - `artifacts`
   - `gates`
   - `nextActions`
   - `followUpToolCalls`
   - `diagnostics`
3. Branch by structured state:
   - `phase == "plan_ready"`: inspect the plan artifact and hand off to `skills/move-review/SKILL.md`.
   - `phase == "review_required"`: inspect gate artifacts and hand off to `skills/extract-review/SKILL.md` or report required review.
   - `phase == "complete"`: report the outcome, such as already-correct.
   - `phase == "blocked"` or `phase == "failed"`: report diagnostics and stop.
4. Resume only through supported returned follow-up params:
   - If the returned action is `prepare_relocate_metadata` or `review_relocate_metadata`, stop and report that relocate metadata continuation is not yet supported by the V2 public surface. This gap is tracked in #125.
   - Otherwise execute only supported returned follow-up params:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "...": "params from followUpToolCalls[].params"
   }
   ```

## Human Review Checklist

- The user reviewed the plan artifact before any apply action.
- The `artifactId` belongs to the same `runId`.
- Open review gates are resolved before apply.
- No latest-plan or path guessing is used.
- Relocate metadata actions are not treated as apply/progress actions until #125 is implemented.

## Unsupported Legacy Cleanup

Some older folder-contamination and direct-title-repair flows depended on hidden legacy public tools. In V2 public operation, report that those flows require a future V2 workflow unless the current run returns an explicit supported `video_pipeline_resume` action for them. Relocate metadata actions that only return another pending metadata action are workflow gaps tracked in #125, not supported continuation paths.
