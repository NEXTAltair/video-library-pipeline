---
name: video-library-pipeline-relocate-review
description: Start and review a V2 relocate workflow for existing library files.
metadata: {"openclaw":{"emoji":"📁","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Relocate Existing Files

Read `video-library-pipeline` if not already loaded in this turn. Apply its
Continuation and Authorization Contract throughout this branch.

## Rule

- Use only V2 public tools.
- Start a new preview with `video_pipeline_start {"flow":"relocate"}`; for existing work, inspect its exact run first and preserve completed effects.
- Do not call hidden legacy relocation tools directly.
- Destination layout is determined by workflow logic and drive routes. Do not ask the user to choose drives or folder taxonomy.
- Relocate metadata follow-up actions `prepare_relocate_metadata` and `review_relocate_metadata` are supported V2 resume actions. They re-evaluate the same run and may advance it to `plan_ready`.

## Roots Rule

- Omit `roots`/`rootsFilePath` to scan configured sourceRoot, destRoot, drive-route destinations and tsRoot together. Explicit roots are not expanded.
- For a single-work trial, set `programTitles:["<exact canonical work title>"]`; keep parent roots broad. Conflicting filename/metadata identities are held, not merged.
- Inspect the run's `relocate_routing_inventory` artifact for work locations, sizes, destination and reason counts alongside `relocate_plan`.
- Preview uses a read-only DB and does not read video content. Unresolved recording holds and quarantined files remain excluded.

- When the user names a program/title to reorganize, scan the parent library root rather than a single possibly-contaminated folder.
- Prefer configured parent scans plus `programTitles` for a named work; do not infer its full placement scope from one folder or drive.
- Use narrower roots only when the operator explicitly provides a safe parent subtree.

## Tool Sequence

1. Start a relocate run:
   ```json
   video_pipeline_start {
     "flow": "relocate",
     "queueMissingMetadata": true,
     "writeMetadataQueueOnDryRun": true,
     "scanErrorPolicy": "fail",
     "scanRetryCount": 2
   }
   ```
   For a named-work request, add `programTitles:["<exact canonical work title>"]`.
   Add explicit safe parent roots only when the user restricts the subtree. Fail
   closed on scan errors by default; do not lower the scan policy to force a plan
   through a repair. Do not start a new run merely because the existing result is
   review-required and already has a supported action.
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
   - `phase == "blocked"` or `phase == "failed"`: inspect partial effects and recover under the shared contract. After fixing a terminal planning failure, start a fresh same-flow preview with the same semantic scope; do not reapply already accepted metadata.
4. Resume only through returned follow-up params:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "...": "params from followUpToolCalls[].params"
   }
   ```

## Human Review Checklist

- The assistant inspected the current plan; the user's explicit approval covers its current scope. Chat summaries suffice for user review; file opening is not required.
- The `artifactId` belongs to the same `runId`.
- Open review gates are resolved before apply.
- No latest-plan or path guessing is used.
- For flagged review actions, check earlier explicit approval before asking again. Continue applicable non-applying preparation first; movement and metadata approval remain distinct scopes.

## Unsupported Legacy Cleanup

Some older folder-contamination and direct-title-repair flows depended on hidden legacy public tools. In V2 public operation, report that those flows require a future V2 workflow unless the current run returns an explicit supported `video_pipeline_resume` action for them.
