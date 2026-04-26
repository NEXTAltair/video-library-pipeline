---
name: video-library-pipeline-extract-review
description: Handle V2 metadata ReviewGate artifacts and resume run-scoped metadata review actions.
metadata: {"openclaw":{"emoji":"🧠","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Metadata Review Gate

## Rule

- Use only `video_pipeline_status`, `video_pipeline_inspect_artifact`, and supported `video_pipeline_resume` actions.
- Do not search for latest extraction JSONL/YAML files.
- The run manifest and `ReviewGate.artifactIds` define the review scope.
- Do not ask the user to decide destination folders in this stage.
- SourceRoot metadata review resume is not supported in the current V2 public surface when the returned action is `apply_reviewed_metadata`. Do not call that unsupported action. Future implementation is tracked in #125.

## Review Scope

Review only metadata fields represented by the run artifact:
- `program_title` should be the series/program title only.
- `air_date` should be correct when present.
- `needs_review` and review reasons should be addressed or intentionally left unresolved.
- YAML alias/canonical-title entries should map contaminated or variant titles to the correct canonical title.

Out of scope:
- destination folder taxonomy
- physical move approval
- dedup decisions
- genre routing decisions

## Tool Sequence

1. If the current result is not already available, call:
   ```json
   video_pipeline_status {
     "runId": "<runId>",
     "includeArtifacts": true
   }
   ```
2. Find open review gates in `gates` / `reviewGates`.
3. For each referenced artifact ID, inspect it:
   ```json
   video_pipeline_inspect_artifact {
     "runId": "<runId>",
     "artifactId": "<artifactId>",
     "includeContentPreview": true
   }
   ```
4. Present the artifact path and concrete review scope to the user.
5. After the user confirms review is complete, check the returned follow-up action:
   - If `resumeAction == "apply_reviewed_metadata"` for a sourceRoot run, stop and report that this metadata review resume path is not yet supported by the V2 public surface.
   - Otherwise execute only supported returned follow-up params:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "resumeAction": "<supported resumeAction from nextActions>",
     "...": "other params from the retained WorkflowResult followUpToolCalls[].params"
   }
   ```

## Required Behavior

- Use params exactly as returned by a retained `WorkflowResult.followUpToolCalls` or `nextActions.params`.
- If a follow-up has `requiresHumanReview: true`, never call it before user confirmation.
- Do not synthesize resume params from `video_pipeline_status`; status is inspect-only and does not return actionable next steps.
- Do not call unsupported sourceRoot metadata review action `apply_reviewed_metadata`; report the workflow gap tracked in #125.
- After resume, read the returned `phase`:
  - `plan_ready`: hand off to `skills/move-review/SKILL.md`.
  - `review_required`: repeat this skill for the new/open gate.
  - `blocked` or `failed`: report diagnostics and stop.

## Legacy Guardrail

The old standalone extraction/apply tools are hidden in the V2 public surface. Do not instruct the operator to call hidden legacy tools directly. If metadata repair cannot be completed through the returned `video_pipeline_resume` action, report the #125 workflow gap rather than guessing a latest JSONL/YAML path.
