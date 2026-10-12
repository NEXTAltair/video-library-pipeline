---
name: video-library-pipeline-extract-review
description: Handle V2 metadata ReviewGate artifacts and resume run-scoped metadata review actions.
metadata: {"openclaw":{"emoji":"🧠","requires":{"plugins":["video-library-pipeline"]}}}
---

# V2 Metadata Review Gate

Read `video-library-pipeline` if not already loaded in this turn. Apply its
Continuation and Authorization Contract throughout this branch.

## Rule

- Use only `video_pipeline_status`, `video_pipeline_inspect_artifact`, and supported `video_pipeline_resume` actions.
- Do not search for latest extraction JSONL/YAML files.
- The run manifest and `ReviewGate.artifactIds` define the review scope.
- Do not ask the user to decide destination folders in this stage.
- SourceRoot metadata review resumes through the returned `apply_reviewed_metadata` action when explicit approval covers the inspected current metadata. Check prior approval first; do not require a separate YAML-opening/review-complete ritual.

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
   Before requesting approval, finish assistant-led inspection and available
   non-applying repair. For relocate filename/date conflicts, use the returned
   `reextract_relocate_metadata` action with `requiresHumanInput:false`.
   It verifies the run-scoped queue, writes new candidate artifacts using a
   read-only DB connection, supersedes the old review rather than approving it,
   and leaves the replacement gate open. Reinspect and report the corrected
   counts and genuine exceptions in chat; file opening is not required.
5. Compare the inspected metadata with the user's existing approval. If the current scope/values are covered, execute the current returned follow-up params without another question; otherwise ask once for concrete metadata acceptance after completing repair:
   ```json
   video_pipeline_resume {
     "runId": "<runId>",
     "resumeAction": "<resumeAction from nextActions>",
     "...": "other params from nextActions.params"
   }
   ```

## Required Behavior

- Use params exactly as returned by `followUpToolCalls` or `nextActions.params`.
- If a follow-up has `requiresHumanReview: true` or its next action has `requiresHumanInput:true`, do not call it without matching user approval. Earlier explicit approval satisfies this requirement when the inspected current values/scope are unchanged; no new confirmation is needed merely because a turn or run changed.
- This restriction concerns review acceptance/application, not the returned non-applying repair action or authorized local development needed to make repair possible.
- Do not synthesize resume params by scanning latest files; use status/result `nextActions`.
- After resume, read the returned `phase`:
  - `plan_ready`: hand off to `skills/move-review/SKILL.md`.
  - `review_required`: repeat this skill for the new/open gate.
  - `blocked` or `failed`: inspect partial effects and follow the shared recovery contract; do not reapply accepted metadata to fix a later planning error.

## Legacy Guardrail

The old standalone extraction/apply tools are hidden in the V2 public surface. Do not instruct the operator to call hidden legacy tools directly. If a required repair action is missing, diagnose it and continue authorized repository repair/tests under the shared contract; do not guess a latest JSONL/YAML path or approve unchanged erroneous candidates. If repair is outside the authorized scope, name that precise blocker after completing available inspection.
