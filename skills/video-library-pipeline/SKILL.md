---
name: "video-library-pipeline"
description: "Run and inspect the video library pipeline through the V2 run-based OpenClaw tool surface."
metadata: {"openclaw":{"emoji":"🎬","requires":{"plugins":["video-library-pipeline"]},"localReads":["~/.openclaw/openclaw.json"]}}
---

# video-library-pipeline

This skill is the V2 orchestrator for `video-library-pipeline`.

## V2 Rules

- Use plugin tools only. Do not call Python, PowerShell, or shell commands directly.
- The active public tool surface is limited to:
  - `video_pipeline_start`
  - `video_pipeline_resume`
  - `video_pipeline_status`
  - `video_pipeline_inspect_artifact`
- Treat `WorkflowResult.nextActions` and `followUpToolCalls` as the source of truth for the next operation.
- Do not infer "latest" JSONL/YAML/plan files. Use `runId`, `artifactId`, `ReviewGate.artifactIds`, and artifact paths returned by the run.
- Human review is explicit. If a result has `requiresHumanReview: true` or an open `ReviewGate`, stop and ask the user to review the referenced artifact before resuming.
- If `nextActions` returns `complete_empty_plan` with `requiresHumanInput: false`, call the exact `video_pipeline_resume` params directly. It only completes a checksum-verified sourceRoot plan whose recorded count is zero and whose JSONL contains no operations.
- If `nextActions` returns `register_unregistered` with `requiresHumanInput: false`, call the exact resume params directly. This action only registers missing paths/observations/events; it uses the non-moving registration mode and then reruns dry-run so the new files enter metadata preparation.
- Execute in the main agent turn; do not delegate to subagents.

## Stale Run Supersession

- Never apply an old plan merely because its run is still active.
- When filesystem or metadata state may have changed, first run a fresh dry-run for the same flow and safe parent root.
- Compare the old artifact summary and preview with the fresh run: source existence, scanned count, planned count, metadata queue count, and destination layout.
- If the old snapshot is no longer valid, supersede it through the public resume tool:
  ```json
  video_pipeline_resume {
    "runId": "<old-run-id>",
    "resumeAction": "supersede_run",
    "supersededByRunId": "<newer-same-flow-run-id>",
    "reason": "<specific evidence that invalidated the old snapshot>"
  }
  ```
- `supersede_run` is state-only: it performs no file move. It requires an active old run, an existing newer run of the same flow, and a non-empty reason. It marks old artifacts and open gates superseded, completes the old run, and records the successor link in the manifest.
- Keep the fresh run active when it represents current human-review work. Do not supersede the current review gate merely to make the active count zero.

## Intent Mapping

| User intent | V2 action |
|---|---|
| Process new recordings from `sourceRoot` | Read `skills/inventory-review/SKILL.md`; start `flow: "source_root"` |
| Reorganize/relocate existing library files | Read `skills/relocate-review/SKILL.md`; start `flow: "relocate"` |
| Continue an existing run | Call `video_pipeline_status` with `runId` and `includeArtifacts:true`; use returned `nextActions` to resume. |
| Inspect review YAML, plan, diagnostics, or apply log | Call `video_pipeline_inspect_artifact` with `runId` and `artifactId` |
| Metadata review handoff | Read `skills/extract-review/SKILL.md`; use the run's `ReviewGate` and artifacts |
| Apply/move after plan review | Read `skills/move-review/SKILL.md`; resume the run with the plan action returned by `nextActions` |
| DB maintenance, EPG ingest, dedup, title repair, folder cleanup | Not exposed through the V2 public surface in this issue. Report that the operation requires a future V2 workflow or an explicit maintenance/admin path. |

If the request targets an already-existing directory tree under the library, treat it as the relocate workflow, not the sourceRoot workflow.

## Standard V2 Loop

1. Start a run or inspect current state:
   - New sourceRoot run: `video_pipeline_start {"flow":"source_root"}`
   - New relocate run: `video_pipeline_start {"flow":"relocate", "roots":[...]}`
   - Existing run status only: `video_pipeline_status {"runId":"<runId>", "includeArtifacts":true}`
2. Read the result fields:
   - `runId`
   - `flow`
   - `phase`
   - `outcome`
   - `artifacts`
   - `gates`
   - `nextActions` / `followUpToolCalls`
   - `diagnostics`
3. If a human review gate is present, inspect the referenced artifact and ask the user to review it.
4. If `complete_empty_plan` or `register_unregistered` is returned with `requiresHumanInput: false`, call its exact resume params and verify the run advances.
5. After other review or approval, call `video_pipeline_resume` only with exact params from `followUpToolCalls[].params` or `nextActions[].params`.
6. Repeat until `phase` is `complete`, `blocked`, or `failed`.

`video_pipeline_status` reconstructs actionable `nextActions` from the run manifest for non-terminal review and plan phases. If no action is returned, do not guess a latest JSONL/YAML/plan path.

## Reporting

- Always state `runId`, `flow`, `phase`, and `outcome`.
- For review gates, show `gate.id`, `gate.status`, and the artifact IDs the user must review.
- For apply/move plans, summarize artifact IDs and paths; do not invent destination decisions outside the artifact content.
- Distinguish "review required", "plan ready", "applied", "complete", "blocked", "failed", and "superseded".

## Legacy Tool Guardrail

Legacy public tools from V1 are hidden after #108. Do not instruct the user to call hidden legacy tools such as `video_pipeline_analyze_and_move_videos`, `video_pipeline_reextract`, `video_pipeline_apply_reviewed_metadata`, `video_pipeline_relocate_existing_files`, or `video_pipeline_validate` as active V2 steps.
