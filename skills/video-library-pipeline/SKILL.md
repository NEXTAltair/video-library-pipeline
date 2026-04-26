---
name: video-library-pipeline
description: Run and inspect the video library pipeline through the V2 run-based OpenClaw tool surface.
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
- Execute in the main agent turn; do not delegate to subagents.

## Intent Mapping

| User intent | V2 action |
|---|---|
| Process new recordings from `sourceRoot` | Read `skills/inventory-review/SKILL.md`; start `flow: "source_root"` |
| Reorganize/relocate existing library files | Read `skills/relocate-review/SKILL.md`; start `flow: "relocate"` |
| Continue an existing run | Call `video_pipeline_status` with `runId` to inspect state. Resume only if you already have `followUpToolCalls` / `nextActions` from the latest `WorkflowResult`; status itself does not return resume actions. |
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
   - `nextActions` / `followUpToolCalls` only when the result is a `WorkflowResult` from `start` or `resume`
   - `diagnostics`
3. If a human review gate is present, inspect the referenced artifact and ask the user to review it.
4. After review or approval, call `video_pipeline_resume` only with exact params from a retained `WorkflowResult.followUpToolCalls[].params` or `nextActions[].params`.
5. Repeat until `phase` is `complete`, `blocked`, or `failed`.

`video_pipeline_status` returns the run manifest for inspection. It does not reconstruct actionable resume parameters. If no prior `WorkflowResult` with next actions is available, report that deterministic continuation is not available from status alone rather than guessing. Future implementation is tracked in #125.

## Reporting

- Always state `runId`, `flow`, `phase`, and `outcome`.
- For review gates, show `gate.id`, `gate.status`, and the artifact IDs the user must review.
- For apply/move plans, summarize artifact IDs and paths; do not invent destination decisions outside the artifact content.
- Distinguish "review required", "plan ready", "applied", "complete", "blocked", and "failed".

## Legacy Tool Guardrail

Legacy public tools from V1 are hidden after #108. Do not instruct the user to call hidden legacy tools such as `video_pipeline_analyze_and_move_videos`, `video_pipeline_reextract`, `video_pipeline_apply_reviewed_metadata`, `video_pipeline_relocate_existing_files`, or `video_pipeline_validate` as active V2 steps.
