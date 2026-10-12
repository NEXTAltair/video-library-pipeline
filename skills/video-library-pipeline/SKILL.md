---
name: "video-library-pipeline"
description: "Run and inspect the video library pipeline through the V2 run-based OpenClaw tool surface."
metadata: {"openclaw":{"emoji":"🎬","requires":{"plugins":["video-library-pipeline"]},"localReads":["~/.openclaw/openclaw.json"]}}
---

# video-library-pipeline

This skill is the V2 orchestrator for `video-library-pipeline`.

## V2 Rules

- Use plugin tools for V2 workflow operations. Do not bypass them with Python, PowerShell, shell commands, hidden tools, or manual manifest edits. This boundary does not prohibit user-authorized repository repair, focused tests, or explicit maintenance/admin paths.
- The active public tool surface is limited to:
  - `video_pipeline_start`
  - `video_pipeline_resume`
  - `video_pipeline_status`
  - `video_pipeline_inspect_artifact`
- Treat `WorkflowResult.nextActions` and `followUpToolCalls` as the source of truth for the next operation.
- Do not infer "latest" JSONL/YAML/plan files. Use `runId`, `artifactId`, `ReviewGate.artifactIds`, and artifact paths returned by the run.
- Human review blocks acceptance/application, not inspection or candidate repair. Complete available non-applying preparation before asking the user to approve a concrete result. An open `ReviewGate` does not block `reextract_relocate_metadata` returned with `requiresHumanInput:false`; this writes replacement candidates using a read-only DB connection and leaves a new review gate open.
- For review-required actions, establish approval using the Continuation and Authorization Contract below. A matching earlier approval remains valid; the review flag does not require another confirmation on every turn or regenerated plan.
- If `nextActions` returns `complete_empty_plan` with `requiresHumanInput: false`, call the exact `video_pipeline_resume` params directly. It only completes a checksum-verified sourceRoot plan whose recorded count is zero and whose JSONL contains no operations.
- If `nextActions` returns `register_unregistered` with `requiresHumanInput: false`, call the exact resume params directly. This action only registers missing paths/observations/events; it uses the non-moving registration mode and then reruns dry-run so the new files enter metadata preparation.
- Execute in the main agent turn; do not delegate to subagents.

## Continuation and Authorization Contract

All branch skills use this contract; load this skill if it is not already available
in the current turn. Branch handoff means continuing work in the same main agent,
not ending the turn or delegating ownership.

1. Inspect the current structured result and referenced artifacts. `ok:false` can
   mean review/preparation is required, not that the task failed. `nextStep` is
   advisory prose; `nextActions` / `followUpToolCalls`, gates, and artifact contents
   determine the action and its exact params. Finish applicable non-applying
   inspection/repair before asking for approval; do not execute all listed actions
   blindly or reuse the old applying params after a repair changes artifacts.
2. For `requiresHumanInput:true` / `requiresHumanReview:true`, check the user's
   existing explicit approval before asking again. Compare operation type,
   reviewed metadata values or planned recordings, titles/roots, destinations,
   collision policy, and protected exclusions against the current artifacts.
   A fresh `runId` or artifact ID alone does not invalidate scope-based approval;
   approval explicitly limited to one exact artifact does not cover its replacement.
   A generic "continue" is not new approval for an unapproved applying action.
3. If the inspected current action is fully within the approved scope and all
   workflow guards pass, execute its returned params without another question.
   Metadata acceptance and physical relocation are separate scopes: approval of
   one does not automatically approve the other. Preserve holds, quarantines,
   title mismatches, and other exclusions; never broaden scope, permanently delete,
   overwrite destinations, weaken checks, or mark gates approved by hand.
4. If approval is genuinely absent or scope has changed, first finish the concrete
   review and repair. Ask once for the specific unresolved decision/application,
   with counts, exceptions, destinations and effect in chat. Artifact links are
   optional verification aids; do not require opening files or a separate ritual
   "review complete" response when the user's concrete approval covers the result.
5. `blocked` / `failed` is a workflow-run state, not automatically the end of the
   user's task. Inspect diagnostics, artifacts and current status; reconcile any
   partial effects before retrying. Continue bounded, authorized reversible repair
   and focused verification, then use a supported returned action. If the old run
   is terminal, preserve it as evidence and start a fresh public same-flow preview
   with the same semantic scope only after the cause is resolved. Recalculate a
   plan after successful metadata acceptance; do not reapply metadata to recover
   a later planning error. Never guess resume params or loop on unchanged failure.
6. A failed/truncated tool response does not prove no mutation occurred. Check
   authoritative status plus relevant apply receipts/DB/native state before retry;
   never repeat an application already verified complete. If execution state is
   still uncertain, inspect rather than apply again.
7. Before releasing a turn for a genuine external wait, persist a small checkpoint
   through available internal continuity storage: exact run/artifact/gate IDs,
   completed effects, approval scope/evidence, next supported action, blocker,
   owner and resume trigger. Verify that the continuation was accepted by a
   supported mechanism; an intention, process launch, or `nothing_pending` receipt
   is not a registered continuation. Process completion is not completion of its
   dependent workflow step. See [Reload continuation](references/reload-continuation.md)
   only when runtime maintenance is needed.
8. End with a final result only when the requested outcome is verified or no
   authorized step can proceed. In the latter case state the actual blocker,
   remaining work, owner and resume condition. Do not call an idle/failed process
   "waiting", promise unverified automatic resumption, or ask the user to prompt
   again when an authorized next step can run now.

**Done when:** the task's requested effects and protected exclusions are verified,
or a concrete external dependency/unapproved scope is documented with all available
non-applying preparation complete. A reload, one accepted gate, or one terminal
run alone does not establish task completion.

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

### Explicit recording-dedup maintenance CLI

When the user asks to configure duplicate-recording automation, the explicit
maintenance path is `openclaw video-pipeline-dedup`, documented in the repository
README. This is not a hidden V1 tool and does not resume or approve V2 gates.
The "plugin tools only" rule above governs V2 workflow operations, not development
or this explicitly requested maintenance CLI. Use preview first; `--apply` performs
recoverable quarantine, never permanent deletion. Preserve original and repeat
broadcasts separately, including byte-identical recordings; unknown flags are not
original broadcasts. Never invoke the old rebroadcast-deleting adapters for this
policy. Restoration uses `--restore-run <exact-dedup-run-id>`, preview by default.

If the request targets an already-existing directory tree under the library, treat it as the relocate workflow, not the sourceRoot workflow.

## Standard V2 Loop

1. Start a run or inspect current state:
   - New sourceRoot run: `video_pipeline_start {"flow":"source_root"}`
   - New relocate run: `video_pipeline_start {"flow":"relocate"}`; use `programTitles` for named works and omit roots to retain configured parent scans unless the user specifies a safe subtree.
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
3. If a human review gate is present, inspect the referenced artifact. If filename/date conflicts return `reextract_relocate_metadata` with `requiresHumanInput:false`, execute its exact params, inspect the replacement artifacts, and verify the conflicts are resolved. Do not approve the gate as part of repair. Summarize the finished review and genuine unresolved decisions in chat; do not require the user to open files.
4. If `complete_empty_plan` or `register_unregistered` is returned with `requiresHumanInput: false`, call its exact resume params and verify the run advances.
5. Resolve approval through the shared contract and call `video_pipeline_resume` only with exact params from the current `followUpToolCalls[].params` or `nextActions[].params`.
6. Verify the action's effects and continue to the next requested stage. For `blocked` / `failed`, follow the recovery branch above; for `complete`, verify the user's requested outcome, not just this run's phase. Stop only for verified completion, a genuine external dependency, or an unresolved approval/scope decision after preparation.

`video_pipeline_status` reconstructs actionable `nextActions` from the run manifest for non-terminal review and plan phases. If no action is returned, do not guess a latest JSONL/YAML/plan path.

## Reporting

- Always state `runId`, `flow`, `phase`, and `outcome`.
- For review gates, show `gate.id`, `gate.status`, the inspected artifact IDs, and the concrete decision that still needs approval, if any.
- For apply/move plans, summarize artifact IDs and paths; do not invent destination decisions outside the artifact content.
- Distinguish "review required", "plan ready", "applied", "complete", "blocked", "failed", and "superseded".

## Legacy Tool Guardrail

Legacy public tools from V1 are hidden after #108. Do not instruct the user to call hidden legacy tools such as `video_pipeline_analyze_and_move_videos`, `video_pipeline_reextract`, `video_pipeline_apply_reviewed_metadata`, `video_pipeline_relocate_existing_files`, or `video_pipeline_validate` as active V2 steps.
