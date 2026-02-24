---
name: video-library-pipeline-extract-review
description: Stage 2 of interactive operation. Generate machine-extracted metadata candidates, export program YAML, and stop for human review.
metadata: {"openclaw":{"emoji":"🧠","requires":{"plugins":["video-library-pipeline"]}}}
---

# Stage 2: Extract + YAML + Human Review

## Rule

- Use plugin tools only. Do not call scripts directly.
- Keep execution in the main agent turn. Do not use subagents.
- This stage must save program info to YAML after extraction.
- This stage ends with human review. Do not continue to move/apply automatically.
- This stage is **metadata review only**. Do not ask the human to decide filesystem move destinations or folder taxonomy here.
- YAML output in this stage is a **human-facing review artifact**. The agent must not infer corrections from YAML prose/text; use structured tool fields (`reviewCandidates`, `reviewSummary`, `reviewGuidance`) for reporting.

## Review scope (required)

- Review in scope (what the human should confirm/fix in this stage):
  - `program_title` (series/program title, not episode description, not genre label)
  - `air_date`
  - `needs_review`
  - `needs_review_reason` (when present)
  - aliases/canonical title organization in the exported YAML
- Review out of scope (do not ask in this stage):
  - destination folder path decisions
  - physical move execution decisions
  - category/genre folder strategy
  - dedup decisions
- If unsure whether a phrase is title vs subtitle vs genre, ask a targeted question for the exact record path and exact column (`program_title` / `subtitle`) instead of proposing a folder/category decision.

## Labels used in this stage (required)

- `machine_extracted_unreviewed_metadata`
  - output of `video_pipeline_reextract`
  - candidate metadata generated mechanically by LLM/rules
  - even if written into DB (`path_metadata`), treat it as not yet visually verified by a human
- `human_reviewed_metadata`
  - metadata visually reviewed/edited by a human and reflected via `video_pipeline_apply_reviewed_metadata`
  - this is the metadata level used for move/apply decisions
- `auto_registered_file_facts`
  - `paths` / `observations` / register-type events
  - file existence/size/mtime observations, separate from program metadata interpretation

## Tool sequence

1. Call `video_pipeline_validate` with `{"checkWindowsInterop": true}`.
2. Call `video_pipeline_reextract` with:
   - `queuePath` from Stage 1 summary (`queue`)
   - optional `batchSize` / `maxBatches`
   - keep default `preserveHumanReviewed=true` (reviewed rows are protected from overwrite)
   - Treat output as **`machine_extracted_unreviewed_metadata`**
3. Call `video_pipeline_export_program_yaml` with:
   - `sourceJsonlPath` omitted (latest extraction output auto-detected), or explicit path
   - optional `outputPath` if user wants a fixed location
   - default is `${windowsOpsRoot}/llm/program_aliases_review_YYYYMMDD_HHMMSS.yaml`
4. If user manually edits extracted JSONL, call `video_pipeline_apply_reviewed_metadata`:
   - `sourceJsonlPath`: edited extraction JSONL path
   - default `markHumanReviewed=true`
   - default `allowNoContentChanges=false` (safety guard)
   - do not use `allowNoContentChanges=true` while review-risk rows remain (for example `suspiciousProgramTitleRows > 0` or `needsReviewFlagRows > 0`)
   - this step writes **`human_reviewed_metadata`** into DB (`path_metadata`)
   - if the tool reports no-content-change guard (`ok=false` with `reviewDiff.changedRowsCount == 0`), do not claim review was applied; ask for actual JSONL edits or an explicit override decision

## Human review checklist

- YAML file was generated successfully.
- Program titles/aliases in YAML are acceptable.
- Rows with `needs_review` are either fixed or intentionally kept for later.
- Manual fixes are applied to DB via `video_pipeline_apply_reviewed_metadata` (reflected as `human_reviewed_metadata`).
- Confirm `video_pipeline_apply_reviewed_metadata` actually applied reviewed edits:
  - do not treat it as successful review apply if the tool reports "raw extraction output" or "no content edits detected"
  - do not use `allowNoContentChanges=true` as a shortcut when suspicious titles or `needs_review` rows are still present
- User confirms YAML should be used as the next review baseline.

## Review reporting rule (required)

- When asking the human to review extraction results, do **not** give only generic themes (for example, "title consistency" or "YAML structure").
- Prefer the structured fields from `video_pipeline_export_program_yaml` tool result when available:
  - `reviewSummary`
  - `reviewCandidates`
  - `reviewGuidance`
- Treat YAML as human-readable review material only. Do not use YAML content as the agent's source of truth for automated correction decisions.
- For each review candidate you mention, include:
  - the exact record path (`path`)
  - the exact columns to review (`columns[]`)
  - the concrete reasons (`reasons[]`)
  - the current extracted values (`current.*`) when helpful
- If `reviewGuidance` is present, follow its in-scope / out-of-scope boundaries instead of inventing review topics.
- If `reviewCandidatesTruncated=true`, say that the list is partial and report the truncation explicitly.
- Only fall back to high-level review themes when the tool result does not include structured review candidates.
- Do not ask broad questions such as "which folder should this move to?" in this stage.
- Do not reinterpret the review task as genre classification unless the user explicitly asks for genre rules.
- When the user corrects a record, restate the correction as column updates (for example, `program_title` stays `X`; `subtitle`/description is not part of `program_title`) instead of inventing additional taxonomy.
- The real apply input is JSONL (`video_pipeline_apply_reviewed_metadata`). Do not claim YAML edits were applied unless the corresponding JSONL edits were applied successfully.

## Handoff

- Return:
  - extraction result summary
  - YAML output path
  - count of programs exported
  - concrete review candidates (path + columns + reasons) when available
- Ask user whether to proceed to Stage 3 (Move + Human review).
