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
  - `program_title` (series/program title only — must NOT contain subtitle, episode description, or guest names. Folder names are derived from this field.)
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

## Metadata state labels

See main `video-library-pipeline` SKILL.md for definitions of `machine_extracted_unreviewed_metadata`, `human_reviewed_metadata`, and `auto_registered_file_facts`.

## YAML vs JSONL: two distinct artifacts (required)

These two file types serve completely different purposes. Never confuse them:

- **`program_aliases_review_YYYYMMDD_HHMMSS.yaml`** (output of `video_pipeline_export_program_yaml`)
  - A **human-facing hints/alias reference file** for organizing canonical titles and aliases
  - Used as **input hints to `video_pipeline_reextract`** (via `hintsYamlPath` parameter) to regenerate better extraction JSONL
  - **MUST NOT be passed to `video_pipeline_apply_reviewed_metadata`** — it is not a reviewed metadata JSONL
  - Editing this YAML does not directly update DB; it only affects subsequent reextract runs

- **`reviewed_metadata_YYYYMMDD_HHMMSS.jsonl`** (output of `video_pipeline_reextract`)
  - The actual per-file metadata records (one JSON object per line)
  - The only valid input to `video_pipeline_apply_reviewed_metadata`
  - After human review/manual edits to this JSONL, apply it to write `human_reviewed_metadata` to DB

## Tool sequence (rule-based extraction — default)

1. Call `video_pipeline_validate` with `{"checkWindowsInterop": true, "intent": "extract"}`. Follow the `nextStep` field in the result.
2. Call `video_pipeline_reextract` with:
   - `queuePath` from Stage 1 summary (`queue`)
   - optional `batchSize` / `maxBatches`
   - optional `hintsYamlPath`: path to a previously reviewed `program_aliases_review_*.yaml` if available
   - keep default `preserveHumanReviewed=true` (reviewed rows are protected from overwrite)
   - Treat output as **`machine_extracted_unreviewed_metadata`**
3. Call `video_pipeline_export_program_yaml` with:
   - `sourceJsonlPath` omitted (latest extraction output auto-detected), or explicit path
   - optional `outputPath` if user wants a fixed location
   - default is `${windowsOpsRoot}/llm/program_aliases_review_YYYYMMDD_HHMMSS.yaml`

## Tool sequence (LLM subagent extraction — useLlmExtract=true)

Use this when rule-based extraction leaves many `needs_review` entries and a high-capability model is available.

1. Call `video_pipeline_reextract` with `useLlmExtract=true` (and optionally `llmModel`, `queuePath`, `batchSize`):
   - The tool writes input JSONL batch file(s) to disk and returns immediately — **it does NOT run extraction**.
   - The result contains `followUpToolCalls` (array of tool+params objects) and an explanatory `nextStep` message.
   - `inputJsonlPaths` and `outputJsonlPaths` in the result are **informational only** — they show what file paths were prepared. **Do NOT pass these back as parameters to any tool.**

2. Execute `followUpToolCalls` in order — use the params exactly as returned, do not modify them:
   - Each `{tool: "sessions_spawn", params: {...}}` entry: call `sessions_spawn` to spawn the LLM subagent.
   - Each `{tool: "video_pipeline_apply_llm_extract_output", params: {...}}` entry: call after the corresponding subagent finishes.

3. Common mistake to avoid:
   - ❌ `video_pipeline_reextract({ inputJsonlPaths: [...] })` — `inputJsonlPaths` is an OUTPUT field, not a valid parameter. This will fail.
   - ✅ Call `sessions_spawn` with `followUpToolCalls[i].params` where `followUpToolCalls[i].tool == "sessions_spawn"`.

4. After each `video_pipeline_apply_llm_extract_output` call, check `reviewSummary.needsReviewFlagRows` in the result:
   - **`needsReviewFlagRows == 0`**: Records are in DB. **Do NOT call `video_pipeline_apply_reviewed_metadata`**. Proceed directly to `video_pipeline_relocate_existing_files` (follow the `nextStep` field).
   - **`needsReviewFlagRows > 0`**: Show `reviewCandidates` (path + columns + reasons) to the user. Then follow the correction flow:
     1. Copy `outputJsonlPath` to a new file (e.g. `reviewed_metadata_YYYYMMDD.jsonl`). The copy **must not** keep the `llm_filename_extract_output_*` name — `apply_reviewed_metadata` rejects raw extraction filenames.
     2. Ask the user to edit `program_title` / `air_date` / `needs_review` fields in the copy.
     3. After user confirms edits, call `video_pipeline_apply_reviewed_metadata` with `sourceJsonlPath` = path to the **edited copy**.
     4. **Do NOT** pass the `.yaml` file or the original `llm_filename_extract_output_*.jsonl` to `video_pipeline_apply_reviewed_metadata`.

## Recovery (LLM subagent failure or timeout)

If a sessions_spawn call fails, times out, or the subagent doesn't produce output:

1. Call `video_pipeline_llm_extract_status` to check batch completion status.
2. The tool scans for missing output JSONL files and returns `followUpToolCalls` for pending batches only.
3. Execute the returned `followUpToolCalls` in order (same pattern as initial run).
4. Repeat until all batches are complete.

---

## Continuing after extraction

4. After human review, choose the editing path (Path A is recommended for most cases):
   - **Path A — Edit the extraction JSONL directly** (immediate per-record fixes, preferred):
     - Human edits `program_title`, `air_date`, `needs_review` fields in the JSONL
     - Call `video_pipeline_apply_reviewed_metadata`:
       - `sourceJsonlPath`: the edited extraction JSONL path (**.jsonl file, NOT the .yaml file**)
       - default `markHumanReviewed=true`
       - default `allowNoContentChanges=false` (safety guard)
       - do not use `allowNoContentChanges=true` while review-risk rows remain (for example `suspiciousProgramTitleRows > 0` or `needsReviewFlagRows > 0`)
       - this step writes **`human_reviewed_metadata`** into DB (`path_metadata`)
       - if the tool reports no-content-change guard (`ok=false` with `reviewDiff.changedRowsCount == 0`), do not claim review was applied; ask for actual JSONL edits or an explicit override decision
   - **Path B — Edit the YAML hints file** (canonical title / alias rule fixes):
     - Human edits `canonical_title` / `aliases` / `rules` in the `program_aliases_review_*.yaml`
     - Propagate those edits to `rules/program_aliases.yaml` (the built-in hints file used by `reextract`)
     - Re-run `video_pipeline_reextract` (it will pick up the updated `rules/program_aliases.yaml` automatically)
     - Apply the resulting new JSONL via `video_pipeline_apply_reviewed_metadata` (Path A above)
     - **CRITICAL: Do NOT pass the `.yaml` file directly to `video_pipeline_apply_reviewed_metadata`**
       - `sourceJsonlPath` must be a `.jsonl` file (reextract output)
       - Passing a YAML file here with `allowNoContentChanges=true` is a data-corruption vector: it bypasses the raw-extraction guard and writes garbage into DB

## Human review checklist

- YAML file was generated successfully.
- Program titles/aliases in YAML are acceptable.
- Rows with `needs_review` are either fixed or intentionally kept for later.
- **The agent calls `video_pipeline_apply_reviewed_metadata`** after the user confirms the JSONL edits are done.
  - The user's role is editing the JSONL (program_title, air_date, needs_review fields). Calling the tool is the agent's responsibility.
  - `sourceJsonlPath` must be a `.jsonl` file (reextract output). Never pass a `.yaml` file here.
- Confirm `video_pipeline_apply_reviewed_metadata` actually applied reviewed edits:
  - do not treat it as successful review apply if the tool reports "raw extraction output" or "no content edits detected"
  - do not use `allowNoContentChanges=true` as a shortcut when suspicious titles or `needs_review` rows are still present

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

- Present to the user:
  - extraction result summary
  - YAML output path
  - count of programs exported
  - concrete review candidates (path + columns + reasons) when available
- Ask the user to review/edit the JSONL if needed. Once the user confirms edits are done (or says no edits needed), the agent calls `video_pipeline_apply_reviewed_metadata` immediately — do not wait for additional permission.
- After apply succeeds, ask user whether to proceed to Stage 3 (Move + Human review).
