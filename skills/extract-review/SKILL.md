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

## YAML→DB 反映フロー (primary path)

`video_pipeline_export_program_yaml` が生成する YAML は **人間のレビュー・編集用アーティファクト** であると同時に、`video_pipeline_apply_reviewed_metadata` の `sourceYamlPath` パラメータ経由で **DB に直接反映できる**。

### 仕組み

1. YAML 内の `source_jsonl` フィールドが元の抽出結果 JSONL を指す
2. `apply_reviewed_metadata` は YAML の `canonical_title` / `aliases` マッピングを読み取り、JSONL の各行に適用する
3. エイリアスにマッチした行は `program_title` が `canonical_title` に書き換わる
4. タイトル修正により `needs_review` の理由がなくなった行は自動的に `needs_review=false` にクリアされる
5. 修正済み行を `source='human_reviewed'` で DB に upsert する
6. upsert 成功後、YAML・抽出出力 JSONL・入力 JSONL を `llm/archive/` に自動アーカイブする

### YAML 編集で解決できること

- 番組名の正規化 (`canonical_title` 設定)
- 同一番組の表記揺れ統合 (`aliases` に追加)
- サブタイトル混入の修正 (例: `"もぎたて!▽天気"` → `canonical_title: "もぎたて!"` + alias に元の文字列を追加)

### YAML 編集で解決できないこと

- `air_date` の修正 (YAML にはタイトル情報のみ)
- 個別レコードの `needs_review` 手動制御 (タイトル起因以外の理由)
- 上記が必要な場合は JSONL 直接編集 (後述の Legacy path) を使用する

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
   - `video_pipeline_reextract({ inputJsonlPaths: [...] })` — `inputJsonlPaths` is an OUTPUT field, not a valid parameter. This will fail.
   - Call `sessions_spawn` with `followUpToolCalls[i].params` where `followUpToolCalls[i].tool == "sessions_spawn"`.

4. After each `video_pipeline_apply_llm_extract_output` call, check `reviewSummary.needsReviewFlagRows` in the result:
   - **`needsReviewFlagRows == 0`**: Records are in DB. **Do NOT call `video_pipeline_apply_reviewed_metadata`**. Proceed directly to `video_pipeline_relocate_existing_files` (follow the `nextStep` field).
   - **`needsReviewFlagRows > 0`**: Show `reviewCandidates` (path + columns + reasons) to the user. Then call `video_pipeline_export_program_yaml` to generate a review YAML, and follow the standard YAML review flow below.

## Recovery (LLM subagent failure or timeout)

If a sessions_spawn call fails, times out, or the subagent doesn't produce output:

1. Call `video_pipeline_llm_extract_status` to check batch completion status.
2. The tool scans for missing output JSONL files and returns `followUpToolCalls` for pending batches only.
3. Execute the returned `followUpToolCalls` in order (same pattern as initial run).
4. Repeat until all batches are complete.

---

## Continuing after extraction

4. After human review, choose the editing path:

   - **Path A — YAML 編集 → DB 反映 (推奨)**:
     - 人間が `program_aliases_review_*.yaml` の `canonical_title` / `aliases` を編集
     - 編集完了の確認後、`video_pipeline_apply_reviewed_metadata` を呼び出す:
       - `sourceYamlPath`: 編集済み YAML のパス
       - default `markHumanReviewed=true`
     - ツールが YAML のエイリアスマッピングを元の抽出結果 JSONL に適用し、DB を更新する
     - タイトル変更により review reason が解消された行は自動的に `needs_review=false` になる

   - **Path B — JSONL 直接編集 (レガシー / air_date 修正等)**:
     - `air_date` の修正やタイトル以外の理由による `needs_review` クリアなど、YAML では対応できない修正が必要な場合に使用
     - 人間が抽出結果 JSONL の `program_title`, `air_date`, `needs_review` フィールドを直接編集
     - Call `video_pipeline_apply_reviewed_metadata`:
       - `sourceJsonlPath`: the edited extraction JSONL path
       - default `markHumanReviewed=true`
       - default `allowNoContentChanges=false` (safety guard)
       - do not use `allowNoContentChanges=true` while review-risk rows remain (for example `suspiciousProgramTitleRows > 0` or `needsReviewFlagRows > 0`)
       - this step writes **`human_reviewed_metadata`** into DB (`path_metadata`)
       - if the tool reports no-content-change guard (`ok=false` with `reviewDiff.changedRowsCount == 0`), do not claim review was applied; ask for actual JSONL edits or an explicit override decision

## Human review checklist

- YAML file was generated successfully.
- Program titles/aliases in YAML are acceptable.
- Rows with `needs_review` are either fixed or intentionally kept for later.
- **The agent calls `video_pipeline_apply_reviewed_metadata`** after the user confirms the YAML edits are done.
  - YAML path: use `sourceYamlPath` parameter (recommended)
  - JSONL path: use `sourceJsonlPath` parameter (legacy, for air_date fixes etc.)
- Confirm `video_pipeline_apply_reviewed_metadata` actually applied reviewed edits:
  - do not treat it as successful review apply if the tool reports "no content edits detected"
  - YAML flow: if `yamlReviewApplied.changedRowsCount == 0`, the YAML aliases did not match any rows — check that aliases are correctly spelled

## Review reporting rule (required)

- When asking the human to review extraction results, do **not** give only generic themes (for example, "title consistency" or "YAML structure").
- Prefer the structured fields from `video_pipeline_export_program_yaml` tool result when available:
  - `reviewSummary`
  - `reviewCandidates`
  - `reviewGuidance`
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

## Handoff

- Present to the user:
  - extraction result summary
  - YAML output path
  - count of programs exported
  - concrete review candidates (path + columns + reasons) when available
- Ask the user to review/edit the YAML if needed. Once the user confirms edits are done (or says no edits needed), the agent calls `video_pipeline_apply_reviewed_metadata` with `sourceYamlPath` immediately — do not wait for additional permission.
- After apply succeeds, ask user whether to proceed to Stage 3 (Move + Human review).
