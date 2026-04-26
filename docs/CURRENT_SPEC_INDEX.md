# video-library-pipeline 現行仕様インデックス

このファイルはプラグインの現在の動作を把握するためのドキュメントエントリポイント。

## 1) 推奨読み順

1. `skills/video-library-pipeline/SKILL.md`
   - V2 の AI エージェント実行ルール、インテントマッピング、tool 使用ガードレール
2. `docs/adr/0008-v2-workflow-kernel-and-run-based-tool-surface.md`
   - V2 run-based workflow kernel と公開 tool surface
3. `docs/adr/README.md`
   - ADR 一覧と legacy/current の位置づけ
4. `index.ts`, `src/tools/tool-workflows.ts`, `src/workflows/workflow-result-translator.ts`
   - 現在の tool 登録、V2 adapter、WorkflowResult 翻訳
5. `py/workflow_cli.py`, `py/video_pipeline/workflows/**/*.py`
   - Python workflow service、run manifest、artifact/review gate 実装
6. `DEPENDENCIES.md`, `docs/adr/0003-windows-powershell-filesystem-boundary.md`
   - ランタイム前提、PowerShell 境界、Windows filesystem safety

## 2) 現行 V2 Public Tool Surface

現在の OpenClaw public tools は次の4つ。

- `video_pipeline_start`
- `video_pipeline_resume`
- `video_pipeline_status`
- `video_pipeline_inspect_artifact`

旧 V1 の多数の direct tools は public surface から隠れている。active docs/skills はそれらを通常操作として案内しない。

## 3) 問い別・情報源マッピング

### 「このリクエストにはどのツールを使うべきか？」

- 第一: `skills/video-library-pipeline/SKILL.md`
- 第二: `index.ts`, `src/tools/tool-workflows.ts`

### 「この run の次に何をするべきか？」

- 第一: tool result の `WorkflowResult.nextActions` と `followUpToolCalls`
- 第二: `video_pipeline_status {"runId":"...", "includeArtifacts":true}`
- 第三: `skills/*/SKILL.md`

自由文の `nextStep` は補助説明であり、状態遷移や次 tool call の source of truth ではない。

### 「レビュー対象は何か？」

- 第一: `ReviewGate.artifactIds`
- 第二: `video_pipeline_inspect_artifact {"runId":"...", "artifactId":"..."}`
- 第三: `skills/extract-review/SKILL.md` と `skills/move-review/SKILL.md`

### 「artifact の中身や checksum を確認したい」

- 第一: `video_pipeline_inspect_artifact`
- 実装: `py/workflow_cli.py`, `py/video_pipeline/workflows/store.py`

### 「V2 の architecture source of truth は？」

- 第一: `docs/adr/0008-v2-workflow-kernel-and-run-based-tool-surface.md`
- 実装確認: `src/tools/tool-workflows.ts`, `py/video_pipeline/workflows/`

## 4) 現行操作ドキュメント

- `skills/video-library-pipeline/SKILL.md`
  - V2 トップレベルオーケストレータ
- `skills/inventory-review/SKILL.md`
  - sourceRoot run start と metadata/review/plan handoff
- `skills/extract-review/SKILL.md`
  - run-scoped metadata `ReviewGate` handling
- `skills/move-review/SKILL.md`
  - run-scoped plan artifact review と `video_pipeline_resume`
- `skills/relocate-review/SKILL.md`
  - existing-library relocate workflow
- `skills/folder-cleanup/SKILL.md`
  - V2 では standalone cleanup が未公開であることの migration note

## 5) Legacy / Historical Documents

以下は背景として有用だが、V2 public surface と一致しない手順を含む。

- `docs/adr/0002-pipeline-architecture-and-review-gates.md`
  - V1 stage/tool 名を含む historical flow
- `docs/adr/0004-tool-orchestration-and-follow-up-calls.md`
  - V1 direct tool orchestration と hidden legacy tool 詳細
- `docs/adr/0005-metadata-and-artifact-lifecycle.md`
  - V1 JSONL/YAML lifecycle の一部を含む
- `FLOW_AND_OWNERSHIP.md`
- `BACKFILL_MOVED_FILES_REQUIREMENTS.md`
- `MULTIDRIVE_EPG_REQUIREMENTS.md`
- `DUPLICATE_DEDUP_REQUIREMENTS.md`

これらのファイル内に旧 tool 名があっても、V2 の active operator instruction として扱わない。

## 6) ドキュメント間で矛盾がある場合の優先順位

1. `index.ts` と `src/tools/tool-workflows.ts`
2. `py/workflow_cli.py` と `py/video_pipeline/workflows/**/*.py`
3. `skills/video-library-pipeline/SKILL.md`
4. `docs/adr/0008-v2-workflow-kernel-and-run-based-tool-surface.md`
5. legacy ADR / historical requirement docs

## 7) メンテナンスルール

- Public tool surface が変わったら `skills/video-library-pipeline/SKILL.md` とこの file を更新する。
- WorkflowResult fields が変わったら `src/workflows/workflow-result-translator.ts` と skills の `nextActions` / `followUpToolCalls` 記述を更新する。
- 新しい workflow を公開したら、対応する sub-skill を V2 runId/artifact/review gate 中心に更新する。
- 旧 direct tool を再公開しない限り、active docs でそれを通常手順として案内しない。
