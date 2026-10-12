# video-library-pipeline 現行仕様インデックス

このファイルはプラグインの現在の動作を把握するためのドキュメントエントリポイント。

2026-10-10の見直しは[設計監査](reviews/2026-10-10-design-audit.md)、
[ADR-0011](adr/0011-unified-application-kernel-and-recoverable-operations.md)、
[移行計画](reviews/2026-10-10-redesign-roadmap.md)を参照。
これらは次期設計の提案であり、以下の現行tool/操作契約を変更しない。

M0の実装済み修正は [ADR-0012](adr/0012-m0-integrity-boundaries.md) と
[実装・検証報告](reviews/2026-10-10-m0-implementation.md)を参照。
metadata反映はPythonのrun/gate検証後に1 transactionで行い、原本をrenameしない。
公開tool名は4つのまま。後段M1-M5は未実装。

v0.3.0の確定的CLIとファイル同一性は [ADR 0009](adr/0009-deterministic-execution-and-file-identity.md) を参照。公開ツール契約はV2のまま維持する。

作業データの保存先・cache整理は [ADR 0010](adr/0010-managed-artifact-retention.md) と
`openclaw video-pipeline-artifacts --help` を参照。旧データはread-onlyで集計し、
管理された完了cacheのみ可逆に退避する。監査記録のTTL永久削除は行わない。

録画の細かい隔離ディレクトリを確認する時は、
`openclaw video-pipeline-quarantine-catalog --help` と READMEの「細かい隔離フォルダを
開かずに確認する」を参照。既存の検証済みhandoffを全ドライブ横断の検索可能な
一覧へ表示し、元の配置・exact-run復元・DB・V2 tool契約は変更しない。

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

### 「ジャンル別配置先へ作品を集約する案は？」

- `video_pipeline_start {"flow":"relocate", "programTitles":["<正式作品名>"]}`。roots省略時は設定済み全メディア配置先を検索する。
- `relocate_routing_inventory` artifactが作品別の所在・容量・配置先・判定理由、`relocate_plan`が移動候補と保留理由を保持する。
- 実装: `py/video_pipeline/domain/routing_inventory.py`、`py/relocate_existing_files.py`。
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
