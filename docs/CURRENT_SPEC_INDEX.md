# video-library-pipeline 現行仕様インデックス

このファイルはプラグインの現在の動作を把握するための**ドキュメントエントリポイント**。

以下の問いに答えが必要なときは最初にこのドキュメントを参照すること:
- 「このプラグインは今何をするべきか？」
- 「この問いに対する情報源はどのファイルか？」
- 「この要件書は現行仕様か、それとも歴史的記録か？」

## 1) 推奨読み順

1. `skills/video-library-pipeline/SKILL.md`
   - AIエージェントの実行ルール、インテントマッピング、ツール使用ガードレール
2. `docs/adr/README.md`
   - 設計判断の一覧。READMEから移した詳細仕様への入口
3. `FLOW_AND_OWNERSHIP.md`
   - 現在の実行順序、レイヤー境界、責務分担
4. `DEPENDENCIES.md`
   - ランタイム前提条件、バイナリ、スクリプト、プリフライト
5. `index.ts`, `src/tools/*.ts`, `src/contracts/types.ts`
   - 現在のツール登録位置と TypeScript 側の共通コントラクト
6. `docs/adr/0007-openclaw-sdk-recommended-plugin-implementation.md`
   - OpenClaw SDK公式docsに基づくplugin実装規約
7. Python/TS 実装ファイル（`src/tools/*.ts`, `src/platform/*.ts`, `py/*.py`, `py/video_pipeline/**/*.py`）
   - 実際の動作詳細とエッジケース処理

## 2) 問い別・情報源マッピング

### 「このリクエストにはどのツールを使うべきか？」
- 第一: `skills/video-library-pipeline/SKILL.md`
- 第二: `index.ts`, `src/tools/*.ts`

### 「このツールの意味・結果のスコープは？」
- 第一: `skills/video-library-pipeline/SKILL.md`（ツールスコープ/結果セマンティクス）
- 第二: `index.ts`, `src/contracts/types.ts`
- 実装詳細: `src/tools/*.ts`, `py/*.py`, `py/video_pipeline/**/*.py`

### 「現在の実行フローは？」
- 第一: `FLOW_AND_OWNERSHIP.md`
- 第二: `docs/adr/0002-pipeline-architecture-and-review-gates.md`
- 第三: `skills/video-library-pipeline/SKILL.md`（エージェント向けのウィニングフロー）

### 「必要なバイナリ/設定/スクリプトは？」
- 第一: `DEPENDENCIES.md`
- 第二: `docs/adr/0003-windows-powershell-filesystem-boundary.md`
- 第三: `src/tools/tool-validate.ts`, `src/platform/windows-scripts-bootstrap.ts`

### 「DB更新/移動照合で具体的に何が起きるか？」
- 主要実装:
  - `py/update_db_paths_from_move_apply.py`
  - `py/relocate_existing_files.py`
  - `py/backfill_moved_files.py`
- 補足ドキュメント:
  - `docs/adr/0005-metadata-and-artifact-lifecycle.md`
  - `docs/adr/0006-mediaops-db-routing-and-safety.md`
  - `FLOW_AND_OWNERSHIP.md`
  - `DEPENDENCIES.md`

### 「AIエージェントはどの内容を人間にレビューさせるべきか？」
- 第一: `skills/extract-review/SKILL.md`
- ツール結果の形: `src/tools/tool-export-program-yaml.ts`

### 「OpenClaw SDKとして推奨される実装方法は？」
- 第一: `docs/adr/0007-openclaw-sdk-recommended-plugin-implementation.md`
- 第二: `docs/adr/0001-plugin-sdk-testing.md`（テスト方針）
- 実装確認: `index.ts`, `openclaw.plugin.json`, `package.json`

## 3) 現行操作ドキュメント（稼働中の動作）

- `skills/video-library-pipeline/SKILL.md`
  - トップレベルオーケストレータスキル
  - インテントマッピング（自然言語 → ツールフロー）
  - エージェントガードレール（ツール/スキル混同禁止、シェルフォールバック禁止）
- `docs/adr/0001-plugin-sdk-testing.md`
  - OpenClaw SDK testing に沿ったプラグインテスト方針
  - Vitest と pytest の責務分担、runtime mock、import discipline
- `docs/adr/0002-pipeline-architecture-and-review-gates.md`
  - sourceRoot / relocate のパイプライン構成
  - Stage 1-3 とヒューマンレビューゲート
- `docs/adr/0003-windows-powershell-filesystem-boundary.md`
  - WSL2/Windows境界、PowerShell委譲、長パス・Unicode前提
- `docs/adr/0004-tool-orchestration-and-follow-up-calls.md`
  - ツール詳細、`followUpToolCalls`、LLMサブエージェントフロー
- `docs/adr/0005-metadata-and-artifact-lifecycle.md`
  - JSONL/YAMLアーティファクト、`windowsOpsRoot`、source遷移
- `docs/adr/0006-mediaops-db-routing-and-safety.md`
  - `mediaops.sqlite`、ジャンル別ドライブルーティング、安全機構
- `docs/adr/0007-openclaw-sdk-recommended-plugin-implementation.md`
  - OpenClaw SDK公式docsに基づくentry point、manifest、runtime、setup、agent harness境界
- `skills/inventory-review/SKILL.md`
  - sourceRoot パイプライン ステージ1（棚卸 + キュー生成 + レビューゲート）
- `skills/extract-review/SKILL.md`
  - メタデータレビューステージ
  - YAML は人間専用アーティファクト。エージェントは構造化フィールドを使うこと
- `skills/move-review/SKILL.md`
  - 移動/apply ステージのレビューゲート
- `FLOW_AND_OWNERSHIP.md`
  - ランタイム境界と順序付き処理フロー
- `DEPENDENCIES.md`
  - 現在のランタイム前提条件、長パス前提、プリフライト

## 4) 歴史的/計画ドキュメント（デフォルトでは現行動作ではない）

設計意図や経緯の把握には有用だが、現在の実装とは一致しない場合がある。

- `BACKFILL_MOVED_FILES_REQUIREMENTS.md`
  - バックフィル機能の歴史的要件・設計メモ
- `DUPLICATE_DEDUP_REQUIREMENTS.md`
  - 重複削除機能の歴史的要件・設計メモ

ルール:
- 以下で検証されるまで、これらのファイルを現行動作仕様として扱わないこと:
  - `skills/video-library-pipeline/SKILL.md`
  - `FLOW_AND_OWNERSHIP.md`
  - `DEPENDENCIES.md`
  - `index.ts`, `src/tools/*.ts`, `src/contracts/types.ts`

## 4b) 現行機能要件ドキュメント（稼働中の動作）

2026年2月に実装した機能の設計・仕様書:

- `MULTIDRIVE_EPG_REQUIREMENTS.md`
  - EPG早期取り込み（`video_pipeline_ingest_epg`）
  - マルチドライブジャンル別ルーティング（`drive_routes.yaml`）
  - 再放送グルーピング（`video_pipeline_detect_rebroadcasts`）

## 5) ドキュメント間で矛盾がある場合のコードレベル情報源

1. `index.ts`（現在登録されているツール一覧）
2. `src/tools/*.ts`（ツールラッパーの動作と返却フィールド）
3. `py/*.py`, `py/video_pipeline/**/*.py`（コア実行ロジック）
4. `templates/windows-scripts/*.ps1`（Windows FS 動作の canonical template）

ランタイムインシデントのトリアージには以下を優先:
- ツールの JSON 結果
- `<windowsOpsRoot>/move` および `<windowsOpsRoot>/llm` 配下の監査 JSONL アーティファクト
- `<windowsOpsRoot>/db/mediaops.sqlite` の DB 状態

## 6) 将来の編集に向けたメンテナンスルール

プラグインの動作を変更する際:

- コードを先に更新する
- 次に以下を更新する:
  - `index.ts`, `src/tools/*.ts`, `src/contracts/types.ts`（ツール登録や共通コントラクトが変わった場合）
  - `skills/video-library-pipeline/SKILL.md`（エージェントの動作や解釈が変わった場合）
  - `FLOW_AND_OWNERSHIP.md` / `DEPENDENCIES.md`（フロー/依存関係が変わった場合）
  - `docs/adr/*.md`（設計判断、フロー、安全機構、DB契約が変わった場合）
- 要件書が古くなった場合は:
  - `Status / Source of Truth` 注記を追加・更新するか
  - 明示的に歴史的セクションとしてマークする

## 7) ドキュメントが現行かどうかの確認チェックリスト

ドキュメントを現行仕様と判断する前に確認:

- `index.ts` と `src/tools/*.ts` に存在するツールが記述されているか？
- 現在のツール名（`video_pipeline_*`）と一致しているか？
- 既存ルートのクリーンアップについて `prepare_relocate_metadata` / `relocate` が言及されているか？
- 以下を区別しているか？:
  - DB同期（`backfill`）
  - メタデータ準備（`prepare_relocate_metadata` / `reextract`）
  - 物理再配置（`relocate`）
- 現在の長パス前提（`pwsh7`, `LongPathsEnabled=1`）と一致しているか？

1つでも「いいえ」なら、コードで検証されるまで歴史的ドキュメントとして扱うこと。
