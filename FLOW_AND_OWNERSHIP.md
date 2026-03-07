# video-library-pipeline フローと責務

このドキュメントはプラグインの実行順序・レイヤー境界・責務の分担を定義する。
ドキュメント体系と情報源マッピングは `docs/CURRENT_SPEC_INDEX.md` から参照すること。

## 1) レイヤー責務

- **TypeScript ツール層** (`src/*.ts`)
  - プラグイン設定を検証（fail-fast）
  - 実行時設定を1つの正規形に解決
  - 明示的な引数で Python ランナーを起動
- **Python オーケストレーション層** (`py/*.py`)
  - パイプラインの各ステージをエンドツーエンドで実行
  - DB のアップサート・照合とアーティファクト生成を担当
  - 実行サマリーを出力
- **Windows PowerShell 層** (`<windowsOpsRoot>/scripts/*.ps1`)
  - Windows ファイルシステムの変更・列挙を担当
  - 移動・インベントリの生のエビデンスを出力
  - 内部で長パス (`\\?\`) を処理し、外部には通常の Windows パスを返す

## 2) ランタイムコントラクト

必須プラグイン設定:

- `windowsOpsRoot`
- `sourceRoot`
- `destRoot`

任意プラグイン設定:

- `db`（デフォルト: `<windowsOpsRoot>/db/mediaops.sqlite`）
- `defaultMaxFilesPerRun`
- `tsRoot` — TS録画ディレクトリ（例: `J:\TVFile`）、`video_pipeline_ingest_epg` で使用
- `driveRoutesPath` — ジャンル別ルーティングYAML、省略時はプラグインルートの `rules/drive_routes.yaml`

`windowsOpsRoot` 配下のランタイムパス:

- `db`, `move`, `llm`, `scripts`
- `scripts` は validate/run 時に必要な PS1 ファイルが欠損していれば自動プロビジョニングされる
- プラグイン管理スクリプトは validate/run/backfill/relocate/dedup 実行時に自動同期（欠損または古いファイルは更新される）
- ユーザーが追加したカスタムサブスクリプトはプラグイン管理外であり上書きされない
- 長パス操作には Windows の `LongPathsEnabled=1` と PowerShell 7 が必要

プラグインローカル設定ファイル群:

- `<plugin-root>/rules/backfill_roots.yaml`
  - `video_pipeline_backfill_moved_files` 向けのオプションルート/拡張子リスト
  - ツールパラメータ指定時はそちらが優先される
- `<plugin-root>/rules/relocate_roots.yaml`
  - `video_pipeline_relocate_existing_files` 向けのオプションルート/拡張子リスト
  - 安全のため、relocate には明示的な `roots` または `rootsFilePath` が依然として必要
- `<plugin-root>/rules/broadcast_buckets.yaml`
  - 放送バケット分類キーワード（`terrestrial` / `bs_cs` / `unknown`）
  - `video_pipeline_dedup_recordings` が使用
- `<plugin-root>/rules/drive_routes.yaml`
  - ジャンル → 移動先ドライブ/レイアウトのルーティングルール
  - `video_pipeline_analyze_and_move_videos` が `make_move_plan_from_inventory.py` 経由で使用
  - `driveRoutesPath` プラグイン設定で上書き可能

## 3) メタデータ抽出ポリシー（AI主導）

有効アーキテクチャ: `A_AI_PRIMARY_WITH_GUARDRAILS`

- 主経路: `run_metadata_batches_promptv1.py` の AI 主導パースフロー
- オプションのガードレール入力: `<plugin-root>/rules/program_aliases.yaml`
  - フォーマット: 人間が書く YAML ヒント（`canonical_title`, `aliases`）
  - `id` なし、正規表現ルールエンジンなし
- YAML ヒントが欠損していても致命的ではない:
  - 抽出は AI のみのモードで継続
  - 不明・曖昧な行は `needs_review=true` として表面化される

ユーザー修正ループ:

- 人間によるレビューがヒント辞書（`hints` / `user_learned`）を更新する
- 次回実行時に更新済みヒントを読み込み、正規タイトル正規化が改善される

## 4) 順序付き処理フロー

1. プラグイン設定を行う
2. **EPG取り込みステージ**（推奨: program.txt 削除前）:
   - `video_pipeline_ingest_epg`（dry-run/apply）を実行
   - `tsRoot` 配下の `.program.txt` をスキャン・パースし、放送局/ジャンル/説明を取得
   - `programs` / `broadcasts` テーブルに `match_key` / `datetime_key` キーで保存
   - 取り込まれた EPG データは後続のメタデータ抽出で自動的に活用される
3. **バックフィルステージ**（任意）:
   - `video_pipeline_backfill_moved_files`（dry-run/apply）を実行
   - ルートをスキャンして `paths/observations/events` を照合
   - reextract フロー向けのメタデータキュー生成（任意）
4. **重複削除ステージ**（任意）:
   - `video_pipeline_dedup_recordings`（dry-run/apply）を実行
   - メタデータキーと任意の放送バケット分割で重複候補を分類
5. **再配置ステージ**（任意）:
   - `video_pipeline_relocate_existing_files`（dry-run/apply）を実行
   - DB メタデータと現在の配置ルールに基づき既存ファイルを物理移動
6. `video_pipeline_analyze_and_move_videos` を実行
7. `src/tool-run.ts` が起動:
   - `uv run python py/unwatched_pipeline_runner.py --db ... --source-root ... --dest-root ... --windows-ops-root ... --max-files-per-run ... --drive-routes ... [--apply] [--allow-needs-review]`
8. ランナーが `db/move/llm` を準備する
9. ランナーが PowerShell 経由でファイル名を正規化し、インベントリをスナップショット
10. ランナーがインベントリを取り込み、メタデータキューを構築
11. ランナーがオプションの YAML ヒントと EPG エンリッチメント（放送局・ジャンル）を用いて抽出を実行
12. ランナーが `drive_routes.yaml` によるジャンル別マルチドライブルーティングで移動計画を構築
13. ランナーが移動アクションを適用（または dry-run）
14. ランナーが DB パスを照合し、残余レポートを書き込み、古いアーティファクトをローテート
15. ランナーが最終 JSON サマリーを出力（`plan_stats.genre_route_counts` を含む）
16. **再放送検出ステージ**（任意）:
    - `video_pipeline_detect_rebroadcasts`（dry-run/apply）を実行
    - `air_date` / `broadcaster` の差異で同一エピソードの録画をグルーピング
    - `broadcast_groups` / `broadcast_group_members` を DB に書き込む（ファイル移動なし）

## 5) 責務マップ

- 設定の情報源: プラグイン設定（`plugins.entries.video-library-pipeline.config`）
- ヒントの情報源: `<plugin-root>/rules/program_aliases.yaml`
- バックフィルルートの情報源: `<plugin-root>/rules/backfill_roots.yaml`
- 再配置ルートの情報源: `<plugin-root>/rules/relocate_roots.yaml`
- 重複削除放送ルールの情報源: `<plugin-root>/rules/broadcast_buckets.yaml`
- マルチドライブルーティングの情報源: `<plugin-root>/rules/drive_routes.yaml`
- バックフィルルートの情報源: `<plugin-root>/rules/backfill_roots.yaml`
- スクリプトテンプレートの情報源:
  - 優先: `<windowsOpsRoot>/templates/windows-scripts/*.ps1`
  - フォールバック: `<plugin-root>/assets/windows-scripts/*.ps1`
- DB 状態: `<windowsOpsRoot>/db/mediaops.sqlite`
- 生エビデンス: `<windowsOpsRoot>/move/*.jsonl`, `<windowsOpsRoot>/llm/*.jsonl`

## 6) 境界ルール

- TypeScript や Python から Windows ファイルシステムの変更を直接実装しない
- 長パス対応のファイルアクセスには WSL ファイル I/O より pwsh7 ヘルパーを優先する
- TS→Python の引数名はランナーの CLI 引数と完全に一致させる
- ヒントはオプションの補助入力として扱い、主たる抽出エンジンにしない

## data_json v2
- Added `genre`, `franchise`, `source_history` fields to metadata payloads.
- Migration script: `python py/migrate_data_json_v2.py --db <mediaops.sqlite> --dry-run` then rerun without `--dry-run`.
