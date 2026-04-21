# video-library-pipeline 依存関係

このドキュメントはプラグインのランタイム依存関係と安全な実行に必要な最小プリフライト条件を定義する。
ドキュメント体系と情報源マッピングは `docs/CURRENT_SPEC_INDEX.md` から参照すること。
ステージ順序と責務境界については `FLOW_AND_OWNERSHIP.md` を参照すること。

## スコープ

- プラグインパッケージ: `extensions/video-library-pipeline`
- ツールエントリポイント: `src/tools/tool-run.ts`, `src/tools/tool-backfill.ts`, `src/tools/tool-relocate.ts`, `src/tools/tool-dedup.ts`, `src/tools/tool-status.ts`, `src/tools/tool-validate.ts`, `src/tools/tool-reextract.ts`, `src/tools/tool-ingest-epg.ts`, `src/tools/tool-detect-rebroadcasts.ts`
- Python オーケストレーション: `py/unwatched_pipeline_runner.py`, `py/backfill_moved_files.py`, `py/relocate_existing_files.py`, `py/dedup_recordings.py`, `py/run_metadata_batches_promptv1.py`, `py/ingest_program_txt.py`, `py/detect_rebroadcasts.py`
- Python 共有ロジック: `py/video_pipeline/domain/*.py`, `py/video_pipeline/db/*.py`, `py/video_pipeline/platform/*.py`
- Windows 操作: `<windowsOpsRoot>/scripts` 配下のスクリプト

## プラグイン設定コントラクト

必須設定キー:

- `windowsOpsRoot`
- `sourceRoot`
- `destRoot`

任意設定キー:

- `db`（デフォルト: `<windowsOpsRoot>/db/mediaops.sqlite`）
- `defaultMaxFilesPerRun`（デフォルト: `200`）
- `tsRoot`（デフォルト: なし）— EPG取り込みで使用するTS録画ディレクトリ（例: `J:\TVFile`）
- `driveRoutesPath`（デフォルト: プラグインルートの `rules/drive_routes.yaml`）— ジャンル別ルーティング設定

`windowsOpsRoot` 配下の必要ディレクトリ:

- `db`
- `move`
- `llm`
- `scripts`

補足:

- ヒントファイル `rules/program_aliases.yaml` は任意。欠損時は AI のみのモードで抽出を継続する
- バックフィルルートファイル `rules/backfill_roots.yaml` は backfill ツールへのオプション入力。未設定時は `destRoot` を使用
- 再配置ルートファイル `rules/relocate_roots.yaml` は relocate ツールへのオプション入力。安全のため、relocate には依然として明示的な `roots` または `rootsFilePath` が必要
- 放送バケットルール `rules/broadcast_buckets.yaml` は dedup ツールが使用（terrestrial / bs_cs / unknown 分類）
- ドライブルーティングルール `rules/drive_routes.yaml` は移動計画時にジャンル別デスト振り分けで使用
- `db/move/llm` はランナーが欠損時に自動作成する
- `scripts` は実行時に必要だが、ディレクトリ/ファイルの欠損は `video_pipeline_validate` および `video_pipeline_analyze_and_move_videos` 実行時にプラグインテンプレートから自動プロビジョニングされる
- `<windowsOpsRoot>/scripts` 配下のプラグイン管理スクリプトは validate/run/backfill/relocate/dedup 実行時にテンプレートから自動同期される（欠損または古いファイルは更新される）
- ユーザーカスタムスクリプトはプラグイン管理外であり上書きされない

## 必須バイナリ

- `uv`
- `python`（`uv run python ...` として起動）
- `pwsh` または `pwsh.exe`（PowerShell 7）
- Windows 長パスサポート有効（`LongPathsEnabled=1`）

## Python ランタイム依存関係

- `pyyaml` — `drive_routes.yaml` ロードに必要（`py/video_pipeline/domain/path_placement_rules.py`）。標準ライブラリ外では唯一の必須依存
- Python 標準ライブラリ（`sqlite3`, `argparse`, `json`, `unicodedata` 等）
- DB アクセスは標準 `sqlite3` のみ（外部 ORM 依存なし）

## 必須 Windows スクリプト

`<windowsOpsRoot>/scripts` 配下:

- `unwatched_inventory.ps1`
- `apply_move_plan.ps1`

プラグイン内部ヘルパースクリプト（自動管理）:

- `_long_path_utils.ps1`
- `enumerate_files_jsonl.ps1`

テンプレートの情報源:

- 優先（プラグイン同梱の canonical source）: `<plugin-root>/templates/windows-scripts/*.ps1`
- フォールバック（移行期間中の legacy source）: `<plugin-root>/assets/windows-scripts/*.ps1`

## 最小プリフライト

**1) 設定確認**

```
openclaw gateway call video-library-pipeline.status --json
openclaw video-pipeline-status
```

**2) バイナリ確認**

```
uv --version
pwsh -NoProfile -Command "$PSVersionTable.PSVersion.ToString()"
reg query "HKLM\SYSTEM\CurrentControlSet\Control\FileSystem" /v LongPathsEnabled
# → 0x1 であること
```

**3) dry-run（1ファイル）**

```
uv run python "<plugin-dir>/py/unwatched_pipeline_runner.py" \
  --windows-ops-root "<windows-ops-root>" \
  --source-root "<source-root>" \
  --dest-root "<dest-root>" \
  --max-files-per-run 1
```

**4) apply（1ファイル）**

```
uv run python "<plugin-dir>/py/unwatched_pipeline_runner.py" \
  --windows-ops-root "<windows-ops-root>" \
  --source-root "<source-root>" \
  --dest-root "<dest-root>" \
  --max-files-per-run 1 --apply
```

長パスに関する補足:

- このプラグインは元のファイル名を保持し、パスを短縮しない
- 長パス処理はプラグイン管理の PowerShell スクリプトおよび Windows 側のフォールバック列挙で実装されている
- DB / JSONL / ログは通常の Windows パス（`C:\\...`）を保持し、`\\?\` パスは使用しない

## 操作上の出力先

| 種別 | パス |
|------|------|
| DB 状態 | `<windowsOpsRoot>/db/mediaops.sqlite` |
| 移動・監査アーティファクト | `<windowsOpsRoot>/move/` |
| 抽出アーティファクト | `<windowsOpsRoot>/llm/` |
| ヒント辞書 | `<plugin-root>/rules/program_aliases.yaml` |
| バックフィルルート設定 | `<plugin-root>/rules/backfill_roots.yaml` |
| 再配置ルート設定 | `<plugin-root>/rules/relocate_roots.yaml` |
| 放送バケットルール | `<plugin-root>/rules/broadcast_buckets.yaml` |
| ドライブルーティングルール | `<plugin-root>/rules/drive_routes.yaml` |
| バックフィルアーティファクト | `<windowsOpsRoot>/move/backfill_plan_*.jsonl` / `backfill_apply_*.jsonl` |
| バックフィルキュー | `<windowsOpsRoot>/llm/backfill_metadata_queue_*.jsonl` |
| 再配置アーティファクト | `<windowsOpsRoot>/move/relocate_plan_*.jsonl` / `relocate_apply_*.jsonl` |
| 再配置キュー | `<windowsOpsRoot>/llm/relocate_metadata_queue_*.jsonl` |
| 重複削除アーティファクト | `<windowsOpsRoot>/move/dedup_plan_*.jsonl` / `dedup_apply_*.jsonl` |
| 隔離先 | `<windowsOpsRoot>/duplicates/quarantine/` |
