# video-library-pipeline 重複削除機能 要件定義

## Current Implementation Status (2026-02-25) [CURRENT]

This document is kept as a **requirements + design-history** record, and the dedup feature is already implemented.
To avoid confusion, this section summarizes the current behavior before the historical requirement text below.

Start here for document roles and source-of-truth mapping:
- `docs/CURRENT_SPEC_INDEX.md`

Current implementation (confirmed in code / tool flow):

- Tool exists and is active: `video_pipeline_dedup_recordings`
- Supports dry-run and apply
- Apply behavior is quarantine move (not hard delete)
- Supports manual-review gating (`allowNeedsReview`, confidence threshold behavior)
- Supports optional terrestrial/BS-CS split policy (`keepTerrestrialAndBscs`)
- Uses `rules/broadcast_buckets.yaml` for broadcast bucket classification
- Emits audit artifacts (`dedup_plan_*.jsonl`, `dedup_apply_*.jsonl`)

Important current behavior notes:

- Large `groupsManualReview` counts are not necessarily failures; they often indicate the safety policy is working.
- The dedup tool is downstream of metadata quality.
  - Poor `program_title` / subtitle normalization causes more manual-review groups.
- Apply moves files to quarantine and then updates DB/move audit state; it does not permanently delete files.

For current behavior, prefer these files:
- `skills/SKILL.md` (agent-facing scope and semantics)
- `FLOW_AND_OWNERSHIP.md` (runtime flow)
- `DEPENDENCIES.md` (runtime prerequisites)
- `src/tool-definitions.ts` (current tool schema)
- `py/dedup_recordings.py` (actual logic)

Use the sections below for:
- original grouping/selection design intent
- auto/manual boundary rationale
- future extension discussion

Tag legend used below:
- `[CONTEXT]`: background / motivation
- `[CURRENT]`: matches current implementation behavior (high confidence)
- `[MOSTLY CURRENT]`: mostly matches current behavior, verify details in code/tools
- `[PARTIAL]`: implemented but details/semantics changed
- `[HISTORICAL]`: design history / no longer authoritative
- Composite tags (for example `[HISTORICAL / PARTIAL]`) mean "historical text with some still-relevant content"

## 1. 目的 [CONTEXT]

録画日が異なっていても、同一番組の同一エピソード重複を安全に整理する。  
誤削除を避けるため、初期実装は「削除」ではなく「隔離先への移動」を行う。

## 2. スコープ [MOSTLY CURRENT]

対象:

- `video-library-pipeline` の抽出済みメタデータとファイルパス情報を使った重複候補判定
- Dry-runでの候補可視化
- apply時の重複ファイル隔離移動
- 監査ログ出力とロールバック可能な記録

非対象:

- 番組同定の新しいAIモデル追加
- 実ファイルの完全削除（ゴミ箱/完全消去）
- 複数ホストからの同時実行制御

## 3. 用語定義 [CURRENT]

- 重複グループ: 同一エピソードと判定されたファイル集合
- keep: グループ内で保持する1件
- drop: keep以外（隔離移動対象）
- 隔離先: `<windowsOpsRoot>/duplicates/quarantine`
- 放送種別バケット: `terrestrial`（地上波）/ `bs_cs`（BS・CS）/ `unknown`

## 4. 判定仕様 [MOSTLY CURRENT]

### 4.1 グルーピングキー [MOSTLY CURRENT]

優先順:

1. `normalized_program_key + episode_no`
2. `normalized_program_key + normalized_subtitle`（`episode_no` 欠損時）

以下は自動重複判定しない:

- `normalized_program_key` 欠損
- `episode_no` と `subtitle` の両方が欠損

注記:

- `normalized_program_key` の品質は `rules/program_aliases.yaml` に依存する。
- 重複判定実行前に、抽出レビューに基づく `program_aliases` 更新を AIエージェント経由でユーザーが実施する。

### 4.2 自動処理対象条件 [MOSTLY CURRENT]

自動で keep/drop を決めるのは次を満たす行のみ:

- `needs_review = false`
- `confidence >= 0.85`

満たさない行は `manual_review_required` として候補に残す（自動隔離しない）。

### 4.3 地上波/BSCSの両保持ポリシー（任意） [CURRENT]

- `keepTerrestrialAndBscs=true` のとき、同一重複グループ内でも `terrestrial` と `bs_cs` は別系統として扱う。
- この場合、最低1件ずつ keep を確保する（地上波1件 + BS/CS1件）。
- drop 判定は同一バケット内でのみ実施する。
- `unknown` が混在する場合は自動判定せず `manual_review_required` を優先する。
- 判定入力は `broadcaster` / `channel` / パス情報を総合してバケット化する。

## 5. keep/drop 選定ルール [PARTIAL]

同一グループ内の優先順（上から比較）:

前提:

- `keepTerrestrialAndBscs=true` の場合は、まず放送種別バケットごとに分割してから下記優先順を適用する。

1. 破損/欠損でないファイル
2. 解像度が高い（取得できる場合）
3. ファイルサイズが大きい
4. `mtime` が新しい
5. パス文字列昇順（最終タイブレーク）

## 6. 実行モード [CURRENT]

### 6.1 dry-run（デフォルト） [CURRENT]

- 実ファイル移動なし
- 候補結果と keep/drop 理由をJSONLで出力

### 6.2 apply [CURRENT]

- `drop` のみ隔離先へ移動
- 元パス/移動先/理由/結果を監査ログへ記録
- 失敗件数があっても継続し、最後に集計を返す

## 7. ファイル配置と出力 [MOSTLY CURRENT]

出力先（`windowsOpsRoot` 基準）:

- 候補: `move/dedup_plan_YYYYMMDD_HHMMSS.jsonl`
- 実行結果: `move/dedup_apply_YYYYMMDD_HHMMSS.jsonl`
- 隔離先: `duplicates/quarantine/<group_key>/...`

## 8. ツールインターフェース要件 [MOSTLY CURRENT]

新規ツール:

- `video_pipeline_dedup_recordings`

パラメータ:

- `apply: boolean`（default `false`）
- `maxGroups: integer`（1..5000, optional）
- `confidenceThreshold: number`（default `0.85`）
- `allowNeedsReview: boolean`（default `false`）
- `keepTerrestrialAndBscs: boolean`（default `true`）

戻り値（主要項目）:

- `ok`
- `planPath`
- `applyPath`（apply時）
- `groupsTotal`
- `groupsAutoProcessed`
- `groupsManualReview`
- `groupsSplitByBroadcast`
- `filesKept`
- `filesKeptByBroadcastPolicy`
- `filesDropped`
- `filesMoved`
- `errors[]`

## 9. 既存フローへの組み込み [HISTORICAL / PARTIAL]

推奨順序:

1. 正則化
2. 抽出
3. 抽出結果レビュー（AIエージェント経由でユーザー実行）
4. `rules/program_aliases.yaml` 更新レビュー（AIエージェント経由でユーザー実行）
5. 重複候補（本機能）
6. 重複判定結果レビュー（AIエージェント経由でユーザー実行）
7. move/apply

重要:

- 抽出結果レビュー前に自動隔離を実行しない
- `program_aliases.yaml` 更新前の重複判定は暫定結果として扱う
- `keepTerrestrialAndBscs=true` の場合、地上波/BSCSを跨ぐ自動dropを禁止する
- 初期導入では dry-run を運用標準とする

## 10. 監査/復旧要件 [MOSTLY CURRENT]

- すべての drop 判定に `reason` を付与
- applyログに `src`, `dst`, `group_key`, `keep_path`, `ts`, `ok`, `error` を保存
- ログから逆移動できること（復旧可能性）

## 11. 受け入れ基準 [HISTORICAL]

1. 録画日違いでも同一エピソードが同一グループになる
2. 同番組の別エピソードはグループ分離される
3. `needs_review=true` または低信頼行は自動隔離されない
4. dry-runでファイル変更が発生しない
5. applyで `drop` だけが隔離先へ移動する
6. 監査ログから復旧情報が取れる
7. `keepTerrestrialAndBscs=true` で地上波1件・BS/CS1件が同一エピソードでも両方keepされる

## 12. 既定値（初期） [MOSTLY CURRENT]

- `confidenceThreshold = 0.85`
- `allowNeedsReview = false`
- `keepTerrestrialAndBscs = true`
- `apply = false`
- 完全削除は行わない（隔離移動のみ）
