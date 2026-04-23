# ADR-0006: mediaops DB、ジャンルルーティング、安全機構

- Status: Accepted
- Date: 2026-04-23
- Source: README sections 9, 10, 11, 12, 14 before ADR split

## Context

このプラグインはファイル移動、メタデータ抽出、EPG照合、再放送グルーピング、重複隔離を同じSQLite DBで追跡する。AIエージェントと人間レビューが関わるため、DB契約、ルーティング、apply安全機構を明確にする必要がある。

## Decision

`mediaops.sqlite` を唯一の操作追跡DBとし、ファイルパス、ファイル実体、観測結果、イベント、メタデータ、EPG由来番組情報を分離して管理する。

ジャンル別移動は `rules/drive_routes.yaml` に基づき、EPGジャンルまたはタイトルパターンで振り分け先を決定する。安全機構として、レビューゲート、dry-run/apply、DBバックアップ、`needs_review + source` ゲートを採用する。

## DB Schema

```mermaid
erDiagram
    runs {
        TEXT run_id PK
        TEXT kind
        TEXT target_root
        TEXT started_at
        TEXT finished_at
        TEXT tool_version
        TEXT notes
    }

    paths {
        TEXT path_id PK
        TEXT path UK
        TEXT drive
        TEXT dir
        TEXT name
        TEXT ext
        TEXT created_at
        TEXT updated_at
    }

    files {
        TEXT file_id PK
        INTEGER size_bytes
        TEXT content_hash
        TEXT hash_algo
        REAL duration_sec
        INTEGER width
        INTEGER height
        TEXT codec
        INTEGER bitrate
        TEXT fps
        TEXT created_at
        TEXT updated_at
    }

    file_paths {
        TEXT file_id FK
        TEXT path_id FK
        INTEGER is_current
        TEXT first_seen_run_id FK
        TEXT last_seen_run_id FK
    }

    observations {
        TEXT run_id FK
        TEXT path_id FK
        INTEGER size_bytes
        TEXT mtime_utc
        TEXT type
        TEXT name_flags
    }

    events {
        INTEGER event_id PK
        TEXT run_id FK
        TEXT ts
        TEXT kind
        TEXT src_path_id FK
        TEXT dst_path_id FK
        TEXT detail_json
        INTEGER ok
        TEXT error
    }

    path_metadata {
        TEXT path_id PK-FK
        TEXT source
        TEXT data_json
        TEXT updated_at
    }

    programs {
        TEXT program_id PK
        TEXT program_key UK
        TEXT canonical_title
        TEXT created_at
    }

    broadcasts {
        TEXT broadcast_id PK
        TEXT program_id FK
        TEXT air_date
        TEXT start_time
        TEXT end_time
        TEXT broadcaster
        TEXT match_key UK
        TEXT data_json
        TEXT created_at
    }

    path_programs {
        TEXT path_id FK
        TEXT program_id FK
        TEXT broadcast_id FK
        TEXT source
        TEXT updated_at
    }

    tags {
        INTEGER tag_id PK
        TEXT name
        TEXT namespace
    }

    path_tags {
        TEXT path_id FK
        INTEGER tag_id FK
        TEXT source
        TEXT updated_at
    }

    broadcast_groups {
        TEXT group_id PK
        TEXT program_title
        TEXT episode_key
        TEXT created_at
    }

    broadcast_group_members {
        TEXT group_id FK
        TEXT path_id FK
        TEXT broadcast_type
        TEXT air_date
        TEXT broadcaster
        TEXT added_at
    }

    files ||--o{ file_paths : "has paths"
    paths ||--o{ file_paths : "linked to files"
    runs ||--o{ file_paths : "first/last seen"
    runs ||--o{ observations : "observed in"
    paths ||--o{ observations : "observed as"
    runs ||--o{ events : "triggered"
    paths ||--o{ events : "src/dst"
    paths ||--|| path_metadata : "has metadata"
    programs ||--o{ broadcasts : "has airings"
    paths ||--o{ path_programs : "linked"
    programs ||--o{ path_programs : "linked"
    broadcasts ||--o{ path_programs : "matched"
    paths ||--o{ path_tags : "tagged"
    tags ||--o{ path_tags : "applied to"
    broadcast_groups ||--o{ broadcast_group_members : "contains"
    paths ||--o{ broadcast_group_members : "member of"
```

| テーブル | 役割 |
|---|---|
| `runs` | パイプライン実行の監査ログ |
| `paths` | ファイルパスの正規化レジストリ |
| `files` | ファイル実体。サイズ、コンテンツハッシュ等 |
| `file_paths` | filesとpathsの多対多マッピング。移動履歴を `is_current` で追跡 |
| `observations` | 実行時のファイル状態スナップショット |
| `events` | 移動・リロケート等のアクション記録 |
| `path_metadata` | 抽出メタデータ。`source` と `data_json` を格納 |
| `programs` / `broadcasts` | EPG由来の番組シリーズ・放送履歴 |
| `path_programs` | ファイルと番組シリーズの紐付け |
| `tags` / `path_tags` | Tablacus連携用タグ |
| `broadcast_groups` / `broadcast_group_members` | 再放送グルーピング |

## Rebroadcast Rule

`video_pipeline_detect_rebroadcasts` は `broadcasts.data_json.is_rebroadcast_flag` を唯一の信頼ソースとして次の優先順で判定する。

1. グループ内に `is_rebroadcast_flag=true` が1件でもあれば、その行を `rebroadcast`、それ以外を `original` とする。
2. `true` が1件も無い場合、全件 `unknown` とする。

EPGフラグが無いケースでは、`air_date` の前後だけで original/rebroadcast を決めない。

## Path ID

`path_id` はWindowsパス正規化後のUUIDv5で生成する。

```python
path_id = str(uuid.uuid5(PATH_NAMESPACE, "winpath:" + normalize_win_for_id(path)))
```

`normalize_win_for_id` はWindowsパスを正規化し、同一ファイルに対して常に同じ `path_id` が生成されることを保証する。

## Metadata Contract

DBの `path_metadata` テーブルに格納されるメタデータは以下の3フィールドを必須とする。

```python
DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}
```

| フィールド | 意味 |
|---|---|
| `program_title` | 番組タイトル。サブタイトルを含めない |
| `air_date` | 放送日。`YYYY-MM-DD` |
| `needs_review` | ヒューマンレビュー待ちフラグ |

| source値 | 意味 | 生成元 |
|---|---|---|
| `rule_based` | ルールベース抽出 | `run_metadata_batches_promptv1.py` |
| `llm` | LLM抽出 | `apply_llm_extract_output.py` |
| `human_reviewed` | ヒューマンレビュー済み | `apply_reviewed_metadata` |

`source='human_reviewed'` はレビュー済み確定データである。`source='llm'` はLLM推測データであり、relocate時にsuspicious titleチェックを通過した場合のみ信頼する。

`file_paths.is_current` はファイル移動時に旧パスを `0`、新パスを `1` に更新する。これにより複数パスで同一ファイルの移動履歴を追跡できる。

## Genre Drive Routing

`rules/drive_routes.yaml` に基づき、EPGジャンルまたはタイトルパターンで振り分け先を決定する。上から順にマッチ判定し、最初にマッチしたルートへ振り分ける。

| ジャンル | 振り分け先 | レイアウト |
|---|---|---|
| 特撮 | `N:\` | `by_title` |
| アニメ | `D:\Anime` | `by_title` |
| 映画 | `L:\` | `by_syllabary` |
| ドラマ | `E:\Dドラマ` | `by_title` |
| ドキュメンタリー・情報 | `E:\Dドキュメンタリー･情報` | `by_program_year_month` |
| バラエティ | `E:\Bバラエティ` | `by_program_year_month` |
| ニュース・報道 | `E:\Nニュース・報道` | `by_program_year_month` |
| 放送大学 | `B:\放送大学` | `by_program_year_month` |
| デフォルト | `B:\VideoLibrary` | `by_program_year_month` |

| タイプ | フォルダ構成 | 用途 |
|---|---|---|
| `by_program_year_month` | `<root>/<program_title>/<year>/<month>/<file>` | 大部分のジャンル |
| `by_syllabary` | `<root>/<五十音フォルダ>/<file>` | 映画 |
| `by_title` | `<root>/<program_title>/<file>` | 特撮、アニメ、ドラマ |
| `flat` | `<root>/<file>` | 現在未使用 |

`by_series` は後方互換名として受け付けつつ、内部で `by_title` に正規化する。

## Safety Mechanisms

### `apply_reviewed_metadata` の2段ゲート

1. 生ファイル名拒否ゲート。`llm_filename_extract_output_NNNN_NNNN.jsonl` 形式の生抽出ファイルは `markHumanReviewed=true` 時に無条件で拒否する。`allowNoContentChanges` でもバイパス不可。
2. 内容変更チェックゲート。レビュー済みJSONLとベースライン抽出出力を比較し、変更が0行の場合は拒否する。`allowNoContentChanges=true` で合法的にバイパス可能だが、疑似タイトルや `needs_review=true` が残る場合はバイパス不可。

YAML apply はcanonical titleが既にsource JSONL側へ反映済みでも、タイトル系理由しか残っていないstale `needs_review` を自動で解除する。既存DBに残っている過去分は `video_pipeline_repair_db` + `action: "clear_review_flags"` で補修する。

### dry-run/apply

全apply系ツールに共通する安全設計として、`apply=false` で計画を生成してレビューし、`apply=true` で物理操作を実行する。

対象は次の通り。

- `analyze_and_move_videos`
- `relocate_existing_files`
- `backfill_moved_files`
- `dedup_recordings`

### 自動DBバックアップ

以下のツールはapply実行前に自動でDBバックアップを作成し、最新10世代を保持する。

- `relocate_existing_files`。`apply=true` 時、`descriptor=pre_relocate_apply`
- `apply_reviewed_metadata`。`descriptor=pre_apply_reviewed_metadata`

### `allowNeedsReview`

`allowNeedsReview` は `relocate_existing_files` と `analyze_and_move_videos` の両方でデフォルトfalseとする。`needs_review=true` のファイルはレビュー完了まで移動計画から除外される。

### relocate の `needs_review + source` ゲート

`relocate_existing_files` は以下の段階的ゲートで移動可否を判定する。

- `needs_review=true` は `allowNeedsReview=true` を指定しない限り常に除外する。
- `needs_review=false` + `source=human_reviewed` はそのまま移動計画対象にする。
- `needs_review=false` + `source=llm` はsuspicious titleチェックを通過した場合のみ移動計画対象にする。
- `source` が `human_reviewed` / `llm` 以外の行は `allowUnreviewedMetadata=true` を指定しない限り除外する。

`source=llm` でsuspicious titleチェックに失敗した行は、apply時のみDB上で `needs_review=true` と `needs_review_reason` 追記に更新し、レビュー待ちに戻す。dry-runではDBを変更しない。

## Known Issues and Outcomes

| # | 問題 | Issue | 状態 |
|---|---|---|---|
| 1 | Stage 2から3のゲートが暗黙的 | - | `allowNeedsReview` デフォルトfalseで実質制御済み |
| 2 | LLMサブエージェントのリカバリ機構がない | [#13](https://github.com/NEXTAltair/video-library-pipeline/issues/13) | 解決済み。`llm_extract_status` で検出・リトライ |
| 3 | EPG `match_key` 衝突リスク | [#7](https://github.com/NEXTAltair/video-library-pipeline/issues/7) | 解決済み。`match_key` にbroadcasterを含める |
| 4 | relocateフローのsource不整合 | [#6](https://github.com/NEXTAltair/video-library-pipeline/issues/6) | open |
| 5 | 再放送判定のair_date依存による誤分類 | [#25](https://github.com/NEXTAltair/video-library-pipeline/issues/25) | 解決済み。EPG `is_rebroadcast_flag` 優先 |
| 6 | DBバックアップのローテーション | - | 解決済み。`rotate_backups(keep=10)` |
| 7 | PowerShellエラーハンドリング不透明 | [#12](https://github.com/NEXTAltair/video-library-pipeline/issues/12) | 解決済み。`moveApplyStats` で構造化エラー伝播 |
| 8 | `program_aliases.yaml` 循環依存 | [#14](https://github.com/NEXTAltair/video-library-pipeline/issues/14) | 不要。既存アーカイブ機構で管理可能 |
| 9 | Python/TS重複コード整理 | - | 完了 |
| 10 | cron定期EPG取り込み | [#8](https://github.com/NEXTAltair/video-library-pipeline/issues/8) | 解決済み。毎日05:00 JST cronジョブ追加 |
| 11 | `normalize_filenames.ps1` 欠落サブスクリプト | [#9](https://github.com/NEXTAltair/video-library-pipeline/issues/9) | 解決済み。ラッパー削除、メタデータ抽出側で対応 |
| 12 | `list_remaining_unwatched.ps1` 再検討 | [#10](https://github.com/NEXTAltair/video-library-pipeline/issues/10) | 解決済み。計算ロジックで置換、PS1削除 |
| 13 | PS1スクリプト統合検討 | [#11](https://github.com/NEXTAltair/video-library-pipeline/issues/11) | 統合見送り。共通部分は `_long_path_utils.ps1` に集約 |
| 14 | WSL2 ERRNO5 (EIO) スキャン不安定性 | [#32](https://github.com/NEXTAltair/video-library-pipeline/issues/32) | 解決済み。PowerShell primary scanで9Pをバイパス |

その他のIssueは以下を参照する。

- [#1](https://github.com/NEXTAltair/video-library-pipeline/issues/1): `data_json` 拡張
- [#2](https://github.com/NEXTAltair/video-library-pipeline/issues/2): `by_series` リネーム
- [#3](https://github.com/NEXTAltair/video-library-pipeline/issues/3): sourceリネーム
- [#4](https://github.com/NEXTAltair/video-library-pipeline/issues/4): filesテーブルにハッシュとメタデータを追加
- [#5](https://github.com/NEXTAltair/video-library-pipeline/issues/5): アニメ・ドラマレイアウト移行

## Consequences

- DBのsource値と `needs_review` を組み合わせることで、推測データと確定データの扱いを分けられる。
- ファイル移動は必ず計画、レビュー、applyの順に行う。
- EPG由来の再放送フラグがない場合、時系列だけでoriginal/rebroadcastを推測しない。
- ルーティング変更は `rules/drive_routes.yaml` とこのADRの両方を確認して行う。
