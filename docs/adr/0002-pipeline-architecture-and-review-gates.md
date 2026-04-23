# ADR-0002: パイプライン構成とヒューマンレビューゲート

- Status: Accepted
- Date: 2026-04-23
- Source: README sections 1, 3, 4, 5 before ADR split

## Context

`video-library-pipeline` はEDCB録画から生成されたMP4を、棚卸、メタデータ抽出、レビュー、ジャンル別ドライブ移動まで処理する。AIエージェントがツールを連鎖実行するため、自動処理が誤ったメタデータや移動計画をそのまま適用しないよう、明示的なレビューゲートが必要である。

既存ライブラリの再整理も同じメタデータ抽出・レビュー・移動基盤を使うため、sourceRootフローとrelocateフローの関係を明確にする必要がある。

## Decision

プラグインは次の2系統のフローを持つ。

- sourceRootフロー: 未視聴フォルダを棚卸し、メタデータ抽出とヒューマンレビューを経てジャンル別ドライブへ移動する。
- relocateフロー: 既存ライブラリのファイルをスキャンし、メタデータ不足を補完して正しいフォルダ構成へ再配置する。

sourceRootフローは3ステージに分ける。

- Stage 1: `inventory-review`。未視聴フォルダのファイル棚卸とキュー生成を行う。
- Stage 2: `extract-review`。ファイル名とEPGヒントからメタデータを抽出し、ヒューマンレビューを経てDBへ書き込む。
- Stage 3: `move-review`。DBのメタデータに基づき、ジャンル別ドライブへファイルを振り分ける。

ステージ間にはヒューマンレビューゲートを置く。`needs_review=true` のファイルは、明示的に許可されない限り移動計画から除外する。

## Architecture Overview

```mermaid
flowchart LR
    subgraph SRC["入力"]
        TS["EDCB録画<br>.ts + .program.txt"]
        MP4["エンコード済<br>.mp4"]
    end

    subgraph PIPELINE["OpenClaw Skills"]
        direction TB
        S0["ingest_epg<br>EPG取り込み"]
        S1["Stage 1: inventory-review<br>ファイル棚卸 + キュー生成"]
        S2["Stage 2: extract-review<br>メタデータ抽出 + レビュー"]
        S3["Stage 3: move-review<br>ジャンル別振り分け + 移動"]
        DB[("mediaops.sqlite")]

        S0 --> DB
        S1 --> S2 --> S3
        DB -.-> S2
        DB -.-> S3
    end

    subgraph DST["ライブラリ (ジャンル別ドライブ)"]
        D1["N: 特撮"]
        D2["D: アニメ"]
        D3["L: 映画"]
        D4["E: ドラマ / バラエティ等"]
        D5["B: デフォルト"]
    end

    TS --> S0
    MP4 --> S1
    S3 --> DST

    DST -. "再整理<br>relocate-review" .-> S2
```

`relocate-review` は既存ライブラリの再整理用フローで、Stage 2-3 を再利用してメタデータ補完・再配置を行う。

## Main Pipeline

```mermaid
flowchart TD
    EPG_START(["EDCB .program.txt"])
    EPG_INGEST["video_pipeline_ingest_epg<br/>py/ingest_program_txt.py<br/>py/edcb_program_parser.py"]
    EPG_DB[("programs / broadcasts")]
    EPG_START --> EPG_INGEST --> EPG_DB

    S1_IN(["B:\未視聴 (MP4)"])
    S1_VALIDATE["video_pipeline_validate<br/>環境チェック"]
    S1_RUN["video_pipeline_analyze_and_move_videos<br/>apply=false"]
    S1_PS_INV["PowerShell: unwatched_inventory.ps1<br/>ファイル列挙 + ハッシュ"]
    S1_OUT_INV["inventory_unwatched_*.jsonl"]
    S1_OUT_Q["queue_unwatched_batch_*.jsonl"]
    S1_GATE{{"ヒューマンレビュー<br/>棚卸・キュー確認"}}

    S1_IN --> S1_VALIDATE --> S1_RUN
    S1_RUN --> S1_PS_INV
    S1_PS_INV --> S1_OUT_INV & S1_OUT_Q
    S1_OUT_INV & S1_OUT_Q --> S1_GATE

    S2_REEXTRACT["video_pipeline_reextract<br/>py/run_metadata_batches_promptv1.py"]
    S2_EPG_MATCH["EPGヒント照合<br/>match_key / datetime_key"]
    S2_EXTRACT_OUT["llm_filename_extract_output_*.jsonl"]
    S2_EXPORT["video_pipeline_export_program_yaml<br/>ヒントYAML生成"]
    S2_YAML["program_aliases_review_*.yaml"]
    S2_GATE{{"ヒューマンレビュー<br/>メタデータ確認・YAML編集"}}
    S2_APPLY["video_pipeline_apply_reviewed_metadata<br/>YAML/JSONL -> DB書き込み"]
    S2_DB[("path_metadata<br/>human_reviewed=true")]

    S1_GATE -->|承認| S2_REEXTRACT
    EPG_DB -.->|ヒント| S2_EPG_MATCH
    S2_REEXTRACT --> S2_EPG_MATCH --> S2_EXTRACT_OUT
    S2_EXTRACT_OUT --> S2_EXPORT --> S2_YAML
    S2_YAML -.->|次回reextractのヒント入力| S2_REEXTRACT
    S2_EXTRACT_OUT --> S2_GATE
    S2_YAML -->|sourceYamlPath| S2_APPLY
    S2_GATE -->|編集済YAML/JSONL| S2_APPLY --> S2_DB

    S3_RUN["video_pipeline_analyze_and_move_videos<br/>apply=true"]
    S3_PLAN["make_move_plan_from_inventory.py<br/>+ drive_routes.yaml ジャンルルーティング"]
    S3_PS_MOVE["PowerShell: apply_move_plan.ps1<br/>物理ファイル移動"]
    S3_DB_SYNC["update_db_paths_from_move_apply.py<br/>DBパス同期"]
    S3_DONE(["ライブラリ配置完了"])

    S2_DB --> S3_RUN --> S3_PLAN --> S3_PS_MOVE --> S3_DB_SYNC --> S3_DONE

    style S1_GATE fill:#f9a825,color:#000
    style S2_GATE fill:#f9a825,color:#000
    style EPG_DB fill:#1565c0,color:#fff
    style S2_DB fill:#1565c0,color:#fff
```

`video_pipeline_analyze_and_move_videos` のdry-runは、`queue_unwatched_batch_*.jsonl` 生成後に内部でrule-based `reextract` まで進む。レビューが必要な抽出結果が残った場合は、`program_aliases_review_*.yaml` を自動生成し、結果JSONに `reviewYamlPath` / `reviewYamlPaths` を返す。

手動で `video_pipeline_reextract` を呼ぶのは、既存キューを個別再処理したい場合の補助フローとする。

## Relocate Flow

```mermaid
flowchart TD
    START(["既存ライブラリ<br/>D:\Anime, E:\Dドラマ 等"])

    VALIDATE["video_pipeline_relocate_existing_files<br/>apply=false (dry-run)"]
    VALIDATE_OUT{"dry-run結果"}

    READY["plannedMoves > 0<br/>移動計画あり"]
    MISSING["メタデータ不足<br/>or 疑似タイトル検出"]
    CORRECT["plannedMoves = 0<br/>全ファイル正位置"]

    PREP["video_pipeline_prepare_relocate_metadata<br/>relocate dry-run + queue生成<br/>+ reextract実行"]
    REVIEW_GATE{{"ヒューマンレビュー<br/>抽出メタデータ確認"}}
    APPLY_META["video_pipeline_apply_reviewed_metadata<br/>or apply_llm_extract_output"]

    APPLY["video_pipeline_relocate_existing_files<br/>apply=true + planPath"]
    DONE(["再配置完了"])

    START --> VALIDATE --> VALIDATE_OUT
    VALIDATE_OUT -->|Route A| READY --> APPLY --> DONE
    VALIDATE_OUT -->|Route B| MISSING --> PREP --> REVIEW_GATE --> APPLY_META
    APPLY_META -.->|再度dry-run| VALIDATE
    VALIDATE_OUT -->|Route C| CORRECT --> DONE

    style REVIEW_GATE fill:#f9a825,color:#000
```

`prepare_relocate_metadata` は内部で以下を順に実行する複合オーケストレーターである。

1. relocate dry-run (`queueMissingMetadata=true, writeMetadataQueueOnDryRun=true`)
2. メタデータ不足ファイルのキューJSONL生成
3. `reextract` によるルールベースメタデータ抽出
4. `followUpToolCalls` で export_program_yaml -> apply_reviewed_metadata -> relocate dry-run を指示

## Skill Flow

各SkillはOpenClawのAIエージェントが呼び出すインタラクティブガイドであり、内部で複数のツールを順序立てて実行する。

| Skill | 概要 |
|---|---|
| `video-library-pipeline` | トップレベルインテントルーター。ユーザーの意図を判別し適切なSkillに誘導する |
| `inventory-review` | Stage 1。未視聴フォルダのファイル棚卸とキュー生成を行う |
| `extract-review` | Stage 2。ファイル名からメタデータを抽出し、ヒューマンレビューを経てDBに書き込む |
| `move-review` | Stage 3。DBのメタデータに基づき、ジャンル別ドライブにファイルを振り分ける |
| `relocate-review` | 既存ライブラリの再配置。メタデータ補完、dry-run、applyのサイクルを誘導する |
| `folder-cleanup` | フォルダ名汚染の修正。ユーザー指定のターゲットを起点に修正する |

```mermaid
graph LR
    subgraph "Skill: inventory-review"
        T_VAL["validate"]
        T_STATUS["status"]
        T_RUN_DRY["analyze_and_move_videos<br/>apply=false"]
    end

    subgraph "Skill: extract-review"
        T_REEXT["reextract"]
        T_EXPORT["export_program_yaml"]
        T_APPLY["apply_reviewed_metadata"]
    end

    subgraph "Skill: move-review"
        T_RUN_APPLY["analyze_and_move_videos<br/>apply=true"]
        T_LOGS["logs"]
    end

    subgraph "Skill: relocate-review"
        T_RELOC_DRY["relocate_existing_files<br/>apply=false"]
        T_PREP["prepare_relocate_metadata"]
        T_LLM_APPLY["apply_llm_extract_output"]
        T_RELOC_APPLY["relocate_existing_files<br/>apply=true"]
    end

    subgraph "補助ツール"
        T_BACKFILL["backfill_moved_files"]
        T_DEDUP["dedup_recordings"]
        T_DEDUP_RB["dedup_rebroadcasts"]
        T_REBROADCAST["detect_rebroadcasts"]
        T_CONTAMINATION["detect_folder_contamination"]
        T_TITLES["update_program_titles"]
        T_NORMCASE["normalize_folder_case"]
        T_EPG["ingest_epg"]
        T_BACKUP["db_backup / db_restore"]
        T_REPAIR["repair_db"]
    end

    T_VAL --> T_STATUS --> T_RUN_DRY --> T_REEXT
    T_REEXT --> T_EXPORT --> T_APPLY --> T_RUN_APPLY
```

## Consequences

- AIエージェントは、ツールの実行順序だけでなくレビューゲートを守る責務を持つ。
- `allowNeedsReview` を明示しない限り、レビュー待ちファイルは物理移動されない。
- sourceRootとrelocateは別フローだが、メタデータ抽出・レビュー・移動安全機構は共有する。
- READMEには概要だけを残し、詳細なフロー図はこのADRを参照する。
