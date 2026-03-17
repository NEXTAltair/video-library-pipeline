# video-library-pipeline マルチドライブ・EPG・再放送機能 要件定義

## Current Implementation Status (2026-02-27) [CURRENT]

このドキュメントは以下3機能の**要件・設計・使い方**を記録する:

1. **EPG早期取り込み** (`video_pipeline_ingest_epg`) — program.txt → DB
2. **ジャンル別マルチドライブルーティング** (`drive_routes.yaml`) — ファイル移動先を自動判定
3. **再放送グルーピング** (`video_pipeline_detect_rebroadcasts`) — 同エピソード別日付を再放送として記録

For document roles and current source-of-truth mapping, start with `docs/CURRENT_SPEC_INDEX.md`.

Tag legend:
- `[CURRENT]`: 現在の実装に一致（高確度）
- `[CONTEXT]`: 背景・動機

---

## 1. 背景と動機 [CONTEXT]

### 録画ワークフロー

```
EDCB録画 → J:\TVFile (.ts + .program.txt)
    ↓ HandBrake等でエンコード
B:\未視聴 (.mp4)
    ↓ video-library-pipeline
各ドライブ（ジャンル別）
```

### 課題

- **ジャンル情報の消失**: エンコード後に program.txt を削除するため、放送局・ジャンル情報がファイル名にしか残らない
- **単一デスト**: 旧実装は `B:\VideoLibrary` 固定で、複数ドライブへのジャンル別振り分けができなかった
- **再放送の管理**: 同一エピソードを別日付・別局で録画した場合、通常の重複検出と区別できなかった

---

## 2. 機能1: EPG早期取り込み [CURRENT]

### 目的

エンコード前に J:\TVFile の `.program.txt` を DB に保存し、エンコード後のファイルとメタデータを紐付ける。

### 対象ファイル

- `py/edcb_program_parser.py` — .program.txt パーサー
- `py/ingest_program_txt.py` — スキャン・DB書き込みスクリプト
- `src/tool-ingest-epg.ts` — TypeScriptツールラッパー

### .program.txt フォーマット（EDCB形式）

```
2026/02/25(水) 22:45～23:35       ← 行1: 放送日時
ＮＨＫ　ＢＳ                      ← 行2: 放送局（全角）
ドキュメンタリーWAVE「...」        ← 行3: タイトル（[二][字]等のアノテーション付き）
                                   ← 行4: 空行
番組説明テキスト...                 ← 行5以降: 説明文
ジャンル :                         ← ジャンルセクション
  ドキュメンタリー/教養 - 社会・時事
OriginalNetworkID : 4
...
```

### DBへの保存形式

テーブル: `path_metadata`
EPGデータは `programs` / `broadcasts` テーブルに保存（`path_metadata` には書き込まない）

```json
{
  "match_key": "ウルトラマンアーク::2025-01-15::09:00",
  "datetime_key": "2025-01-15::09:00",
  "official_title": "ウルトラマンアーク",
  "broadcaster": "テレビ東京",
  "air_date": "2025-01-15",
  "start_time": "09:00",
  "epg_genres": [{"category": "アニメ/特撮", "subcategory": "特撮"}],
  "description": "第35話 ...",
  "is_rebroadcast_flag": false,
  "network_ids": {"onid": 32742, "tsid": 32742, "sid": 1068, "eid": 25384}
}
```

### マッチキー方式

TS録画とエンコード済みMP4を日付が違っても紐付けるためのキー:

| キー種別 | フォーマット | 用途 |
|---------|------------|------|
| `match_key` | `正規化タイトル::YYYY-MM-DD::HH:MM` | 精密マッチ |
| `datetime_key` | `YYYY-MM-DD::HH:MM` | タイトル不一致時のフォールバック |

正規化: 全角→半角変換, 小文字化, 記号除去

### ツールインターフェース [CURRENT]

```
video_pipeline_ingest_epg
  tsRoot: string   (省略時: config.tsRoot = "J:\TVFile")
  apply: boolean   (default: false)
  limit: integer   (オプション)
```

戻り値:
```json
{
  "ok": true,
  "total": 41,
  "parsed": 41,
  "parseFailed": 0,
  "wouldIngest": 41,   // dry-run時
  "ingested": 41       // apply時
}
```

### 実行タイミング

- **エンコード前・program.txt削除前**に実行
- エンコード後でも可（B:\未視聴 にファイルがなくても動作する）
- 冪等: 既取り込み済みはスキップ
- 一度取り込めば、対応MP4が後から現れてもマッチする

### 後続への効果

`run_metadata_batches_promptv1.py` が `_EpgCache` でEPGデータを参照し、メタデータ抽出時に以下を自動付与:

- `broadcaster`: 放送局名
- `epg_genre`: 第一ジャンル文字列（例: `アニメ/特撮 - 特撮`）
- `epg_genres`: 全ジャンルリスト
- `is_rebroadcast_flag`: EPGによる再放送フラグ
- `epg_description`: 番組説明（先頭300文字）

---

## 3. 機能2: ジャンル別マルチドライブルーティング [CURRENT]

### 目的

ジャンルに応じてファイルの移動先ドライブを自動判定する。旧来の単一 `destRoot` に代わるマルチデスト方式。

### 対象ファイル

- `rules/drive_routes.yaml` — ルーティング設定
- `py/path_placement_rules.py` — `DriveRoutes`, `build_routed_dest_path()` 実装

### drive_routes.yaml 構造

```yaml
version: 1
default_dest: "B:\\VideoLibrary"         # どのルートにもマッチしない場合
default_layout: by_program_year_month

routes:
  - genre: 特撮
    dest_root: "N:\\"
    layout: by_series                    # N:\<番組タイトル>\<filename>
    epg_genre_match:                     # EPGジャンルによる判定（優先）
      - "アニメ/特撮 - 特撮"
    title_patterns:                      # タイトル部分一致フォールバック
      - "ウルトラマン"
      - "仮面ライダー"
```

### ドライブ構成（現在設定）

| ジャンル | 保存先 | レイアウト |
|---------|--------|----------|
| 特撮 | `N:\` | by_series |
| アニメ | `D:\Anime` | by_program_year_month |
| 映画 | `L:\` | by_syllabary |
| ドラマ | `E:\Dドラマ` | by_program_year_month |
| ドキュメンタリー・情報 | `E:\Dドキュメンタリー･情報` | by_program_year_month |
| バラエティ | `E:\Bバラエティ` | by_program_year_month |
| ニュース・報道 | `E:\Nニュース・報道` | by_program_year_month |
| 放送大学 | `B:\放送大学` | by_program_year_month |
| (デフォルト) | `B:\VideoLibrary` | by_program_year_month |

### レイアウト種別

| layout | パス構造 |
|--------|---------|
| `by_program_year_month` | `<dest_root>\<番組名>\<年>\<月>\<filename>` |
| `by_series` | `<dest_root>\<番組名>\<filename>` |
| `by_syllabary` | `<dest_root>\<ア/カ/サ...>\<filename>` |
| `flat` | `<dest_root>\<filename>` |

### マッチング優先順位

1. `epg_genre_match` — EPGジャンル文字列との完全一致またはワイルドカード (`*`) マッチ
2. `title_patterns` — `program_title` の部分一致（小文字化）
3. デフォルト (`default_dest`) — いずれにもマッチしない場合

### 設定方法

`openclaw.json`:
```json
"video-library-pipeline": {
  "config": {
    "driveRoutesPath": "rules/drive_routes.yaml"
  }
}
```

省略時はプラグインルート内の `rules/drive_routes.yaml` を自動使用。

### 移動計画への影響

`video_pipeline_analyze_and_move_videos` の move plan JSONL に以下フィールドが追加:

```json
{
  "path_id": "...",
  "src": "B:\\未視聴\\番組名 2025_01_15_09_00.mp4",
  "dst": "D:\\Anime\\ガンダム\\2025\\01\\番組名 2025_01_15_09_00.mp4",
  "genre_route": "アニメ",
  "dest_drive": "D"
}
```

`plan_stats` に `genre_route_counts` が追加:
```json
{"アニメ": 5, "特撮": 3, "ドラマ": 12, "default": 2}
```

### backfill_roots.yaml（全ドライブ登録済み）

既存ファイルをDB取り込みする際のルートリスト:
```yaml
roots:
  - /mnt/b/VideoLibrary
  - /mnt/b/放送大学
  - /mnt/d/Anime
  - /mnt/e/Bバラエティ
  - /mnt/e/Dドキュメンタリー･情報
  - /mnt/e/Dドラマ
  - /mnt/e/Nニュース・報道
  - /mnt/l
  - /mnt/n
```

---

## 4. 機能3: 再放送グルーピング [CURRENT]

### 目的

同一エピソードを別日付・別局で録画したファイルを「本放送」と「再放送」に分類してDB上でリンクする。
**ファイルは削除しない**。dedup（重複削除）とは別の機能。

### dedup との違い

| 機能 | 対象 | ファイル操作 | 判定根拠 |
|------|------|------------|---------|
| `dedup_recordings` | 完全な重複（削除候補） | 隔離移動 | バイト一致 or 同一エピソード同一局 |
| `detect_rebroadcasts` | 本放送 + 再放送 | **なし** | 同エピソード・air_date or broadcaster の差異 |

### 対象ファイル

- `py/detect_rebroadcasts.py` — 検出・DB書き込みスクリプト
- `src/tool-detect-rebroadcasts.ts` — TypeScriptツールラッパー
- `py/mediaops_schema.py` — `broadcast_groups`, `broadcast_group_members` テーブル定義

### DBテーブル

```sql
-- グループ（同一エピソードの集合）
CREATE TABLE broadcast_groups (
  group_id TEXT PRIMARY KEY,   -- sha256(episode_key)[:16]
  program_title TEXT NOT NULL,
  episode_key TEXT,            -- "番組名::ep::1" or "番組名::sub::サブタイトル"
  created_at TEXT NOT NULL
);

-- グループメンバー（録画1件につき1行）
CREATE TABLE broadcast_group_members (
  group_id TEXT NOT NULL,
  path_id TEXT NOT NULL,
  broadcast_type TEXT NOT NULL DEFAULT 'unknown',  -- 'original' | 'rebroadcast' | 'unknown'
  air_date TEXT,
  broadcaster TEXT,
  added_at TEXT NOT NULL,
  CONSTRAINT uq_bgm UNIQUE (group_id, path_id)
);
```

### グルーピングキー

優先順:
1. `normalize_program_key(program_title) + episode_no` → `"番組名::ep::1"` （都度計算）
2. `normalize_program_key(program_title) + subtitle` → `"番組名::sub::サブタイ"`
3. `normalize_program_key(program_title) + air_date` → `"番組名::date::2025-01-15"` (ep/sub欠損時)

### 再放送判定ロジック

1. 同じ `episode_key` を持つファイルが2件以上ある
2. かつ `air_date` または `broadcaster` が異なる
3. `air_date` の最古のものを `original`、それ以降を `rebroadcast` に分類

### ツールインターフェース [CURRENT]

```
video_pipeline_detect_rebroadcasts
  apply: boolean     (default: false)
  maxGroups: integer (オプション, 1..5000)
```

戻り値:
```json
{
  "ok": true,
  "apply": false,
  "totalMetadataRows": 8102,
  "skippedNoKey": 42,
  "multiEpisodeGroups": 15,     // 2件以上存在するグループ総数
  "rebroadcastGroups": 8,       // air_date/broadcaster差異があるグループ数
  "groupsProcessed": 8,
  "membersTotal": 20,
  "dbInsertedGroups": 0,        // dry-run時は0
  "dbInsertedMembers": 0,
  "plan": [...],                // 最大200件のプレビュー
  "errors": []
}
```

### 冪等性

`ON CONFLICT DO UPDATE` で同じグループ・メンバーを再実行しても安全。

### 実行タイミング

- メタデータ抽出（`video_pipeline_analyze_and_move_videos` or `reextract`）後
- EPG情報がある場合は `broadcaster` フィールドが充実して検出精度が上がる
- 定期実行しても安全（冪等）

---

## 5. 推奨ワークフロー [CURRENT]

### 新規録画の処理フロー

```
① (エンコード前) video_pipeline_ingest_epg apply=false
   → wouldIngest > 0 なら apply=true で取り込み

② (エンコード後) video_pipeline_analyze_and_move_videos apply=false
   → EPG情報でジャンル判定 → drive_routes.yaml でルーティング
   → plan の genre_route_counts でジャンル内訳確認
   → 問題なければ apply=true

③ (定期) video_pipeline_detect_rebroadcasts apply=false
   → rebroadcastGroups > 0 なら内容確認 → apply=true
```

### 既存ファイルのDB取り込み（初回セットアップ）

```
① video_pipeline_backfill_moved_files apply=false
   roots: backfill_roots.yaml の全ドライブ
   → DB取り込み確認 → apply=true

② video_pipeline_reextract
   → メタデータ抽出（EPG情報も活用）

③ video_pipeline_detect_rebroadcasts
   → 再放送グルーピング
```

---

## 6. 設定ファイル一覧 [CURRENT]

| ファイル | 用途 | 変更頻度 |
|---------|------|---------|
| `rules/drive_routes.yaml` | ジャンル→ドライブのルーティングルール | 低（ドライブ構成変更時） |
| `rules/backfill_roots.yaml` | バックフィル対象ルート一覧 | 低（新ドライブ追加時） |
| `rules/program_aliases.yaml` | 番組タイトル正規化ヒント | 中（レビュー後に更新） |
| `rules/broadcast_buckets.yaml` | 地上波/BS-CS分類キーワード | 低 |

---

## 7. 注意点・制約 [CURRENT]

### EPG取り込み

- `path_metadata` テーブルの `path_id` は program.txt のパスハッシュ（実際のmp4パスとは別）
- マッチは `match_key` / `datetime_key` で行い、`path_id` の直接紐付けはしない
- 同一番組が複数チャンネルで放送されると `datetime_key` が重複する場合がある → `match_key`（タイトル含む）でより精密にマッチ

### ドライブルーティング

- EPGジャンルがない場合は `title_patterns` フォールバック
- どちらにもマッチしない場合は `default_dest` (B:\VideoLibrary)
- ルートの優先順は YAML 上の記述順（最初にマッチしたルートが採用）

### 再放送グルーピング

- `episode_no` と `subtitle` の両方が欠損しているファイルは `air_date` キーになるため、同一エピソードでも別グループになる可能性がある
- 再放送グルーピングはファイルを移動しない → dedup とは独立して実行可能
- `broadcast_type = 'original'` は air_date 最古のものへの暫定的な割り当て（EPGの `is_rebroadcast_flag` がある場合はより正確）
