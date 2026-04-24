# ADR-0008: V2 ワークフローカーネルと run-based tool surface

- Status: Proposed
- Date: 2026-04-24
- Related Issues:
  - [#102 V2 redesign: run-based workflow kernel for video-library-pipeline](https://github.com/NEXTAltair/video-library-pipeline/issues/102)
  - [#73 Refactor pipeline around one shared multi-stage workflow instead of semi-independent tool logic](https://github.com/NEXTAltair/video-library-pipeline/issues/73)
  - [#74 Define a cross-language test strategy for Python / TypeScript / PowerShell pipeline layers](https://github.com/NEXTAltair/video-library-pipeline/issues/74)

## Context

現行の `video-library-pipeline` は、未視聴フローと relocate フローを多数の `video_pipeline_*` tool と Python スクリプトで実現している。しかし、ワークフロー状態、レビューゲート、アーティファクト識別、次アクション誘導が複数レイヤーに分散している。

具体的には次の問題がある。

- どの JSONL/YAML が「今の対象」なのかを、`latestJsonlFile` やファイル名規約に依存して推測している。
- `nextStep` / `nextAction` / `nextSteps` / `followUpToolCalls` が tool ごとに手書きされ、状態遷移の source of truth が存在しない。
- sourceRoot フローと relocate フローが同じ概念上のレビュー・計画・apply を扱っているのに、実装上は別々のミニワークフローとして分岐している。
- 人間レビュー済みデータ、LLM 推測データ、計画ファイルの関係が run 単位で束ねられておらず、apply 安全性の判定が局所的になっている。

#73 と #74 では、共有ワークフロー化と横断テスト戦略の必要性を整理した。V2 ではこれを実装可能な設計として固定し、以後のリファクタの基準にする。

## Decision

### 1. V2 の中心概念は `WorkflowRun` とする

V2 では、ユーザーが開始する処理単位を `WorkflowRun` として表現する。run は stable な `runId` を持ち、レビュー、計画、apply、監査ログを同じ run に束ねる。

`WorkflowRun` の必須フィールド:

- `runId`
- `flow`
- `phase`
- `status`
- `createdAt`
- `updatedAt`
- `configSnapshot`
- `artifactIds`
- `reviewGateIds`
- `diagnostics`

`flow` は少なくとも以下を持つ。

- `source_root`
- `relocate`

他の flow を追加する場合は、新しい ADR または本 ADR の後続 ADR で定義する。

### 2. アーティファクトは `ArtifactRef` で明示管理する

V2 では、inventory、抽出結果、レビュー YAML、移動計画、apply ログ、監査 JSON は単なる副作用ファイルではなく `ArtifactRef` として管理する。

`ArtifactRef` の必須フィールド:

- `id`
- `type`
- `path`
- `sha256`
- `createdAt`
- `producer`
- `status`
- `inputArtifactIds`
- `metadata`

アーティファクトの主保存先は `<windowsOpsRoot>/runs/<runId>/` とする。V2 では run ごとに以下のようなディレクトリを持つ。

```text
<windowsOpsRoot>/runs/<runId>/
├── run.json
├── inventory/
├── metadata/
├── review/
├── plan/
├── apply/
└── logs/
```

`llm/` や `move/` 直下の「最新ファイル探索」は、V2 の primary coordination には使わない。旧フロー互換のために残す場合でも、V2 の source of truth にはしない。

### 3. レビューゲートは `ReviewGate` として first-class object にする

レビュー待ち状態は `needs_review=true` の行や YAML ファイル名から間接的に推測するのではなく、run に紐づく `ReviewGate` として持つ。

`ReviewGate` の必須フィールド:

- `id`
- `type`
- `status`
- `artifactIds`
- `requiresHumanReview`
- `openedAt`
- `resolvedAt`
- `resolution`

`status` は少なくとも以下を持つ。

- `open`
- `approved`
- `rejected`
- `superseded`
- `cancelled`

`approved` されていない review gate に依存する plan は apply できない。

### 4. ワークフロー結果は `WorkflowResult` に統一する

Python ワークフロー実装が返す標準契約は `WorkflowResult` とする。TypeScript はこの構造化結果を OpenClaw tool response に翻訳する。

`WorkflowResult` の必須フィールド:

- `ok`
- `runId`
- `flow`
- `phase`
- `outcome`
- `artifacts`
- `gates`
- `nextActions`
- `diagnostics`

自由文の `nextStep` は補助メッセージとして生成してよいが、状態遷移や follow-up 実行の source of truth にはしない。

### 5. 状態遷移は phase table で定義する

V2 の phase は次の通りとする。

| Phase | 意味 | 許可される次 phase |
|---|---|---|
| `created` | run 作成直後 | `inventory_ready`, `metadata_extracted`, `blocked`, `failed` |
| `inventory_ready` | 入力対象とキューが確定 | `metadata_extracted`, `blocked`, `failed` |
| `metadata_extracted` | 抽出結果が生成済み | `review_required`, `metadata_accepted`, `blocked`, `failed` |
| `review_required` | 人間レビュー待ち | `metadata_accepted`, `blocked`, `failed` |
| `metadata_accepted` | planner 入力として許可済み | `plan_ready`, `blocked`, `failed` |
| `plan_ready` | apply 可能な計画が確定 | `applied`, `complete`, `blocked`, `failed` |
| `applied` | 物理変更または DB 反映が完了 | `complete`, `blocked`, `failed` |
| `complete` | 正常終了 | terminal |
| `blocked` | 外部入力待ちまたは安全停止 | terminal |
| `failed` | 実行失敗 | terminal |

flow によっては一部 phase をスキップしてよいが、スキップも明示的な状態遷移として扱う。たとえば relocate dry-run が即座に `plan_ready` または `blocked` に進むのは許容する。

### 6. Python と TypeScript の責務境界を固定する

Python が所有する責務:

- ワークフロー状態遷移
- ドメイン判定
- アーティファクト登録
- レビューゲート生成と検証
- plan/apply の整合性検証
- PowerShell 呼び出しを含む end-to-end orchestration

TypeScript が所有する責務:

- OpenClaw tool 登録
- パラメータ schema
- プラグイン設定解決
- Python 実行境界
- `WorkflowResult` から OpenClaw response への翻訳
- `followUpToolCalls` 生成

TypeScript はワークフロー状態機械の重複実装を持たない。状態判定は Python から返る構造化結果に従う。

### 7. V2 の public tool surface は 4 つに集約する

V2 で公開する OpenClaw tool は次を基本とする。

- `video_pipeline_start`
  - `flow` を指定して run を開始する。
  - 実行可能な最初の segment まで進め、`WorkflowResult` を返す。
- `video_pipeline_resume`
  - 既存 `runId` を継続し、`nextActions` に対応する action を実行する。
- `video_pipeline_status`
  - run 単位または全体の状態、未解決 gate、最新 artifact を返す。
- `video_pipeline_inspect_artifact`
  - artifact のメタデータ、checksum、要約、関連 gate を返す。

旧来の多数の `video_pipeline_*` tool は、V2 では互換維持を前提にしない。必要なら移行期間だけ shim を置くが、public surface の基準は上記 4 tool とする。

### 8. V2 で禁止する実装パターン

以下は V2 で禁止する。

- `latestJsonlFile` 等による暗黙の最新ファイル選択を primary flow に使うこと
- tool ごとに `followUpToolCalls` を手書きし、状態遷移ロジックを分散させること
- `planPath` が同じ `runId` に属するか検証せずに apply すること
- review 済みかどうかをファイル名や慣例だけで判定すること
- Python と TypeScript の双方で別々に「次に何をするか」を推論すること

## Consequences

- 既存の public tool surface は breaking change の対象になる。
- `CURRENT_SPEC_INDEX` では、本 ADR を現行動作ではなく V2 計画ドキュメントとして扱う必要がある。
- 実装は `#104` 以降の issue で段階的に進めるが、以後の PR は本 ADR の概念に沿って設計判断を行う。
- V2 移行後は、レビュー、計画、apply、アーカイブが run 単位で追跡できるため、エージェントの follow-up と監査が簡潔になる。

## Non-goals

本 ADR は次を扱わない。

- 旧データや旧アーティファクトの自動移行方式
- PowerShell スクリプトの全面書き換え
- 新しいタイトル抽出ヒューリスティクスの追加
- channel plugin / setup entry / agent harness への転換

これらが必要になった場合は別 ADR で扱う。
