# Architecture Decision Records

このディレクトリは `video-library-pipeline` の設計判断を記録する。READMEには概要と導線だけを置き、詳細な設計・運用判断はADRで管理する。

## ADR一覧

| ADR | Status | 概要 |
|---|---|---|
| [ADR-0001](0001-plugin-sdk-testing.md) | Accepted | OpenClaw SDK testing に沿ったプラグインテスト方針 |
| [ADR-0002](0002-pipeline-architecture-and-review-gates.md) | Accepted | 全体パイプライン、Stage、Skillフロー、ヒューマンレビューゲート |
| [ADR-0003](0003-windows-powershell-filesystem-boundary.md) | Accepted | WSL2/Windows境界、PowerShellスクリプト、長パス・Unicode対応 |
| [ADR-0004](0004-tool-orchestration-and-follow-up-calls.md) | Accepted (Legacy) | V1 ツール構成、followUpToolCalls、LLMサブエージェント |
| [ADR-0005](0005-metadata-and-artifact-lifecycle.md) | Accepted | JSONL/YAMLアーティファクト、`windowsOpsRoot`、source遷移 |
| [ADR-0006](0006-mediaops-db-routing-and-safety.md) | Accepted | `mediaops.sqlite`、ジャンルルーティング、安全機構 |
| [ADR-0007](0007-openclaw-sdk-recommended-plugin-implementation.md) | Accepted | OpenClaw SDK公式docsに基づくplugin推奨実装方針 |
| [ADR-0008](0008-v2-workflow-kernel-and-run-based-tool-surface.md) | Accepted | V2 run-based workflow kernel と公開tool surface方針 |
| [ADR-0009](0009-deterministic-execution-and-file-identity.md) | Accepted | 確定的日次CLIと不変のfile/location同一性 |
| [ADR-0010](0010-managed-artifact-retention.md) | Accepted | cache/evidence分離、可逆整理、旧データのソース外移設 |
| [ADR-0011](0011-unified-application-kernel-and-recoverable-operations.md) | Proposed | 共通kernel、durable job、承認と副作用の境界、統合照会、中断復旧 |
| [ADR-0012](0012-m0-integrity-boundaries.md) | Accepted | M0: run-bound metadata、一括transaction、online snapshot、writer排他、失敗判定 |

`ADR-0008` は #108 以降の現行 V2 公開 tool surface の基準である。`ADR-0004` は V1/legacy orchestration の背景として残すが、現行の公開 tool 一覧ではない。

`ADR-0011`は次期設計。現行実装を置き換えたとは扱わない。
M0の実装済み境界のみ[ADR-0012](0012-m0-integrity-boundaries.md)で区別する。
採用・実装の判定は[設計監査](../reviews/2026-10-10-design-audit.md)と
[段階移行の受け入れ条件](../reviews/2026-10-10-redesign-roadmap.md)を使う。

## メンテナンスルール

- コードの挙動を変更した場合は、まず [docs/CURRENT_SPEC_INDEX.md](../CURRENT_SPEC_INDEX.md) の情報源マッピングに従って現行仕様を確認する。
- 設計判断が変わる場合は、既存ADRを直接書き換えるよりも新しいADRを追加し、過去判断との差分を明示する。
- READMEへ長い仕様本文を戻さない。READMEには入口、主要リンク、短い運用ガードレールだけを置く。
