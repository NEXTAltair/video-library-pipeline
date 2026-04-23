# Architecture Decision Records

このディレクトリは `video-library-pipeline` の設計判断を記録する。READMEには概要と導線だけを置き、詳細な設計・運用判断はADRで管理する。

## ADR一覧

| ADR | Status | 概要 |
|---|---|---|
| [ADR-0001](0001-plugin-sdk-testing.md) | Accepted | OpenClaw SDK testing に沿ったプラグインテスト方針 |
| [ADR-0002](0002-pipeline-architecture-and-review-gates.md) | Accepted | 全体パイプライン、Stage、Skillフロー、ヒューマンレビューゲート |
| [ADR-0003](0003-windows-powershell-filesystem-boundary.md) | Accepted | WSL2/Windows境界、PowerShellスクリプト、長パス・Unicode対応 |
| [ADR-0004](0004-tool-orchestration-and-follow-up-calls.md) | Accepted | ツール構成、followUpToolCalls、LLMサブエージェント |
| [ADR-0005](0005-metadata-and-artifact-lifecycle.md) | Accepted | JSONL/YAMLアーティファクト、`windowsOpsRoot`、source遷移 |
| [ADR-0006](0006-mediaops-db-routing-and-safety.md) | Accepted | `mediaops.sqlite`、ジャンルルーティング、安全機構 |

## メンテナンスルール

- コードの挙動を変更した場合は、まず [docs/CURRENT_SPEC_INDEX.md](../CURRENT_SPEC_INDEX.md) の情報源マッピングに従って現行仕様を確認する。
- 設計判断が変わる場合は、既存ADRを直接書き換えるよりも新しいADRを追加し、過去判断との差分を明示する。
- READMEへ長い仕様本文を戻さない。READMEには入口、主要リンク、短い運用ガードレールだけを置く。
