# ADR-0001: OpenClaw SDK Testing に沿ったプラグインテスト方針

- Status: Accepted
- Date: 2026-04-21
- Source: https://docs.openclaw.ai/plugins/sdk-testing

## Context

`video-library-pipeline` は 2 つのテスト対象を持つ。

1. OpenClaw プラグイン層
   - `index.ts`
   - `src/tool-*.ts`
   - `src/plugin-hooks.ts`
   - `src/runtime.ts` のうち SDK 境界に近い補助ロジック
2. Python 実行層
   - `py/*.py`
   - DB / パス判定 / CLI スクリプトのコアロジック

現状でも TypeScript 側は Vitest、Python 側は pytest を使っているが、プラグイン層のテスト戦略を OpenClaw SDK の推奨に明示的に揃えておく必要がある。

OpenClaw の `sdk-testing` ドキュメントでは、プラグイン作者向けに以下が示されている。

- プラグインテストは Vitest を前提にする
- SDK テスト用ヘルパーは `openclaw/plugin-sdk/testing` から import する
- runtime store を使うコードは、テストで mock runtime を set / clear する
- stub は prototype mutation ではなく per-instance で行う
- テストは scoped 実行できる形を保つ
- in-repo plugin 向け lint 制約は外部 plugin に必須ではないが、同じ import discipline に従うことを推奨する

## Decision

### 1. プラグイン層の標準テストランナーは Vitest とする

OpenClaw SDK 境界に接する TypeScript テストは Vitest で書く。対象は以下とする。

- プラグインエントリ登録
- ツール登録とパラメータ/返却コントラクト
- runtime helper
- plugin hook
- OpenClaw 設定解決や runtime 依存の薄いラッパー

### 2. SDK 提供の testing subpath を優先する

OpenClaw プラグイン向けのテストで SDK ヘルパーや型が必要な場合は、`openclaw/plugin-sdk/testing` から import する。

- `openclaw/plugin-sdk` の root barrel は使わない
- SDK 内部の `src/` を直接 import しない
- 今後このプラグインが SDK 向けの補助 subpath を持つ場合、自分自身の self-import も避ける

外部 plugin リポジトリであるため lint 強制そのものには依存しないが、実装規約として同じ制約に従う。

### 3. runtime 依存コードは mock runtime を明示管理する

`createPluginRuntimeStore` またはそれに準ずる runtime singleton を使うコードは、テストごとに mock runtime を注入し、終了時に clear する。

- テスト間で runtime 状態を共有しない
- secret や config 書き込みの副作用は mock で閉じ込める
- OpenClaw 側の account / config inspection は materialized secret を露出しない前提で検証する

### 4. stub は per-instance を原則とする

クライアントや wrapper の振る舞い差し替えは、prototype mutation ではなくインスタンス単位で行う。

- 推奨: `client.sendMessage = vi.fn()`
- 非推奨: `Client.prototype.sendMessage = vi.fn()`

これにより、並列実行や別テストケースへの汚染を避ける。

### 5. Python コアロジックのテストは pytest を維持する

`sdk-testing` は OpenClaw のプラグイン層に対する指針であり、Python 実行層を Vitest に寄せる意図ではない。したがって以下は pytest を継続する。

- DB 更新ロジック
- パス解析/正規化
- メタデータ修復
- ルール判定
- Python CLI スクリプトの関数単位テスト

つまり本リポジトリのテスト方式は「OpenClaw 境界は Vitest、Python 実行層は pytest」の二層構成とする。

### 6. 実行方法は scoped run を前提に保つ

日常運用では以下を標準とする。

- TypeScript 全件: `npm test`
- TypeScript 単体: `npx vitest run <path>`
- Python 全件: `pytest`
- Python 単体: `pytest <path>`

Vitest 実行でメモリ圧がある場合は、OpenClaw docs に合わせて worker 数を絞る。

- `OPENCLAW_VITEST_MAX_WORKERS=1 npm test`

## Consequences

- 今後の TypeScript テストは、単なる pure helper のみではなく、OpenClaw に公開される契約を意識して追加する
- plugin 層のテストを書くときは、SDK の testing subpath と runtime mocking パターンを優先する
- Python テストは引き続き重要であり、SDK testing 方針と競合しない
- この ADR は「どの層をどのツールでどう検証するか」の基準になる

## Follow-up

- 新しく plugin entry / tool registration を直接検証する Vitest を追加するときは、この ADR の import discipline と mocking ルールに従う
- `package.json` の test scripts を増やす場合も、この ADR の二層構成を崩さない
