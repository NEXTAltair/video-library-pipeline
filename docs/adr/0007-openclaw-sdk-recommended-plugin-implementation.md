# ADR-0007: OpenClaw SDK推奨実装方針

- Status: Accepted
- Date: 2026-04-23
- Sources:
  - https://docs.openclaw.ai/plugins/sdk-overview
  - https://docs.openclaw.ai/plugins/sdk-entrypoints
  - https://docs.openclaw.ai/plugins/sdk-runtime
  - https://docs.openclaw.ai/plugins/sdk-agent-harness
  - https://docs.openclaw.ai/plugins/sdk-setup
  - https://docs.openclaw.ai/plugins/sdk-testing
  - https://docs.openclaw.ai/plugins/manifest
  - https://docs.openclaw.ai/plugins/architecture
  - https://docs.openclaw.ai/plugins/building-plugins

## Context

`video-library-pipeline` はnative OpenClaw pluginであり、主なsurfaceは agent tools、hooks、CLI、gateway method、skillsである。channel plugin、provider plugin、agent harness pluginではない。

OpenClawのplugin systemは、manifest/discovery、enablement/validation、runtime loading、surface consumptionを分離している。つまり、設定検証や発見は可能な限りmanifest/schemaだけで完結し、runtime behaviorはplugin moduleの `register(api)` で登録する。

このリポジトリでは、READMEをADRに分割したため、OpenClaw公式SDKドキュメントに沿った実装判断もADRとして固定しておく。

## Decision

### 1. Entry pointは `definePluginEntry` を使う

このプラグインはchannelではないため、entry pointは `openclaw/plugin-sdk/plugin-entry` の `definePluginEntry` を使う。

```ts
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
```

`index.ts` のdefault exportは以下を満たす。

- `id` は `openclaw.plugin.json` の `id` と一致させる。
- `name` と `description` はmanifestと意味を揃える。
- `register(api)` でtools、hooks、CLI、gateway methodを登録する。
- channel固有の `defineChannelPluginEntry` や `defineSetupPluginEntry` は使わない。

このプラグインのshapeは「tools/hooks/CLI/gatewayを持つnon-capability plugin」として扱う。provider、channel、memory、context-engineなどのexclusive slotには登録しない。

### 2. SDK importはsubpath限定にする

OpenClaw SDKはroot barrelではなく、目的別subpathからimportする。

許可する代表例:

- `openclaw/plugin-sdk/plugin-entry`
- `openclaw/plugin-sdk/runtime-store`
- `openclaw/plugin-sdk/testing`
- `openclaw/plugin-sdk/zod`
- 必要になった場合の中立的なcapability/runtime subpath

禁止するもの:

- `openclaw/plugin-sdk` root import
- OpenClaw本体の `src/` 直接import
- 自分自身を `openclaw/plugin-sdk/<plugin-name>` 経由でimportするself-import
- 他plugin固有のSDK subpathに依存するplugin間結合

plugin内部の共有surfaceは、ローカルの `src/plugin/*`、`src/platform/*`、`src/contracts/*`、`src/core/*` へ置く。外部pluginと本当に共有すべき抽象が出た場合だけ、中立的なSDK subpathへ昇格する前提で設計する。

### 3. Manifestを静的契約として維持する

native OpenClaw pluginはplugin rootに `openclaw.plugin.json` を持つ。manifestはplugin codeを実行する前に読まれる静的契約であり、設定検証、依存plugin、skills、UI hintsをここに置く。

このリポジトリでは以下を維持する。

- `id: "video-library-pipeline"` をentry pointの `pluginId` と一致させる。
- `requires.plugins` で `czkawka-cli` 依存を宣言する。
- `skills` に同梱skill rootを列挙する。
- `configSchema` にruntime設定の型制約を置く。
- `uiHints` は設定UI向けの補助情報に留め、実行ロジックのsource of truthにはしない。

新規configを追加するときは、manifest schema、TypeScript config resolver、validate tool、docsを同時に更新する。可能な範囲で `additionalProperties: false` を使い、未知フィールドを意図せず受け入れない。

### 4. `package.json` の `openclaw` metadataを維持する

`package.json` の `openclaw.extensions` はplugin entry pointを指す。外部配布を意識する場合は、`compat.pluginApi`、`compat.minGatewayVersion`、`build.openclawVersion`、`build.pluginSdkVersion` を更新し、実際に検証したOpenClaw SDK/Gateway範囲を表す。

plugin依存を追加する場合は、plugin directory内で解決できる依存にする。postinstall buildを前提にするnative dependencyは避け、追加が必要な場合は `DEPENDENCIES.md` と `video_pipeline_validate` のpreflightに反映する。

### 5. Runtime accessは `api` 経由にする

OpenClaw runtimeへのアクセスは `register(api)` に注入される `api` と `api.runtime` を経由する。host内部moduleやglobal stateへ直接依存しない。

使い分け:

- `api.pluginConfig`: このplugin専用の設定入力。
- `api.config`: OpenClaw全体の現在config snapshot。
- `api.logger`: plugin scoped logging。
- `api.runtime.config`: config load/writeが必要な場合。
- `api.runtime.system`: system command、heartbeat、native dependency hintが必要な場合。
- `api.runtime.subagent`: plugin内部からbackground subagentを直接管理する場合。

`register(api)` の外でruntimeが必要な設計になった場合は、`createPluginRuntimeStore` を使う。runtime storeを使うコードは、テストでmock runtimeをset/clearする。

このリポジトリのPython/PowerShell実行境界はADR-0003の通り維持する。OpenClaw runtime helperはOpenClaw統合のための境界であり、Windowsファイル操作を直接OpenClaw SDKへ移す意図ではない。

### 6. Gateway methodとCLIはplugin-owned namespaceへ閉じる

gateway methodは `${pluginId}.*` prefixを使う。OpenClaw core admin namespace、特に `config.*`、`exec.approvals.*`、`wizard.*`、`update.*` は使わない。

CLIを登録する場合は、通常runtimeでの互換登録とroot helpのlazy metadataを区別する。root commandをpluginが所有し、OpenClawのroot helpでlazy-load可能にしたい場合は、CLI descriptorを使う設計を検討する。

### 7. Setup entryは現時点では作らない

`setup-entry.ts` は、channel pluginのonboarding、disabled/unconfigured channel inspection、deferred full loadで重いruntimeを避けるための軽量entryである。

このプラグインはchannel pluginではなく、現時点でsetup-only surfaceを持たないため、`setupEntry` は追加しない。

将来、channelまたはoptional install setup surfaceを追加する場合のみ、以下を行う。

- `package.json` の `openclaw.setupEntry` を追加する。
- setup entryには重いruntime code、CLI registration、background serviceを入れない。
- full `index.ts` にruntime-only registrationを残す。
- setup-safe helperは `openclaw/plugin-sdk/setup-runtime`、`channel-setup`、`setup-tools` などの狭いsubpathからimportする。

### 8. Agent harnessは採用しない

Agent harnessは、prepared OpenClaw agent turnを低レベルに実行するtrusted native plugin向けsurfaceである。新しいLLM APIの追加、通常のtool registry、channel delivery、provider selectionの代替としては使わない。

このプラグインのLLM抽出は、ファイル名メタデータ抽出のworkflowであり、OpenClaw agent turn executorではない。そのため `registerAgentHarness` は使わない。

LLM抽出を高度化する場合の優先順は以下とする。

1. 既存の `followUpToolCalls` による明示的なtool orchestrationを維持する。
2. plugin内部でsubagentを直接管理する必要がある場合だけ `api.runtime.subagent` を検討する。
3. native coding-agent daemonのようにOpenClawのprovider abstractionでは表現できないexecutorが必要になった場合のみ、agent harnessを別pluginとして検討する。

### 9. Tools/hooks/servicesはimport side effectを避けて登録する

tool moduleは `registerToolX(api, getCfg)` 形式を維持し、import時にruntime side effectを起こさない。重い処理、DB access、filesystem access、Python/PowerShell起動はtool invocation時に行う。

hookを追加・変更する場合は、OpenClaw hook decision semanticsを守る。

- `before_tool_call` / `before_install` の `block: true` はterminal decision。
- `block: false` は明示許可ではなくno decisionとして扱う。
- `reply_dispatch` の `handled: true` はterminal decision。
- `message_sending` の `cancel: true` はterminal decision。

### 10. Testing方針はADR-0001に従う

OpenClaw plugin layerのテストはVitestを標準とし、SDK testing helperは `openclaw/plugin-sdk/testing` からimportする。runtime依存はmock runtimeを明示的にset/clearし、stubはprototype mutationではなくper-instanceに置く。

Python実行層は引き続きpytestで検証する。OpenClaw SDK testingの詳細はADR-0001をsource of truthとする。

## Consequences

- このプラグインはOpenClaw SDKの推奨通り、specific subpath import、manifest-first validation、`register(api)` runtime registrationを基本にする。
- setup entry、channel entry、agent harnessは現状のplugin shapeには不要であり、追加する場合は別ADRまたはこのADRの改訂が必要。
- manifest、entry point、package metadata、config resolver、validate toolの間に不整合を作らないことが今後のレビュー基準になる。
- `register(api: any)` の型は将来的に `OpenClawPluginApi` 相当へ狭める余地があるが、本ADRではコード変更しない。
