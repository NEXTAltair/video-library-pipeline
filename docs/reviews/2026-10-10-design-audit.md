# 2026-10-10 全体設計監査

## 結論

主要な弱点はツール数ではなく、**状態・副作用・復旧の責任が入口ごとに分かれていること**。
V2の4ツールは維持し、その下に統合kernel、durable job、操作journal、共通の照会を置く。
設計は[ADR-0011](../adr/0011-unified-application-kernel-and-recoverable-operations.md)、
実施順序は[移行計画](2026-10-10-redesign-roadmap.md)に記録した。

これは次期設計の監査であり、現行操作手順を置き換える文書ではない。
その後のM0修正は[実装・検証報告](2026-10-10-m0-implementation.md)を参照。
以下のコード位置と初回probe観測は、修正前の監査証拠として保持する。
この見直しで本番録画の移動・削除、DB変更、gate承認、Gateway再起動は行っていない。

## 調査範囲と確度

- 現行README/spec index、ADR、公開tool adapter、workflow/CLI、store、backup、native boundary、成果物管理を調査。
- OpenClaw公式docsを先に確認し、現在のSDK sourceを照合した。
- public statusとDB journal modeを読取専用で確認。実ファイルの全数再走査は今回していない。
- 2つの不整合は一時SQLite/一時runだけで再現した。残りはコード経路から確認した問題または設計上の不足。
- legacy全コードの行単位レビュー、Windowsでの新executor試験、実利用者画面の直接確認は未実施。
- 過去のテスト通過数は新設計の保証として流用しない。実装がない新設計に「検証済み」とは付けない。

## 優先順位

P0は次の変更releaseより前に対処するデータ整合性問題、P1は運用の確実さ、P2は保守・契約整備。
P0は本番被害が確認されたという意味ではない。

| ID | 優先 | 観測 / 根拠 | 改善方針 |
|---|---|---|---|
| F01 | P0 | reviewed metadataのDB反映がrun/gate検証より先。コード経路で確認 | kernel内で所属・revision・確定版・grantを先に検証し、反映まで同じ実行境界で所有 |
| F02 | P0 | WALの確定済み1行がbackupに0行。helperは成功を返す。合成DBで再現 | online backup、integrity、参照付き保持、一意backup ID |
| F03 | P0 | 並行登録2件が成功してmanifestには1件だけ。合成runで再現 | transaction/CAS、writer排他。JSON atomic replaceと競合制御を区別 |
| F04 | P1 | Gateway toolでspawnSync。async executeでも呼び出しは同期 | 非同期adapter、durable job、期限・停止・結果回収 |
| F05 | P1 | V2 run、daily/dedup、artifact maintenanceが別々のstate/lock | 共通kernelと操作基盤。旧stateはread-onlyで統合照会 |
| F06 | P1 | blocked/failedがphaseの終端、activeは実行中とは限らない | 業務phase、実行状態、coverageを分離。照合付き再開 |
| F07 | P1 | 大きいHTMLとsession境界で一覧を配信できなかった | query pagination、表示とexportの分離、実配信経路の受入試験 |
| F08 | P2 | TS/Python間のschema・exit判定・SDK型が弱い | version付きschema、型生成/検証、配布パッケージで契約試験 |
| F09 | P2 | config検証がroot存在確認と一体、依存もplugin全体必須 | syntaxと機能別preflightを分離。offlineでも履歴照会可能に |
| F10 | P2 | ADR statusとindex、依存文書とV2動作が不一致 | current / proposed / historicalの区分と参照検査 |

## F01: runを拒否する前にDBが変わり得る

ソース:

- [tool-workflows.ts](../../src/tools/tool-workflows.ts):86, 98, 214, 227。
- [tool-apply-reviewed-metadata.ts](../../src/tools/tool-apply-reviewed-metadata.ts):548, 591。
- [workflow_cli.py](../../py/workflow_cli.py):306。
- [source_root.py](../../py/video_pipeline/workflows/source_root.py):259。
- [relocate.py](../../py/video_pipeline/workflows/relocate.py):1243。

`apply_reviewed_metadata`はcallerのreviewYamlPathsをlegacy内部toolへ渡してDB反映する。
その後Python CLIがrunを読み、flow/phase/open gateを検証する。
gateId/artifactIdsの入力項目はあっても、このDB反映の前提として渡されていない。

legacy toolにもYAML由来、checksum、title等の検査はある。
問題は「検査が一切ない」ことではなく、**そのreviewが指定runの許可された対象かを先に検証していない**こと。
複数YAML反映は個別実行なので、途中失敗時の一括整合性もない。
成功後のYAML/JSONL archive renameは、runのartifact参照を古いパスのままにし得る。

現在の[tool-workflows.test.ts](../../src/tools/tool-workflows.test.ts):278は、この「反映してからresume」の順序を期待している。
テスト数を増やすだけではなく、守るべき境界を修正する必要がある。
本番DBに誤runを渡す再現はしていない。

## F02: backupの成功条件がSQLite snapshotになっていない

[backup_mediaops_db.py](../../py/backup_mediaops_db.py):28, 49はmain DBをcopy2し、主にsize一致で成功判定する。
WALをcheckpointしてから1行をcommitし、接続を開いたままbackupすると、成功応答でもbackup内の行数は0だった。
**元DBから行が消えたのではなく、backupが確定済み行を含まなかった。**

この再現は合成WAL DBによるもの。実運用のWAL障害や実被害と解釈してはいけない。
一方、動作モードや並行書き込みに依存する通常copyを、復旧可能なbackupの共通契約にはできない。

補足:

- rotate_backups(:138)は保持数だけでos.removeし、runからの参照を見ない。
- restore_db(:109)はDB全体copyで、後続runへの影響やlive connectionを処理しない。
- relocate.py:281-322はbackupの参照を登録してからrotateする。
  backup本体がなくてもsummary artifactへ進む分岐がある。
- daily_cli.py:115とrepair_legacy_records.py:136にはonline backupを使う実装が既にある。
  新しい依存を増やさず共通化できる。

## F03: atomicなファイル保存だけでは更新を失う

[store.py](../../py/video_pipeline/workflows/store.py):104, 125はread、snapshotの変更、temp+fsync+replaceで保存する。
途中のJSON破損は避けられても、2つのwriterが同じ版を読んだ時のlost updateを防がない。
再現では2つのread完了位置をbarrierで合わせ、実際のregister_artifactを並行実行した。
両呼び出しは成功し、最後のmanifestはartifact IDを1つだけ保持した。

daily/dedupにはdaily.lock、artifact maintenanceには別のlockがある。
public workflowに同じ排他はない。個別のlockがあることを、全入口の排他と見なしてはいけない。
本番で同じ欠落が発生したとは確認していない。

## F04-F06: 実行中、判断待ち、処理済みを区別できない

- [runtime.ts](../../src/platform/runtime.ts):11, 23はspawnSync。Gateway event loopを同期呼び出し中占有し得る。
  runCmdの経路には共通deadline/cancelがない。CLI自身の同期処理とは別の問題。
- [dedup.py](../../py/video_pipeline/workflows/dedup.py):47と[date_dedup.py](../../py/video_pipeline/workflows/date_dedup.py):23は
  duplicates/runsの別manifestを使う。V2 statusはruns/run.jsonを対象にし、dedup結果は統合されない。
- [models.py](../../py/video_pipeline/workflows/models.py):67はphaseからstatusを作り、
  [state_machine.py](../../py/video_pipeline/workflows/state_machine.py):53はblocked/failedを終端とする。
  期限付き再試行、実行喪失、照合待ち、部分成功を共通契約として表せない。

live statusでは完了run、blocked run、古い判断待ちのgateを区別する必要があった。
`review_required/active`はプロセスが実行中、またはgateが不要という証拠ではない。
調査で既存gateを閉じたり承認したりしていない。raw運用状態は非公開receiptに保持する。

## F07: 「生成できた」と「利用者が開けた」は別

隔離一覧のstandalone HTMLがOpenClaw session previewの上限256 KiBを超えていた。
軽量化で上限内に収めた。さらにruntime領域のfileはsession境界外として拒否され、
許可されたrepo内の小さいexportコピーで実際のsessions.files.getが成功した。

現在の修正はstandalone exportとして有効だが、件数増加・更新・共通集計を担う画面基盤ではない。
次期設計では認証付きqueryでページを取得する。実物のquarantine_v3は平坦化しない。

直前の検証はoffline Chromiumのsandbox frameとlive file RPCまで。
利用者の接続中ブラウザで表示・操作を直接確認したという主張はしない。
host根拠はOpenClaw checkoutの`src/gateway/workspace-file-limits.ts`。

## F08-F10: 契約と文書を実装に追従させる

- [types.ts](../../src/contracts/types.ts)の手製PluginApi/AnyObj、
  tool-workflows.ts:21のJSON parse成功優先、弱いresult shape検査を見直す。
- package.jsonのcheckはbundleとnode構文確認。TSのtypecheckではない。
  `>=2026.4.20`の互換宣言は、その範囲全体の実証と同義ではない。
- [config.ts](../../src/platform/config.ts):65-92はconfigとroot存在検査を一体化している。
  [openclaw.plugin.json](../../openclaw.plugin.json)はczkawka-cliを全体必須としている。
  provider別の必要条件を確認してから分離する。単純に依存宣言を削除する提案ではない。
- ADR-0008本文はProposed、ADR indexはAccepted。DEPENDENCIES.mdには旧tool群、
  relocateのroots必須、旧quarantine path等の記述が残る。
  それを現在の実行ガイドとして使わせない。

## 再現資料

開発用の[合成データprobe](probes/design_audit_probes.py)を保存した。
本番DBや録画rootの入力を受けず、TemporaryDirectory内でのみ動作する。

```sh
python3 docs/reviews/probes/design_audit_probes.py
```

不整合検出時のexit codeは1、検出なしは0、probe自体の失敗は2。
初回probeはread位置を計装した。M0修正後は呼び出し前barrierに置換したversion2で
再確認し、実際のthread/process競合は別の非計装回帰試験でも検証する。
このprobeだけをrelease試験の代わりにはしない。

初回観測:

| Probe | 結果 |
|---|---|
| WAL snapshot | helperSuccess=true、sourceCommittedRows=1、backupRows=0 |
| artifact concurrent register | successfulCalls=2、persistedArtifactIds=1 |

マシン固有のraw runtime資料はplugin-runtimeのdesign-review-20261010に保存している。
配布パッケージへは含めない。

## 設計の完成判定

機能追加数やテスト総数ではなく、**どの入口でも同じ前提で検証し、止まった場所から説明可能に復旧できること**を判定する。
採用前に移行計画のcritical acceptance casesを通し、旧runの照会とexact-run復元を保つ。
