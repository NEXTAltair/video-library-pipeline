# 2026-10-10 次期設計の移行計画

- Status: M0 implemented; M1-M5 proposed implementation backlog
- Design: [ADR-0011](../adr/0011-unified-application-kernel-and-recoverable-operations.md)
- Evidence: [全体設計監査](2026-10-10-design-audit.md)

## 1. 進め方

既存実装を残して共通基盤へ入口を移す。DB/録画の一括移行はしない。
各段階は独立に検証・reviewできる変更単位に分ける。
最初の実装単位はM0。後段のUIやdedup公開を先行させない。
M0の実装済み境界は [ADR-0012](../adr/0012-m0-integrity-boundaries.md) と
[実装報告](2026-10-10-m0-implementation.md)を参照。M1-M5は未実装。
全23条件が合格したという意味ではない。M2のgrant・M3のnative復旧は未実装。
現在のtool名を変更する指示ではない。

| 段階 | 具体的な成果 | 主な変更先 | 退出条件 |
|---|---|---|---|
| M0 整合性の穴を閉じる | metadata preflightと所属検査、online backup、writer排他、exit判定 | tool-workflows、workflow_cli、workflows/store、backup_mediaops_db | A01-A06。各既存入口が同じ検証を使う |
| M1 共通照会 | V2/dedup/maintenanceのread-only adapters、coverage、ページ付き一覧 | application/queries、platform/legacy、contracts | A07-A09。旧原本・録画を変更せず総数が一致 |
| M2 kernelとreviewを統合 | versioned schema、command registry、immutable review revision、grant、CAS control store | contracts、application、platform/control_store | A01-A04、A10-A11。adapterからdomain writeがなくなる |
| M3 durable実行 | jobs/attempts、非同期SDK adapter、native排他、journal、reconciler、service lifecycle | jobs、operations、native executor、plugin service | A12-A16。中断・再送・reloadで重複操作しない |
| M4 各運用を一本化 | source_root/relocate/dedup/restore/maintenanceを同じkernelで実行、旧CLIは委譲 | application/flows、CLI、skills | A17-A19。hold、identity、旧復元記録を維持 |
| M5 表示・配布を仕上げる | チャット照会、軽いexport、任意UI、doctor、SDK互換範囲、古い入口整理 | plugin/UI、presentation、package、docs | A20-A23。現物の配信・操作とpackaged installで検証 |

## 2. M0をreview可能な変更へ分解する

| 変更単位 | 作業 | 守る条件 |
|---|---|---|
| M0a backup | 全呼び出しが共通online snapshotを使う。存在・整合性を確認。参照付き保持へ移るまで参照不明のbackupをrotateしない | live restoreを自動で行わない。WAL/DELETE両方のfixture |
| M0b metadata | run所属draftの解決・前提検証をPythonの共通入口へ移す。反映とresumeの順序を一元化。複数reviewは1 transaction | 誤run/wrong phase/closed gateでDB write=0。編集draftと不変原本を区別 |
| M0c concurrency | 全writerの一覧を取り、store read-modify-writeとmetadata applyの排他を共通化。再入可能なscopeを設計 | V2/CLI/maintenance間の競合も試験。lock待ちをGatewayで同期blockしない |
| M0d response | nonzero exit、壊れたJSON、stdout log混入、timeoutを一貫した失敗に変換 | 成功に見えるpartial resultを返さない |

M0cは将来のSQLite制御DBを先に全面導入する必要はない。
ただしlockの対象をwrite_runだけにしては不十分で、read-modify-writeと副作用の全区間を守る。
他プロセスの同じstore操作も対象にする。

## 3. モジュール配置の目標

```text
contracts/                       # versioned JSON schema / shared fixtures
src/plugin/                      # SDK registration, service, CLI
src/adapters/                    # tools, gateway queries, protocol client
src/presentation/                # small reports / optional UI
py/video_pipeline/domain/        # existing pure policies
py/video_pipeline/application/   # commands, queries, flow orchestration
py/video_pipeline/jobs/           # claim, attempt, scheduling policy
py/video_pipeline/operations/     # journal, apply, reconcile, compensate
py/video_pipeline/platform/      # control store, media DB, native bridge, artifacts
templates/windows-scripts/       # one native script source
```

これは移行先の責務であり、今すぐディレクトリだけをrenameする計画ではない。
各モジュールのpublic seamを作り、利用している経路から置換する。
互換shimを削除する前にCLI、skill、test、packaged plugin、ローカル定期実行の参照を確認する。

## 4. Critical acceptance cases

| ID | 条件 | 合格基準 |
|---|---|---|
| A01 | 存在しないrun / wrong flow / wrong phaseでreview apply | DB、draft、gate、物理ファイルの変更0。具体的な拒否理由 |
| A02 | 別runのartifact、closed gate、古いrevision、grantと違うdigest | 書き込み前に拒否。勝手に新しい版を承認しない |
| A03 | 2つのmetadata YAMLの後半でvalidation/write失敗 | media DB transaction全体rollback。前半だけ成功にしない |
| A04 | 同時resume、同時artifact登録、CLIとの競合 | 1つだけが副作用claim。全登録が保存されるか明示的conflict。lost updateなし |
| A05 | WAL/DELETE、並行commit中のsnapshot、backup名重複 | online snapshotとしてintegrityと必要な内容が一致。別IDで保存 |
| A06 | backupが参照中 / 本体欠損 / nonzero exitなのにJSON ok | 参照中をrotateしない。欠損でapply停止。exit矛盾はfailure |
| A07 | V2 / dedup / maintenance / 旧未解決gateの照会 | 原本checksum不変。全flowが一覧に出る。古いactiveをrunningと表示しない |
| A08 | 10万件の合成recordと途中のデータ更新 | cursor、応答予算、snapshot整合性を守り、countsは全体値。更新時の再取得が明示的 |
| A09 | 0候補 / holdあり / offline root / scan途中停止 | 対象範囲と未知coverageを残す。「全体整理済み」と誤表示しない |
| A10 | schema version未知、必須項目不足、action schemaと不一致 | TS/Python両端でfail closed。自由文fallbackで副作用を開始しない |
| A11 | actor偽装、任意review path、scope拡大した再生成plan | 信頼済みcontextとartifact参照で処理。承認範囲を広げない |
| A12 | intent後 / native move後 / receipt後 / DB commit後にkill | 各地点を照合して再開。1つのoperationを二重移動・二重DB適用しない |
| A13 | lease切れ、旧native子processがまだ実行中 | 新writerを入れない。process/OS排他を確認し、照合する |
| A14 | cross-volume copy途中 / sourceとdestination両方残存 | checkpointと署名で残りを処理。成功済みと未完了を説明できる |
| A15 | cancel、service reload/stop、root切断、通知送信中の停止 | 新操作を止め、結果とoutboxを回収。孤立worker/無言の成功なし |
| A16 | 長いjob実行中の別status問い合わせ | Gatewayは応答する。bounded log、deadline、job progressが利用可能 |
| A17 | unknown放送種別、hold、別season、字幕版、短い録画、plan後のrouting変更 | 既存分類・除外を維持。根拠なしで隔離せず、旧grantの宛先を変えない |
| A18 | 同名/大小文字/Unicode/長パス/reparse/宛先衝突 | Windows nativeで検査。既存destinationを上書きせず、安全に保留 |
| A19 | 旧exact-run復元と、その後の別runの変更 | 復元対象以外を変えない。後続DB更新を古い全体backupで消さない |
| A20 | 大きい一覧、session境界外export、機能なしhost | size/path/権限を事前確認。許可された配信経路で実際に取得・検索できる |
| A21 | UI lab無効 / 有効、未認証route、権限不足 | lab無効でもchat/CLIが使える。UI dataはgateway auth。表示操作は副作用0 |
| A22 | pack済みpluginを宣言する各対応hostへinstall | SDK型/manifest/CLI/tool/protocol/service試験通過。runtime/private data混入0 |
| A23 | 文書とCLI help、skill、capabilityが不一致 | current/proposedを区別し、存在しない通常手順を案内しない |

上限・応答予算は次期設計の目標であり、現行の達成値ではない。
長パス・Windows排他・service lifecycleはmockだけで合格にしない。
物理試験は隔離した合成Windows領域を使い、本番録画で故障注入しない。

## 5. テストと配布の基準

- Domain unit: 判定条件と例外を検証。実装を写しただけのassertは増やさない。
- Contract: version付きschema、TS/Python golden fixture、非zero exit、未知action。
- Integration: control/media DB、競合、snapshot、故障注入、操作journal。
- Windows: Unicode/長パス/衝突/跨volume/checkpointと排他の実物試験。
- Delivery: 実際のfile RPC、認証付きquery、chat/dashboard frameでの検索・操作。
- Package: typecheck、build、package allowlist、宣言したSDK hostでinstall/CLI/tool smoke。

各releaseの証拠にはsource revision、host/SDK/Python/PowerShell version、fixture、実行command、結果を残す。
テスト通過総数だけをrelease判定にしない。既存testが間違った順序を固定している場合は先に契約を修正する。

## 6. 段階切替とrollback

1. M1で旧状態を読取専用集計し、件数・checksum・gate・artifact refsを照合する。
2. 新runだけ新kernelへ送るfeature flagを導入する。原本とlegacy adapterを保持する。
3. 既存runの書き込み移管は1件ずつrevision/digest/参照を検証し、ownershipを明示する。
   新旧が同じrunを書かない。未解決operationがあるrunは先に照合する。
4. 移管後のrollbackは新workerの受付停止とdrain、制御状態の保全、読取専用legacy照会への切替。
   新形式runを理解できない旧writerへ戻して再実行しない。
5. schemaはadditiveに移行し、破壊的整理は互換期間とバックアップ・復旧試験後の別変更とする。

control storeはsnapshot/exportを用意し、バックアップを戻す前に未解決receiptとの時点差を検査する。
古いsnapshotへ戻しただけでは物理状態を巻き戻せない。
録画・quarantine_v3・legacy receiptの削除は、この移行のrollbackやcleanupに含めない。

## 7. 保留せず決めたこと / 実機で詰めること

| 決定済みの提案 | 実装時に証拠で確定すること |
|---|---|
| Pythonが業務と副作用の順序を所有 | 新旧writerを跨ぐWindows native排他の方式 |
| plugin所有のcontrol DB、media DBは維持 | control DBのexact schema、migration runner |
| 4 tool名維持、CLIは同じkernelへ | 対応hostの下限とservice APIの実際の互換範囲 |
| 表示を配置から分離、query pagination | UI labあり/なしの最適な表示入口 |
| 永久削除と全面rewriteは対象外 | 旧run書込移管の対象・参照audit結果 |

ここに残した実装判断は、調査を行わず利用者へ選択を丸投げするための保留ではない。
M0から順番に実証し、変更単位ごとに現行仕様とADR statusを更新する。
