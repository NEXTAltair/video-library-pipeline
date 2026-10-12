# ADR-0012: M0の整合性境界

- Status: Accepted
- Date: 2026-10-10
- Scope: [ADR-0011](0011-unified-application-kernel-and-recoverable-operations.md)移行のM0。M1-M5の採用・完成を意味しない。

## Decision

### 1. Reviewed metadataはPythonが検証・反映・resumeの順序を所有する

公開`video_pipeline_resume`はlegacy TS metadata toolを先に呼ばない。
`workflow_cli.py`がrun、flow、phase、open metadata gate、artifact所属、
review path、元抽出のchecksum、cohort、DB所属を検証してから反映する。
sourceRoot/relocateの直接service入口も同じ`application/reviewed_metadata.py`を使う。

YAMLは編集可能なdraft。登録時のYAML checksumを編集禁止条件にしない。
元JSONLは不変であり、source_jsonlを別のrun/ファイルへ付け替えることは拒否する。
受理したYAML/元JSONLの正確なbytesは`review/accepted/<digest>/`へ保存する。
元のdraft/JSONLをarchiveへrenameしないので、artifact参照を壊さない。

複数batchを1つの`BEGIN IMMEDIATE`で反映する。既存値とのmergeもtransaction内。
後半のvalidation/SQL失敗は全rollbackし、gate/phaseを変更しない。
成功時だけgateを閉じてplanを再計算する。plan失敗はDB反映失敗とは区別する。

commit receiptをmedia DBの`workflow_metadata_commits`へ同じtransactionで保存する。
DB commit後、gate更新前にcallerが停止した場合は、同じdigestの再送で以前の結果を返す。
違うdigestを自動で再承認しない。このreceiptはmetadataだけの回復境界であり、
native移動のjournal/reconcilerや全jobのdurable実行はM3へ残る。

`gateId`/`artifactIds`/`reviewYamlPaths`は、指定時はrunのopen metadata gate全体と一致させる。
省略時もrun所属refsのみを解決する。任意パス・latest推測へのfallbackはない。
`runRevision`と`reviewDigests`は省略可能な追加のCAS/digest拘束。
それらは人間の承認やM2のgrantに代わるものではない。

### 2. SQLite backupは共通online snapshot

`platform/sqlite_snapshot.py`をbackup utility、daily preview、history repair、
schema migration、dedup apply/restoreで使用する。WALを読めるreadonly接続から
backup APIでcloneし、`integrity_check`、fsync、standalone DELETE-modeを確認する。
sourceのjournal modeは変更しない。既存destinationを上書きしない。

backup utilityの名前は日時+descriptor+UUID。失敗した出力は`.failed_<id>`へ保持し、
backup一覧には載せない。WAL/SHM/journal sidecarもsnapshotとして数えない。
relocateはsuccess payloadだけで進まず、snapshot本体の存在とintegrityを確認する。

`rotate_backups`は互換no-opにする。参照不明の古いbackupを件数で削除しない。
参照付きの保持・可逆整理は後段で実装する。元DBのwhole-snapshot restoreは
独立した明示的admin操作であり、exact-run録画復元へ転用しない。

### 3. Writer scopeとrevision

`<ops>/.workflow-writer.lock`を全V2 serviceのdry_run/resume/apply、全store mutation、
daily、dedup/date-dedupの公開functionとCLIで共有する。
`<ops>/artifacts/job`の作成/完了/maintenanceもこのops scopeを取る。
旧任意artifact treeは親directoryのscopeを使い、archive対象treeの中へlockを追加しない。

scopeはthread/process間排他で、同一threadの入れ子は再入可能。OS lockのinodeを
unlinkしない。read-modify-writeとnative副作用の全区間を覆う。
read-only statusはlockを待たず、atomic replaceされたmanifestを読む。

manifestにadditiveな`revision`を保存し、旧manifestの欠損値は0とする。
direct `read_run` -> `write_run`のstale snapshotはconflictで拒否する。
全writerはstore mutationメソッド、または同じwriter scopeを使用する。

この排他は現行WSL Python writersの契約。任意の外部SQL、旧コードを保持した既存process、
Pythonと別起動Windows native process間のOS共通排他を保証するものではない。
schema/data migrationや旧V1 adminスクリプトは通常workflowではなく、
SQLite transactionを使う独立したadmin入口。native子processのkill後照合はM3で扱う。

### 4. Process/protocolの失敗を成功に見せない

公開4toolは`runCmdAsync`を使う。Gateway event loopで`spawnSync`しない。
既定30分deadline、stdout+stderr合計32MiB上限。POSIXではuv/Pythonのprocess groupを停止する。
timeout/output超過は「処理途中の可能性あり」の失敗で、再実行許可とは扱わない。
Gateway service lifecycleや永続job、Windows native子process回収はM3の未実装範囲。

Python CLIのstdoutはJSON objectを1つだけ。helper logはstderr。
`ok:false`ならCLI exitも非0。review_requiredはWorkflowResultとして保持する。
adapterはnonzero exitの`ok:true`、壊れたJSON、stdout log混入、必須shape不足を拒否する。

## Writer inventory / boundaries

| 入口 | 整合性のowner |
|---|---|
| V2 tool / workflow CLI / SourceRoot・Relocate service | ops scope + store revision。metadataは共通preflight/transaction |
| daily CLI | ops scope。preview DBも共通snapshot |
| dedup/date-dedup make/apply/recover/restore + CLI | 同じops scope。旧identity/hold/receiptを維持 |
| artifact job create/finish / cleanup/restore | canonical namespaceでは同じops scope + maintenance lock |
| standalone metadata upsert CLI | merge/read/writeを1つのSQLite transactionへ移動。V2 run承認入口ではない |
| history repair / schema migration | SQLite transaction + 共通snapshot。通常workflowのwriterとして公開しない |
| その他のhidden V1 DB admin | 独立したSQLite admin入口。M0のops排他やbackup保証へ含めず、通常操作として公開しない |
| raw SQLite / PowerShellを直接起動する外部process | この契約の管理外。M3 native排他/照合の受入試験が必要 |

## Verification

実施結果と環境・配布・activationの証跡は
[M0実装報告](../reviews/2026-10-10-m0-implementation.md)に記録する。
元の監査probeは呼び出し前barrierへ更新し、lock内barrierでdeadlockする計装を撤去した。
不整合を再現した元の観測値は監査文書へ保持する。

## Rollback / Compatibility

公開4tool名、既存run ID、録画identity、hold、quarantine_v3、exact-run復元記録を維持する。
新receipt/revisionはadditive。旧runtimeに戻す場合はまずwriterをdrainし、未照合の
metadata commitとgateのずれを確認する。旧コードの単純再実行をrollbackとは呼ばない。
この実装/検証は本番録画の移動・削除、本番DBのmigration・反映、既存gateの承認を行わない。
