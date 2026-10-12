# 2026-10-10 M0実装・検証報告

- Scope: 監査F01/F02/F03とM0a-M0d。F04の同期実行とF08のexit/JSON境界も修正。
- Contract: [ADR-0012](../adr/0012-m0-integrity-boundaries.md)
- Full design: [ADR-0011](../adr/0011-unified-application-kernel-and-recoverable-operations.md)はProposedのまま。
- Runtime activation: **適用済み**。`restartRequired:false`、稼働captureの重要21ファイル一致とfresh public statusを確認。machine固有のraw receiptは非公開で保持する。

## 修正内容

| 元の問題 | 実装した境界 |
|---|---|
| run検証前にTSがmetadataを反映 | Pythonの共通preflight -> snapshot -> 1 transaction -> gate -> plan |
| 後半YAMLの失敗で前半だけ反映 | 全batchの検証と同一transaction。SQL故障も全rollback |
| DB commit後の応答喪失 | DB内commit receipt。同じ受理digestなら再送でDB適用を再実行しない |
| review archive renameでartifact参照が切れる | draft/元JSONLを保持。受理bytesは別のcontent-addressed固定版 |
| raw copy backupがWALを欠落 | 共通online snapshot、integrity、独立DELETE-mode、fsync、一意名 |
| 参照中backupの件数rotate / 本体欠損でもapply継続 | rotateを保護no-opへ。本体存在+integrityなしでnative開始しない |
| 並行manifest更新の欠落 | ops writer scope + store RMW排他 + stale direct writeのrevision conflict |
| Gateway spawnSync / nonzero JSON ok | async child、deadline、output cap、process/JSON shape両方の判定 |
| helper stdout log混入 / false payloadでCLI exit0 | logはstderr、stdoutはJSON1個、falseなら非0 |
| metadata成功後plan失敗の曖昧な表示 | `metadataCommitted:true`と実際のblocked状態。再反映を案内しない |

## 実施した検証

| 検査 | 観測結果 |
|---|---|
| Python full suite | **335 passed**、31.22s |
| TypeScript full suite | **126 passed**、12 test files |
| `pnpm run check` | build + bundled JS syntax check成功 |
| Hermetic audit probe v2 | WALのsource1/backup1、artifact2件成功/2件保持、exit0 |
| Windows Bドライブの隔離fixture | WSL経由のSQLite/lock/review回帰31件成功、118.84s |
| Package dry-run | 129 entries。必要application/platform modulesあり。private data混入0 |
| Fresh unpacked package Python smoke | empty statusはexit0/JSON、invalid resumeはexit1/JSON |
| `git diff --check` | 成功 |

Bドライブ試験は本番DB・録画を使わず、独立した一時領域のsynthetic fixturesだけを使用した。
この31件の後に追加したprocess-exit、helper-log、
同時resume、plan失敗の試験はLinux full suiteの335件に含まれる。
Windows native moverやmsvcrtとWSLの混在排他を検証したという意味ではない。

故障/競合の主な検証:

- WAL/DELETEのopen接続、並行transaction commit中のsnapshot、同秒12backup、一意destination。
- thread/processで24artifact登録が全件残る。stale direct writeは原本不変のconflict。
- 2つの同時resumeは1つだけがmetadata副作用を所有する。
- lock所有processを終了しても後続writer/maintenanceが再開できる。
- 16種のpreflight拒否でDB/manifest/draft変更0。backupや固定版も作らない。
- 2番目のSQL INSERTをtriggerで失敗させ、前半・receipt・schema変更がrollbackする。
- DB commit直後に子processを`os._exit`し、再送後のDB内容とbackup数が変わらない。
- commit後のplan失敗ではDB適用を成功済みとして保持し、gateを再承認しない。
- actual async childのevent-loop応答、Unicode、nonzero、timeout、output overflow、missing executable。
- nonzero+JSON ok、必須shape不足、stdout log混入をTS adapterでfailureにする。

## 環境と証跡

- OpenClaw checkout package version: 2026.9.8 (custom local checkout)。これは宣言した互換範囲全hostの試験ではない。
- Node: v24.18.0、pnpm: 12.4.2、Python: 3.10.20。既存toolchain pinを変更していない。
- 変更前working tree、package検査、fresh package smoke、Windows fixtureのstdout/stderrを非公開検証領域へ保存。
- Durable source/activation receiptはplugin外のruntime stateへ保存。個人設定やraw statusを公開source/packageへ含めない。

一時packageは最初のpack検証用であり、その後のplan失敗診断と最終sourceの一致は
durable verification receiptに記録したfinal pack/source hashesで確認する。

## 稼働版への反映

- 初回の再読込は旧instance cleanupと、checkout内の開発用`.venv/bin/python`が
  package外を指すため失敗。成功扱いせず、ログと読込境界を確認した。
- `.venv`を削除せずdurable receipt領域の`development-venv.before-activation`へ退避。
  uvの実行環境は`~/.openclaw/plugin-runtime/video-library-pipeline/.venv`を使う。
- 境界の修正後、当該pluginだけの再読込が成功。`restartRequired:false`。
- 適用時source digestとoperation receiptは非公開検証領域に保持。
- 継続中のturnは旧callbackを保持し得るため、そのturnのstatus応答だけでは版を証明しない。
  fresh Gateway `tools.invoke`で公開`video_pipeline_status(limit=1)`を呼び、
  新captureの実使用とsource hashes **21/21一致**を確認した。
- 旧instanceのcleanup timeout警告はapplied receiptにも残る。隠さず保存したが、
  新版の適用とread-only status成功は別の証跡で確認済み。
- Gateway PIDとサービス開始時刻は前後で不変。別セッションを中断せず、
  Gatewayを再起動していない。本番DB反映・録画移動・既存gate承認も実施していない。

詳細はdurable領域の`verification.json`、`reload-after-env-repair.json`、
`capture-sha256-verification.json`、`fresh-public-status.json`に保存した。

## 完了範囲と残り

M0の修正単位を実装した。A01-A06に対応する回帰を通したが、M2のgrant/review revision、
M3のnative排他・lease/journal/reconciler、M1の統合照会・pagination、M4/M5の統合運用/UIは未実装。
全23 acceptance cases、全host互換、power-lossやWindows子processの回復を合格扱いしない。
任意SQLやhidden V1 adminの全排他を追加したという意味でもない。

このM0検証時点では既存の未コミット変更を保持。録画の移動/削除、本番DBの反映/migration、既存gate承認、
Gateway restart、git commit/push、registry publicationは実施していない。
runtime更新は当該pluginの再読込だけ。applied receipt、hash照合、fresh read-only public statusで完了を確認した。
その後の公開単位は[v0.4.0](../releases/v0.4.0.md)を参照。
