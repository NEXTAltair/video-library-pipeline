# ADR-0010: 作業データの分離と可逆なcache整理

- Status: Accepted
- Date: 2026-10-09

## Context

調査用スクリプトがリポジトリ直下の `artifacts/` に個別フレーム、一覧画像、
試行DB、途中世代のJSONを残し、約97,000ファイルへ増殖した。
通常のV2 run artifactとこの調査データは別物だが、保管区分がなかった。
旧 `rotate_move_audit_logs.py` は未知のJSONLもアーカイブし、古いgzipを期限だけで
永久削除していた。監査・復元に必要な記録へTTLを適用してはいけない。

## Decision

1. 保存先は `windowsOpsRoot/artifacts/<job-id>`。プラグインが作った
   `.artifact-policy.json` が所有権、完了状態、cache保持期間を明示する。
2. 再生成用データは `cache/`、最終記録・根拠画像・実変更前DBバックアップは
   `evidence/`。それ以外も保護し、既存ディレクトリを拡張子だけで再分類しない。
3. 台帳のない旧データはread-onlyで集計できるが、整理対象へ自動登録しない。
   完了済みのcacheのみ、既定7日経過後に計画候補となる。
4. 未完了/失敗/保留workspace、未完了V2 run/open review gate、cacheへの証拠参照、
   symlink/特殊ファイル/hardlink/録画を含むcacheは保留する。
5. 計画は物理署名と全cacheファイルのSHA256に拘束。全候補を再検査後に
   `.trash/<plan-id>/<job-id>/cache` へ同一filesystem内でrenameする。
   公開CLIにpurge操作を作らず、ディスク空きが増えたとは案内しない。
6. 移動前にfsync付きintentを保存。中断後はplan checksumと両側のtree snapshotを
   照合し、復元のpreviewを行える。再applyは禁止、復元時の上書きも禁止する。
7. 日次CLIのプレビューDBを `cache/`、run/plan/結果を `evidence/` に保存する。
   未完了レビューは自動で完了扱いにせず、保留workspaceとして保存する。
8. 旧ログ整理はpreviewを既定とする。`--apply` のlossless gzip変換は
   解凍内容SHA256と原本の不変性を検証してから元の平文を置換する。
   未知のJSONL、V2 runディレクトリ、旧gzipのTTL削除は対象外。
9. 既存の巨大なsource内 `artifacts/` は削除・書換えずに丸ごとsource外へ移設できる。
   OpenClawのcaptureが約97,000ファイルを複製する実害を、CLIプロセスの開いたJPEGと
   current sourceのcapture規則で確認した。archive計画/receiptはsource外へ保存し、
   全treeのSHA256・inode・mtime・link文字列を移設前後で一致確認する。
   絶対パスは書き換えず、元の位置への復元はpreviewを既定とする。

## Implementation

- `py/video_pipeline/platform/artifact_retention.py`: 管理workspace、集計、計画、退避、復元
- `py/artifact_cli.py`, `src/plugin/cli.ts`: `video-pipeline-artifacts` 保守CLI
- `py/daily_cli.py`: 日次プレビューのmanaged保存先
- `py/rotate_move_audit_logs.py`: 旧監査ログの無損失アーカイブ
- `py/video_pipeline/platform/legacy_artifact_archive.py`: source内旧データの完全保存移設

V2の4公開tools、録画の隔離/復元、DB hold、レビュー承認の契約は変更しない。
永久削除や過去の重複整理の再実行はこの変更の対象外。
