# ADR-0013: relocate apply の分割実行と中断回復

- Status: Accepted (relocate のみ実装)
- Date: 2026-11-11
- Issue: #138
- Builds on: ADR-0008, ADR-0009, ADR-0011

## 背景

relocate の apply は計画全体を1回の同期呼び出しで実行していた。公開 tool の期限(30分)で子プロセスごと
打ち切られると、次の問題が同時に起きた。

- 移動済みの分が DB に反映されない(反映は全件完了後に1回だけ)。
- 移動ログの場所を run が知らない(PowerShell の正常終了後に初めて登録される)。
- ドライブをまたぐ移動の途中で止まると、移動先に未完了のコピーが残る。
- run は `plan_ready` のままで、同じ apply を再び提示する。

## 決定

ADR-0011 の M3(durable job service)全体は待たず、既存の同期呼び出しの形のまま次を入れる。

1. **新 phase `applying`**。`plan_ready → applying → applied/complete/failed/blocked`。
   二重の apply は `relocate_apply_already_started` で拒否する(呼び出しの拒否。run は変えない)。
2. **分割実行**。1回の呼び出しは、チャンク(既定10件)ごとに `apply_move_plan.ps1` を呼ぶ。
   チャンクごとに専用の receipt ログ(`apply/receipts/chunk_NNNN.jsonl`)を Python が決めて渡し、
   終わった直後に `update_db_paths_from_move_apply.py` で DB へ反映する(同じログの再反映は冪等)。
   時間予算(既定20分、公開 tool の期限より十分短い)を超えそうなら `relocate_apply_in_progress` を返し、
   `continue_relocate_apply` を提示する。
3. **durable な状態**。`apply/relocate_apply_state.json` にチャンク一覧と `inFlight` を持つ。
   ファイルを動かす前に `inFlight` を書き、チャンク完了後に消す。死んだ呼び出しは `inFlight` が残ることで
   検出する(writer lock は OS ロックなので、新しい呼び出しが取れた時点で前の持ち主は死んでいる)。
4. **回収 `reconcile_relocate_apply`**。DB 未反映の receipt を反映し、中断チャンクで receipt が無い最初の
   1件(動いていた可能性があるのはその1件だけ)を読み取りのみで調べる。
   - 元だけ存在: 未着手 → そのまま再開。
   - 移動先だけ存在し、サイズが計画の前提と一致: 移動済み → receipt を補って DB へ反映。
   - 両方存在: `settle_interrupted_move` を提示(下記)。
   - どちらも無い、または移動先のサイズが合わない: 判断不能。run は `applying` のまま、診断を残して止める。
5. **`settle_interrupted_move`**(承認を要する)。`settle_interrupted_move.ps1` が SHA-256 で両者を比べ、
   削除の直前に再検証する。完全一致なら元ファイルを削除(移動の完了)、不一致なら移動先の部分コピーを削除。
   それ以外のファイルには触れない。
6. 最終的に全チャンクを1つの `relocate_move_apply_log` にまとめ、従来どおり統計・DB 更新 artifact を登録する。
   移動失敗が1件でもあれば従来どおり `failed`(#139 で別途扱う)。
7. 返却される `nextActions` の定義は `chunked_apply.applying_next_actions` に一本化し、結果生成と
   status 再構築が同じ関数を使う。`status` は `applyProgress`(total/done/failed/pending、最終更新、状態)を返す。
8. 期限や出力上限で打ち切られた resume の応答に `runId` と、status を確認して同じ action を繰り返さない旨の
   `recovery` を含める。

## 見送ったもの

- **apply_move_plan.ps1 の一時名コピー**。dedup など他の呼び出し元と共有するため、変更の影響範囲が広い。
  代わりに中断した1件を事後に検証して片づける(4, 5)。
- **sourceRoot の apply**。同じ構造の問題があるが、今回は relocate だけを実装した。共通の部品
  (`chunked_apply`)はそのまま使える。
- **バックグラウンド実行と進捗の push**(M3 の job service)。呼び出しは引き続き同期で、継続は呼び出し側が
  `continue_relocate_apply` を繰り返す。
- **ネイティブ側の排他**。writer lock は plugin のプロセス間だけを守る。

## 帰結

- 数百件の apply が期限で失われず、途中で死んでも、DB と移動済みファイルの食い違いは
  最大でも「中断した1件」に限られ、回収で解消できる。
- チャンクごとに PowerShell を起動するため、全体の所要時間はわずかに増える。
- 状態ファイルは manifest の外にある。`status` と `reconcile` がそれを読む。
