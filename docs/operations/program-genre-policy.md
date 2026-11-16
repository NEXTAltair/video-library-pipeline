---
type: runbook
title: 番組単位のgenreレビューと確認者の出所
description: 回別分類の分裂をレビュー用提案へまとめ、日時と既存配置を保つ。
tags: [metadata, genre, review]
timestamp: 2026-11-16T12:49:00+09:00
status: active
---

# 番組単位のgenreレビュー

同じ正規化題名を1つの番組として扱う。`alias_key` を再利用し、異なる題名を
結ぶ場合は公式番組ページなどの根拠つきaliasを明示する。日時、回番号、subtitle、
franchise、既存シリーズroot/layoutは変更しない。外部依存の追加は不要。

## 判定

- 明示規則を最優先。規則・alias・公式分類には `reference` と `note` が必要。
- 規則がなければ回別metadata genreを票として集計する。元のEPG genre一覧は保持する。
- 最大票の比率が全行の `3/5` 以上、次点との差が `2` 以上なら合意。
  未知genreも分母に含む。これは確率や公的基準ではなく保守的な運用閾値。
  `4:3`、`2:1`、`1:0`は保留、`4:1`、`5:2`、`64:37`は候補となる。
  合流後モーサテの `64:36:1`を扱いつつダークサイドの `4:2:1`を拮抗として残す。
- 正配置側から別genreへ変更するときは番組多数派の一致と、実際に確認した公式分類
  (`official_classifications.<title>.checked: true`) の非矛盾が必要。
  URLを書いただけ、未確認、分類が違う場合は止める。
- B/defaultからの分類は題名単位の合意を使える。個別の本人承認は
  `approved_source_changes.<path_id>` の根拠で表し、他の行へ広げない。
- 映画、Godzilla、L五十音、zeroの保留は別。genreを根拠に解除しない。
- 未確定の番組は `held_rows`、対象ジャンルと票・根拠は `programs` に残す。

## 非適用の準備

```sh
./scripts/uv-project run python py/prepare_program_genre_review.py \
  --input <complete-metadata-cohort.jsonl> --policy <evidence-policy.json> \
  --report <new-report.json> --review-yaml <new-draft.yaml>
```

入力はJSONLか `rows` 配列を持つJSON。metadata `genre`、`source_genre`
(`default/news/documentary/variety/anime/drama/movie/tokusatsu`)、path_id、
program_titleを使う。配置が不明ならdefaultと推測せず保留する。
監査reportの `metadata_genre` も読めるが、古いreportはfresh計画の代用にしない。

```json
{
  "aliases": [{"canonical_title":"モーサテ", "aliases":["Newsモーニングサテライト"],
    "reference":"公式番組URL", "note":"同一番組の表記である根拠"}],
  "explicit_rules": {"番組題名": {"genre":"news", "reference":"元指示/公式URL", "note":"分類理由"}},
  "official_classifications": {"番組題名": {"genre":"news", "checked":true,
    "reference":"実際に確認した公式URL", "note":"確認できた分類の引用"}}
}
```

任意の `--review-template <exact-current-run-review.yaml>` を与えると、その
source_jsonl内のpath_idに限定して既存hints/cohort/sourceを保ったコピーを作る。
独立した既存row_overrideと衝突すれば停止する。入力・既存出力の上書きはしない。
このコピーそのものはrun artifactではない。内容を確認後、対象runの返された
review YAMLへeligibleなrow_overridesだけを反映し、返されたV2 resumeを使う。
複数batchに分かれていても投票は全番組cohortで行い、各draftへ該当IDだけを移す。
heldな行の受理や、raw extraction/旧planの書換えは行わない。

## agent受理と本人確認

通常レビューの既定値は `agent_accepted`。本人の実際の確認記録があるときだけ
human confirmationとして保存する。既存のsource名やbooleanは人の確認の証拠ではない。
証拠限定の移行CLIと手順は同梱の `py/migrate_review_provenance.py --help` を参照。

DBの変更とファイル移動は別操作。レビュー受理後はfresh relocate previewを検収し、
既存シリーズ外向きや映画・五十音の保留が混ざっていないことを確かめる。

新規受理は全batchをまとめた同一正規化題名のgenre分裂をDB書込前に検査する。
upsertの既存値merge/EPG解決後も、transaction内の実保存値を検査してからcommitする。
既存のcommit receiptの再生は元の契約を保ち、この新検査で過去の受理をやり直さない。
共有upsert境界でもhuman sourceには `human_confirmed` markerと
`human_confirmation: {confirmed_by, evidence}` が必要。source文字列だけでは確定しない。

既存値を今回のrunで実際に再レビューした場合は、検証済みrun/digestを伴う正規の
受理が現在値を `agent_accepted` で更新できる。以前のsource/human flag/確認記録は
`source_history` の `review_superseded` に保存し、以前の確認者を推定しない。
これは旧記録の一括移行ではない。一般upsert/CLIからsource文字列だけでhuman値を
上書きする経路は拒否する。1918件のような証拠済み旧受理だけを移行したい場合は
別の証拠限定migrationを使い、unknownな旧記録を移行対象へ広げない。

1つのmetadata gateはartifact集合全体を受理する契約で、held行を抜いて受理する
操作ではない。gate内に保留番組が混在する場合、source/cohortを書き換えて縮めず、
合意済み番組だけを `programTitles` に指定したfresh relocate runで正規レビューを
準備する。別表記を合流する番組は両方の題名を指定し、全cohortの投票結果を使う。
旧2365計画や今回のdraftを直接applyする手順ではない。
