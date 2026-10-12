"""One flat, read-only view of an existing verified recording handoff.

The catalogue does not move media or replace exact-run restoration. Current
native entries must still match the historical checksum-bound move population.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import gzip
import hashlib
import html
import json
import ntpath
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

from video_pipeline.platform.artifact_retention import read_json, safe_path
from video_pipeline.platform.pathscan_common import windows_to_wsl_path, wsl_to_windows_path
from video_pipeline.platform.windows_pwsh_bridge import find_pwsh7_executable


# Keep standalone HTML compatible with OpenClaw's session-file preview budget.
CATALOG_PREVIEW_MAX_BYTES = 256 * 1024


def path_key(path):
    return ntpath.normpath(path).casefold()


def checksum(path):
    return hashlib.sha256(safe_path(path).read_bytes()).hexdigest()


def verified_population(handoff_path, ops_root):
    handoff_path, ops_root = safe_path(handoff_path), safe_path(ops_root)
    handoff = read_json(handoff_path)
    if handoff.get("ok") is not True or handoff.get("task_scope_complete") is not True:
        raise ValueError("a completed verified-only handoff is required")
    # Whole-source archival preserves bytes, including obsolete absolute paths.
    # Resolve only this exact sibling and only under the original proof checksum.
    proof_path = handoff_path.parent / ntpath.basename(handoff["proof_path"])
    if checksum(proof_path) != handoff["proof_sha256"]:
        raise ValueError("handoff proof checksum changed")
    proof = read_json(proof_path)
    if (proof.get("ok") is not True or proof.get("delete_ready") is not True
            or proof["directories"] != handoff["directories"]):
        raise ValueError("handoff and verified proof differ")
    roots = sorted(handoff["directories"])
    if not roots or any(not re.fullmatch(r"[A-Za-z]:\\_AI_WORK\\duplicates\\quarantine_v3", r) for r in roots):
        raise ValueError("expected exact per-volume quarantine roots")
    expected = {}
    runs_root = ops_root / "duplicates" / "runs"
    for summary in proof["run_summary"]:
        run_id = summary["run_id"]
        if not re.fullmatch(r"[A-Za-z0-9_]+", run_id):
            raise ValueError("invalid exact run ID")
        directory = safe_path(runs_root / run_id)
        manifest = read_json(directory / "manifest.json")
        if (manifest.get("phase") != "complete" or manifest["plan_checksum"] != summary["manifest_checksum"]
                or checksum(directory / "plan.json") != manifest["plan_checksum"]):
            raise ValueError("original run plan changed or incomplete: " + run_id)
        intent = read_json(directory / "intent.json")
        move_path, receipt_path = directory / "move_plan.jsonl", directory / "move_receipts.jsonl"
        if (Path(windows_to_wsl_path(intent["plan_path"])).absolute() != move_path
                or Path(windows_to_wsl_path(intent["receipt_path"])).absolute() != receipt_path
                or intent["run_id"] != run_id or checksum(move_path) != intent["checksum"]):
            raise ValueError("original move intent changed: " + run_id)
        moves = [json.loads(line) for line in safe_path(move_path).read_text(encoding="utf-8-sig").splitlines() if line.strip()]
        receipts = [json.loads(line) for line in safe_path(receipt_path).read_text(encoding="utf-8-sig").splitlines() if line.strip()]
        receipts = [row for row in receipts if row.get("op") == "move"]
        by_pair = {(path_key(row["src"]), path_key(row["dst"])): row for row in moves}
        if not len(by_pair) == len(moves) == len(receipts) == intent["operations"] == summary["moved"]:
            raise ValueError("original move population differs: " + run_id)
        seen = set()
        for receipt in receipts:
            pair = (path_key(receipt["src"]), path_key(receipt["dst"]))
            move = by_pair.get(pair)
            if (move is None or pair in seen or receipt.get("ok") is not True or receipt.get("dry_run")
                    or not move.get("file_id") or receipt.get("file_id") != move["file_id"]):
                raise ValueError("invalid successful receipt: " + run_id)
            seen.add(pair)
            key = path_key(move["dst"])
            matching = [root for root in roots if key.startswith(path_key(root) + "\\")]
            if len(matching) != 1 or key in expected:
                raise ValueError("duplicate or out-of-scope quarantine path")
            expected[key] = {"root": matching[0], "path": move["dst"], "run": run_id, **move["precondition"]}
    if (len(expected) != handoff["verified_quarantine_files"]
            or sum(row["size_bytes"] for row in expected.values()) != handoff["verified_quarantine_bytes"]):
        raise ValueError("handoff population differs from original receipts")
    return handoff, expected


def native_census(roots):
    # Roots are constrained above; encoded JSON is data, never shell code.
    data = base64.b64encode(json.dumps(roots).encode()).decode()
    script = r"""$ErrorActionPreference='Stop';[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false)
$roots=[System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('ROOT_DATA'))|ConvertFrom-Json
$files=[System.Collections.Generic.List[object]]::new()
foreach($root in $roots){
 $current=$root
 while($current){
  $long='\\?\'+$current
  if(([System.IO.File]::Exists($long)-or[System.IO.Directory]::Exists($long))-and
    ([System.IO.File]::GetAttributes($long)-band[System.IO.FileAttributes]::ReparsePoint)){throw 'quarantine_reparse_ancestor'}
  $parent=[System.IO.Path]::GetDirectoryName($current.TrimEnd('\'));if($parent-eq$current){break};$current=$parent
 }
 if(![System.IO.Directory]::Exists('\\?\'+$root)){throw ('quarantine root missing: '+$root)}
 $queue=[System.Collections.Generic.Stack[string]]::new();$queue.Push($root)
 while($queue.Count){
  $directory=$queue.Pop()
  foreach($entry in [System.IO.Directory]::EnumerateFileSystemEntries('\\?\'+$directory)){
   $attrs=[System.IO.File]::GetAttributes($entry)
   if($attrs-band[System.IO.FileAttributes]::ReparsePoint){throw 'quarantine_reparse_entry'}
   if($attrs-band[System.IO.FileAttributes]::Directory){$queue.Push($entry.Substring(4));continue}
   $file=[System.IO.FileInfo]::new($entry)
   $files.Add(@{root=$root;path=$entry.Substring(4);size_bytes=$file.Length;mtime_ns=($file.LastWriteTimeUtc.Ticks-621355968000000000)*100})
  }
 }
}
@{files=@($files)}|ConvertTo-Json -Depth 5 -Compress
""".replace("ROOT_DATA", data)
    encoded = base64.b64encode(script.encode("utf-16le")).decode()
    result = subprocess.run([find_pwsh7_executable(), "-NoProfile", "-EncodedCommand", encoded],
                            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=90)
    if result.returncode:
        raise ValueError("native quarantine census failed: " + result.stderr[:800])
    return json.loads(result.stdout)["files"]


def validate_census(handoff, expected, files):
    actual = {path_key(row["path"]): row for row in files}
    if len(actual) != len(files) or actual.keys() != expected.keys():
        raise ValueError("current quarantine population differs; reconcile before rebuilding the catalogue")
    for key, row in actual.items():
        wanted = expected[key]
        if (path_key(row["root"]) != path_key(wanted["root"]) or row["size_bytes"] != wanted["size_bytes"]
                or row["mtime_ns"] != wanted["mtime_ns"]):
            raise ValueError("quarantined recording changed: " + row["path"])
    for root, counts in handoff["directories"].items():
        members = [row for row in files if path_key(row["root"]) == path_key(root)]
        if len(members) != counts["files"] or sum(row["size_bytes"] for row in members) != counts["bytes"]:
            raise ValueError("per-volume handoff counts differ")


def file_url(path):
    return "file:///" + quote(path.replace("\\", "/"), safe="/:")


def render_catalog(handoff, files):
    esc = html.escape
    ordered = sorted(files, key=lambda row: (ntpath.basename(row["path"]).casefold(), row["path"]))
    # Store each path once. Decompression is entirely local, including in an
    # opaque-origin preview; no fetch, CDN, or additional file is required.
    data = json.dumps([[row["path"], row["size_bytes"]] for row in ordered],
                      ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    packed = base64.b64encode(gzip.compress(data, mtime=0)).decode("ascii")
    cards = "".join(f'<a class="card" href="{esc(file_url(root))}/"><strong>{esc(root[:2])}</strong>'
                    f'<span>{counts["files"]:,}本 · {counts["bytes"] / 10**12:.2f} TB</span>'
                    f'<small>{esc(root)}</small></a>' for root, counts in sorted(handoff["directories"].items()))
    rows = "".join(f'<tr data-drive="{esc(row["root"][:2])}"><td>{esc(row["root"][:2])}</td>'
                   f'<td><a href="{esc(file_url(row["path"]))}">{esc(ntpath.basename(row["path"]))}</a></td>'
                   f'<td>{row["size_bytes"] / 10**9:.2f} GB</td><td><details><summary>保存場所</summary>'
                   f'<code>{esc(row["path"])}</code></details></td></tr>'
                   for row in ordered[:50])
    return f'''<!doctype html><html lang="ja"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>削除待ちの録画一覧</title><style>
:root{{color-scheme:dark}}*{{box-sizing:border-box}}body{{margin:0;background:#111827;color:#e5edf9;font:16px system-ui,sans-serif}}
main{{max-width:1440px;margin:auto;padding:36px 24px}}h1{{margin:0 0 12px;font-size:30px}}p{{line-height:1.65;color:#b8c7dc}}
a{{color:#9dc9ff;text-decoration:none}}a:hover{{text-decoration:underline}}.cards{{display:flex;flex-wrap:wrap;gap:16px;margin:24px 0}}
.card{{flex:1;min-width:240px;padding:20px;border:1px solid #334155;border-radius:12px;background:#1e293b}}
.card strong,.card span,.card small{{display:block}}.card strong{{font-size:28px}}.card span{{margin:6px 0}}.card small{{word-break:break-all}}
.controls{{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin:24px 0}}input,select{{padding:12px;border:1px solid #475569;border-radius:8px;background:#1e293b;color:inherit;font:inherit}}input{{flex:1;min-width:220px}}
.table{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;font-size:14px}}th,td{{padding:12px;text-align:left;border-bottom:1px solid #334155}}th{{color:#cbd5e1}}td:nth-child(2){{min-width:300px;overflow-wrap:anywhere}}td:nth-child(3){{white-space:nowrap}}code{{overflow-wrap:anywhere}}summary{{cursor:pointer}}[hidden]{{display:none}}
</style><main><h1>削除待ちの録画</h1><p><strong>{len(files):,}本 · {sum(row["size_bytes"] for row in files) / 10**12:.2f} TB</strong> を1つの一覧にまとめた。細かいフォルダを開く必要はない。</p>
<p>削除対象は下の <code>quarantine_v3</code> 全体。親の <code>duplicates</code> は復元記録を含むので残す。保留{handoff["unresolved_groups_preserved"]}組{handoff["unresolved_files_preserved"]}本はこの一覧に含まれない。</p>
<section class="cards" aria-label="ドライブごとの削除対象">{cards}</section>
<div class="controls"><input id="search" type="search" placeholder="番組名・話数・ファイル名で検索" aria-label="録画を検索" disabled><select id="drive" aria-label="ドライブ" disabled><option value="">全ドライブ</option>{''.join(f'<option>{esc(root[:2])}</option>' for root in sorted(handoff["directories"]))}</select><span id="count" role="status">先頭{min(50, len(files))}本を表示 · 全件を読み込み中</span></div>
<noscript><p>全件の表示と検索にはJavaScriptが必要。現在は先頭{min(50, len(files))}本のみ表示。</p></noscript>
<div class="table"><table><thead><tr><th>ドライブ</th><th>ファイル名</th><th>容量</th><th>詳細</th></tr></thead><tbody>{rows}</tbody></table></div>
<p>一覧作成: {datetime.now(timezone.utc).isoformat()} / 内容確認・削除対象の引き渡し: {esc(handoff["verified_at"])}。現物のパス・容量・更新時刻を元の移動記録と照合済み。内容の再判定、録画の移動、DB更新、削除は行っていない。ブラウザがフォルダリンクを開けない場合は表示されたパスをエクスプローラーに貼り付ける。</p></main>
<script id="catalog-data" type="application/octet-stream">{packed}</script>
<script>
(async()=>{{
 const search=document.getElementById('search'),drive=document.getElementById('drive'),count=document.getElementById('count');
 try{{
  const bytes=Uint8Array.from(atob(document.getElementById('catalog-data').textContent),c=>c.charCodeAt(0));
  const stream=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
  const records=JSON.parse(await new Response(stream).text()),tbody=document.querySelector('tbody'),fragment=document.createDocumentFragment();
  for(const [path,size] of records){{
   const row=document.createElement('tr');row.dataset.drive=path.slice(0,2);
   const cells=Array.from({{length:4}},()=>row.appendChild(document.createElement('td')));
   cells[0].textContent=row.dataset.drive;
   const link=cells[1].appendChild(document.createElement('a'));
   link.textContent=path.split('\\\\').pop();
   link.href='file:///'+path.split('\\\\').map(part=>encodeURIComponent(part).replace(/%3A/gi,':')).join('/');
   cells[2].textContent=(size/1e9).toFixed(2)+' GB';
   const details=cells[3].appendChild(document.createElement('details'));
   details.appendChild(document.createElement('summary')).textContent='保存場所';
   details.appendChild(document.createElement('code')).textContent=path;
   fragment.appendChild(row);
  }}
  tbody.replaceChildren(fragment);
  const rows=[...tbody.rows];
  function filter(){{const q=search.value.normalize('NFKC').toLocaleLowerCase();let n=0;for(const row of rows){{row.hidden=!!((drive.value&&row.dataset.drive!==drive.value)||!row.cells[1].textContent.normalize('NFKC').toLocaleLowerCase().includes(q));if(!row.hidden)n++;}}count.textContent=n.toLocaleString()+'本を表示';}}
  search.disabled=drive.disabled=false;search.addEventListener('input',filter);drive.addEventListener('change',filter);filter();
 }}catch(error){{count.textContent='全件を読み込めないため先頭{min(50, len(files))}本のみ表示。最新のChrome・Edge・Safariで開いて。';console.error(error);}}
}})();
</script></html>'''


def create_catalog(handoff_path, ops_root, output, *, census=native_census):
    handoff, expected = verified_population(handoff_path, ops_root)
    output = safe_path(output)
    if output.suffix.lower() != ".html":
        raise ValueError("catalogue output must be HTML")
    output_key = path_key(wsl_to_windows_path(str(output)))
    for root in handoff["directories"]:
        if output_key == path_key(root) or output_key.startswith(path_key(root) + "\\"):
            raise ValueError("catalogue must remain outside deletable quarantine directories")
    runs_key = path_key(wsl_to_windows_path(str(safe_path(ops_root) / "duplicates" / "runs")))
    if output_key == runs_key or output_key.startswith(runs_key + "\\"):
        raise ValueError("catalogue must not change exact run records")
    files = census(sorted(handoff["directories"]))
    validate_census(handoff, expected, files)
    content = render_catalog(handoff, files)
    html_bytes = len(content.encode("utf-8"))
    if html_bytes > CATALOG_PREVIEW_MAX_BYTES:
        raise ValueError("catalogue exceeds the 256 KiB preview budget; no HTML was written")
    output.parent.mkdir(parents=True, exist_ok=True)
    # Never overwrite unrelated HTML (or a concurrently created destination).
    with output.open("x", encoding="utf-8") as stream:
        stream.write(content)
    return {"ok": True, "catalogPath": str(output), "files": len(files),
            "htmlBytes": html_bytes,
            "bytes": sum(row["size_bytes"] for row in files), "mediaOperations": 0,
            "databaseWrites": 0, "permanentlyDeleted": 0}
