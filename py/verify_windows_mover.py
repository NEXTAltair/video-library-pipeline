"""Real Windows integration check using only generated scratch files."""
import argparse
import json
import uuid
from datetime import datetime,timezone
from pathlib import Path
from video_pipeline.platform.windows_pwsh_bridge import run_pwsh_json,wsl_path_to_win_str


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--config",required=True); parser.add_argument("--out",required=True)
    args = parser.parse_args()
    cfg = json.loads(Path(args.config).read_text())["plugins"]["entries"]["video-library-pipeline"]["config"]
    ops = Path(cfg["windowsOpsRoot"])
    scratch = ops / ("verify_mover_" + uuid.uuid4().hex); scratch.mkdir()
    import shutil
    for name in ("apply_move_plan.ps1","_long_path_utils.ps1"):
        shutil.copy2(Path(__file__).resolve().parent.parent/"assets"/"windows-scripts"/name,scratch/name)
    plan = scratch / "plan.jsonl"; receipts = scratch / "receipts.jsonl"
    rows = []
    for mode in ["safe","collision","stale"]:
        src,dst = scratch/(mode+"-src.mp4"),scratch/(mode+"-dst.mp4")
        src.write_bytes(b"generated fixture only")
        stat = src.stat()
        rows.append({"path_id":mode,"file_id":mode,"src":wsl_path_to_win_str(str(src)),"dst":wsl_path_to_win_str(str(dst)),"precondition":{"size_bytes":stat.st_size,"mtime_utc":datetime.fromtimestamp(stat.st_mtime,timezone.utc).isoformat()}})
        if mode == "collision": dst.write_bytes(b"must never be overwritten")
        if mode == "stale": src.write_bytes(b"changed fixture")
    plan.write_text("\n".join(json.dumps(row) for row in rows)+"\n")
    result = run_pwsh_json(str(scratch/"apply_move_plan.ps1"),["-PlanJsonl",str(plan),"-OpsRoot",str(scratch),"-OutJsonl",str(receipts)])
    records = [json.loads(line) for line in receipts.read_text(encoding="utf-8-sig").splitlines()]
    moves = [r for r in records if r.get("op") == "move"]
    assert len(moves) == 3
    assert moves[0]["ok"] and moves[0]["file_id"] == "safe"
    assert moves[1]["error"] == "dst_exists"
    assert moves[2]["error"] == "source_changed"
    assert (scratch/"collision-dst.mp4").read_bytes() == b"must never be overwritten"
    assert (scratch/"collision-src.mp4").exists()
    assert (scratch/"stale-src.mp4").exists()
    assert not (scratch/"stale-dst.mp4").exists()
    assert not (scratch/"safe-src.mp4").exists()
    assert (scratch/"safe-dst.mp4").read_bytes() == b"generated fixture only"
    payload = {"ok":True,"scratch":str(scratch),"checks":["move_and_durable_receipt","destination_not_overwritten","changed_source_not_moved"],"result":result,"receipts":records}
    Path(args.out).write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({"ok":True,"checks":payload["checks"]}))


if __name__ == "__main__": main()
